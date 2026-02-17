#
# FILE: `StockTrader/infrastructure/redis/job_worker.py`
#

import os
import io
import json
import signal
import socket
import time

import pandas as pd
import redis
from datetime import datetime
from minio import Minio

from infrastructure.redis.job_schema import (
	PricingJob,
	Q_PENDING,
	Q_PROCESSING,
	Q_COMPLETED,
	Q_FAILED,
	WORKER_HEARTBEAT
)

from StockTrader.pricing.batch import process_job_shard, results_to_df
from StockTrader.settings import logger

class JobWorker:
	def __init__ (
		self,
		redis_url: str=None,
		minio_endpoint: str=None,
		minio_access_key: str=None,
		minio_secret_key: str=None,
		input_bucket: str="pricing-inputs",
		output_bucket: str="pricing-outputs",
		model_name: str="crr_binomial_american_dividends",
		**model_kwargs
	):
		redis_url = redis_url or os.environ.get("REDIS_URL", "redis://localhost:6379/0")
		self.redis = redis.from_url(redis_url, decode_responses=True)

		minio_endpoint = minio_endpoint or os.environ.get("MINIO_ENDPOINT", "localhost:9000")
		minio_access_key = minio_access_key or os.environ.get("MINIO_ROOT_USER")
		minio_secret_key = minio_secret_key or os.environ.get("MINIO_ROOT_PASSWORD")

		self.minio = Minio(minio_endpoint, access_key=minio_access_key, secret_key=minio_secret_key, secure=False)
		self.input_bucket = input_bucket
		self.output_bucket = output_bucket
		self.model_name = model_name
		self.model_kwargs = model_kwargs
		self.worker_id = f"{socket.gethostname()}_{os.getpid()}"

		self._shutdown = False

	def load_shard (self, job:PricingJob) -> pd.DataFrame:
		prefix = job.prefixS3_input
		objs = list(self.minio.list_objects(self.input_bucket, prefix=prefix, recursive=True))
		parquet_objs = [o for o in objs if o.object_name.endswith(".parquet")]


		if not parquet_objs:
			raise FileNotFoundError(f"No parquet at {self.input_bucket}/{prefix}")

		frames = []

		for o in parquet_objs:
			response = self.minio.get_object(self.input_bucket, o.object_name)
			bffr = io.BytesIO(response.read())
			response.close()
			response.release_conn()
			frames.append(pd.read_parquet(bffr))

		df = pd.concat(frames, ignore_index=True) if len(frames) > 1 else frames[0]
		# logger.info(f"Loaded {len(df)} records {self.input_bucket}/{prefix} ")
		logger.info(f"Loaded n={len(df)} records, k={len(parquet_objs)} files, from={self.input_bucket}/{prefix}")

		return df


	def write_results (self, job:PricingJob, df_results:pd.DataFrame, manifest:dict) -> str:
		prefix = job.prefixS3_output

		parquet_key = f"{prefix}results.parquet"
		bffr = io.BytesIO()
		# df_results.to_parquet(bffr, index=False, engine="pyarrow", compression="snappy")
		df_results.to_parquet(bffr, index=False, engine="pyarrow")
		bffr.seek(0)

		self.minio.put_object(self.output_bucket, parquet_key, bffr, length=bffr.getbuffer().nbytes, content_type="application/octet-stream")

		manifest_key = f"{prefix}manifest.json"
		manifest_bytes = json.dumps(manifest, indent=2, default=str).encode("utf-8")
		manifest_bffr = io.BytesIO(manifest_bytes)

		self.minio.put_object(self.output_bucket, manifest_key, manifest_bffr, length=len(manifest_bytes), content_type="application/json")

		return parquet_key


	def complete_job (self, job:PricingJob, job_json: str):
		pipe = self.redis.pipeline()
		pipe.lrem(Q_PROCESSING, 1, job_json)
		pipe.sadd(Q_COMPLETED, job.job_id)
		pipe.execute()


	def fail_job (self, job:PricingJob, job_json:str, error:str):
		pipe = self.redis.pipeline()
		pipe.lrem(Q_PROCESSING,1, job_json)
		pipe.sadd(Q_FAILED, job.job_id)
		pipe.hset(
			f"pricing:job_errors:{job.job_id}",
			mapping={"error":error[:500], "worker":self.worker_id, "failed_at":datetime.utcnow().isoformat()}
		)
		# pipe.exectue()
		pipe.execute()

	def heartbeat (self):
		self.redis.hset(WORKER_HEARTBEAT, self.worker_id, datetime.utcnow().isoformat())

	def process_one(self, job_json:str) -> bool:
		job = PricingJob.from_json(job_json)
		logger.info(f"Worker={self.worker_id}, job={job.job_id}") 

		try:
			df = self.load_shard(job)
			market_date = datetime.strptime(job.market_date, "%Y-%m-%d").date()

			batch_result = process_job_shard(
				df=df,
				job_id=job.job_id,
				batch_id=job.batch_id,
				market_date=market_date,
				shard=job.shard,
				model_name=self.model_name,
				**self.model_kwargs
			)

			df_results = results_to_df(batch_result.results)
			manifest = batch_result.to_manifest_entry()
			manifest["worker_id"] = self.worker_id

			output_key = self.write_results(job, df_results, manifest)

			self.complete_job(job, job_json)

			# logger.info(
			# 	f"Job {job.job_id} complete"
			# 	f"{batch_result.n_success}/{batch_result.n_total} priced"
			# 	f""
			# 	f""
			# 	f""
			# )
			logger.info(f"Job {job.job_id} complete")
			logger.info(f"Priced={batch_result.n_success}/{batch_result.n_total}")
			logger.info(f"SolvedIV={batch_result.n_iv_solved}")
			logger.info(f"Compute={batch_result.elapsed_sec:.2f}s")
			logger.info(f"Output={output_key}")

			return True
		except Exception as e:
			logger.error(f"Job {job.job_id} failed: {e}")
			self.fail_job(job, job_json, str(e))
			return False

	def run (self, poll_timeout:int=5, max_jobs:int=0):
		def _handle_signal (signum, frame):
			logger.info(f"Worker={self.worker_id}, signal={signum}")
			logger.info("Shutdown following job")
			self._shutdown = True

		signal.signal(signal.SIGTERM, _handle_signal)
		signal.signal(signal.SIGINT, _handle_signal)

		n_processed = 0
		n_success = 0

		while not self._shutdown:
			self.heartbeat()

			job_json = self.redis.brpoplpush(Q_PENDING, Q_PROCESSING, timeout=poll_timeout)

			if job_json is None:
				continue

			ok = self.process_one(job_json)
			n_processed += 1
			n_success += int(ok)

			if 0 < max_jobs <= n_processed:
				logger.info(f"reached max_jobs={max_jobs}")
				break;

		logger.info(f"Shut down, worker={self.worker_id}")
		logger.info(f"n_processed={n_processed}, n_success={n_success}")

def main():
	import argparse

	parser = argparse.ArgumentParser(description="Pricing Job Worker")
	parser.add_argument("--model", help="Registered pricing model name")
	parser.add_argument("--n-steps", type=int, help="BOPM tree steps")
	parser.add_argument("--max-jobs", type=int, default=0, help="Exit after max_jobs")
	parser.add_argument("--timeout", type=int, default=5, help="Q poll timeout sec")

	args = parser.parse_args()

	import StockTrader.pricing.qlib.models

	worker = JobWorker(model_name=args.model, n_steps=args.n_steps)
	worker.run(poll_timeout=args.timeout, max_jobs=args.max_jobs)

if __name__ == "__main__":
	main()


















