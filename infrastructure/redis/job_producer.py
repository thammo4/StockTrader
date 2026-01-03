#
# FILE: `StockTrader/infrastructure/redis/job_producer.py`
#

"""
Scans MinIO pricing-inputs bucket -> Enqueues Redis Job.
"""

import os
import re
import redis
from minio import Minio
from typing import List, Set

from infrastructure.redis.job_schema import (
	PricingJob,
	Q_PENDING,
	Q_PROCESSING,
	Q_COMPLETED,
	Q_FAILED
)

from StockTrader.settings import logger



class JobProducer:
	"""
	Discovers work units in MinIO. Pushes new work to Redis job queue.
	"""

	def __init__(
		self,
		redis_url: str = None,
		minio_endpoint: str = None,
		minio_access_key: str = None,
		minio_secret_key: str = None,
		input_bucket: str = "pricing-inputs"
	):
		redis_url = redis_url or os.environ.get("REDIS_URL", "redis://localhost:6379/0")
		self.redis = redis.from_url(redis_url, decode_responses=True)

		minio_endpoint = minio_endpoint or os.environ.get("MINIO_ENDPOINT", "127.0.0.1:9000")
		minio_access_key = minio_access_key or os.environ.get("MINIO_ROOT_USER")
		minio_secret_key = minio_secret_key or os.environ.get("MINIO_ROOT_PASSWORD")

		self.minio = Minio(
			minio_endpoint,
			access_key = minio_access_key,
			secret_key = minio_secret_key,
			secure=False
		)

		self.input_bucket = input_bucket

	def get_job_ids_existing (self) -> Set[str]:
		"""
		Returns job IDs currently in Redis queue to avoid re-enqueuing the same job.
		"""

		existing_jobs = set()

		for q in [Q_PENDING, Q_PROCESSING, Q_COMPLETED, Q_FAILED]:
			if q in [Q_PENDING, Q_PROCESSING]:
				jobs = self.redis.lrange(q, 0, -1)
				for job_json in jobs:
					job = PricingJob.from_json(job_json)
					existing_jobs.add(job.job_id)
			else:
				existing_jobs.update(self.redis.smembers(q))
		return existing_jobs

	def discover_jobs (self, batch_id: str=None) -> List[PricingJob]:
		""" Scans MinIO to ID new possible jobs. """

		jobs = []
		prefixS3 = "batch_" if batch_id is None else f"batch_{batch_id}"
		objects = self.minio.list_objects(self.input_bucket, prefix=prefixS3, recursive=True)
		seen_jobs = set() # counting unique combinations of (batch, date, shard)
		path_pattern = re.compile(r"batch_(?P<batch_id>[^/]+)/market_date=(?P<market_date>[^/]+)/shard=(?P<shard>\d+)/"

		for obj in objects:
			m = path_pattern.match(obj.object_name)
			if m:
				key = (m.group("batch_id"), m.group("match_date"), int(m.group("shard")))
				if key not in seen_jobs:
					seen_jobs.add(key)
					jobs.append(
						PricingJob(batch_id=key[0], market_date=key[1], shard=key[2])
					)

		logger.info(f"Discovered {len(jobs)} prospective jobs in MinIO")

		return jobs


	def enqueue_jobs (self, batch_id: str=None, force: bool=False) -> int:
		""" Identifies and enqueues new jobs to Redis """

		jobs_potential = self.discover_jobs(batch_id)
		jobs_existing  = set() if force else self.get_job_ids_existing()
		jobs_new = [j for j in jobs_potential if j.job_id is not in jobs_existing]

		if not jobs_new:
			logger.info("No new jobs to enqueue")
			return 0

		pipe = self.redis.pipeline()
		for job in jobs_new:
			pipe.lpush(Q_PENDING, job.to_json())

		pipe.execute()
		logger.info(f"Enqueued {len(jobs_new)} new jobs")

		return len(jobs_new)

	def get_queue_status (self) -> dict:
		return {
			"pending": self.redis.llen(Q_PENDING),
			"processing": self.redis.llen(Q_PROCESSING),
			"completed": self.redis.scard(Q_COMPLETED),
			"failed": self.redis.scard(Q_FAILED)
		}

def main():
	import argparse
	pass


if __name__ == "__main__":
	main()

