#
# FILE: `StockTrader/src/StockTrader/execution/candidate_store.py`
#

import io
import os


import pandas as pd
from minio import Minio
from minio.error import S3Error

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ROOT_USER")
MINIO_SECRET_KEY = os.environ.get("MINIO_ROOT_PASSWORD")

CANDIDATES_BUCKET = "trading-candidates"
ORDERS_BUCKET = "trading-orders"


class CandidateStore:
	def __init__ (self):
		self._minio = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)

	# def read_candidtes (self, strategy: str, run_date:)
	def read_candidates (self, mart: str, fname_date:str) -> pd.DataFrame:
		try:
			r = self._minio.get_object(
				 bucket_name = CANDIDATES_BUCKET,
				 object_name = f"{mart}/{fname_date}.parquet"
			)
			data = r.read()
			r.close()
			r.release_conn()
		except S3Error as e:
			raise FileNotFoundError(
				f"No candidate file: s3://{CANDIDATES_BUCKET}/{mart}/{fname_date}.parquet"
			) from e
		return pd.read_parquet(io.BytesIO(data))


	def write_order_results (self, df:pd.DataFrame, fname_date: str) -> str:
		object_name = f"{fname_date}.parquet"

		if not self._minio.bucket_exists(ORDERS_BUCKET):
			self._minio.make_bucket(ORDERS_BUCKET)

		buf = io.BytesIO()
		df.to_parquet(buf, index=False, engine="pyarrow", compression="zstd")
		buf.seek(0)

		self._minio.put_object(
			bucket_name = ORDERS_BUCKET,
			object_name = object_name,
			data = buf,
			length = buf.getbuffer().nbytes,
			content_type = "application/octet-stream"
		)

		return f"s3://{ORDERS_BUCKET}/{object_name}"