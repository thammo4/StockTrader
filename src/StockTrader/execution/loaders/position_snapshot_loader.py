#
# FILE: `StockTrader/src/StockTrader/execution/loaders/position_snapshot_loader.py`
#

import pandas as pd

from StockTrader.execution.order_iface import DataLoader
from utils.minio_store import MinioStore

class PositionSnapshotLoader (DataLoader):
	def __init__(self, store: MinioStore, bucket: str="portfolio-snapshots"):
		self._store = store
		self._bucket = bucket

	def load (self, snapshot_id: str, **kwargs) -> pd.DataFrame:
		try:
			return self._store.read_parquet(bucket=self._bucket, obj_name=snapshot_id)
		except FileNotFoundError as e:
			raise FileNotFoundError(f"Portfolio snapshot not found: s3://{self._bucket}/{snapshot_id}") from e

