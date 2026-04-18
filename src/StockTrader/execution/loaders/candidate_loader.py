#
# FILE: `StockTrader/src/StockTrader/execution/loaders/candidate_loader.py`
#

import pandas as pd
from utils.minio_store import MinioStore
from StockTrader.execution.order_iface import DataLoader

class CandidateLoader(DataLoader):
    """Loads candidate Parquet files from MinIO using the existing MinioStore utility."""
    def __init__(self, store: MinioStore, bucket: str):
        self.store = store
        self.bucket = bucket

    def load(self, mart: str, date_str: str) -> pd.DataFrame:
        object_name = f"{mart}/{date_str}.parquet"
        try:
            return self.store.read_parquet(bucket=self.bucket, obj_name=object_name)
        except FileNotFoundError as e:
            raise FileNotFoundError(f"Candidate file not found: s3://{self.bucket}/{object_name}") from e
