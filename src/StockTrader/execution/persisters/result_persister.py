#
# FILE: `StockTrader/src/StockTrader/execution/persisters/result_persister.py`
#

import pandas as pd
from utils.minio_store import MinioStore
from StockTrader.execution.order_iface import ResultPersister


# class ResultPersister(ResultPersister):
class MinioResultPersister(ResultPersister):
    """Persists order results as Parquet to MinIO using the existing MinioStore utility."""

    def __init__(self, store: MinioStore, bucket: str):
        self.store = store
        self.bucket = bucket

    # def save(self, df: pd.DataFrame, context: dict) -> str:
    def write(self, df: pd.DataFrame, ctx: dict) -> str:
        mart = ctx.get("mart", "unknown_mart")
        snapshot_id = ctx.get("snapshot_id")

        #
        # Intraday Snapshot-Driven Runs: One Object per Snapshot (e.g. mart/YYYYMMDD/HHMMSS.parquet)
        # EOD Date-Driven Runs (main2.py): One Object per Date
        #

        if snapshot_id:
            object_name = f"{mart}/{snapshot_id}"
        else:
            date_str = ctx.get("date_str", "unknown_date")
            object_name = f"{mart}/{date_str.replace('-', '')}.parquet"

        return self.store.write_parquet(bucket=self.bucket, obj_name=object_name, df=df, ensure_bucket=True)
