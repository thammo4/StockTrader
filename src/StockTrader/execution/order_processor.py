#
# FILE: `StockTrader/src/StockTrader/execution/order_processor.py`
#

import pandas as pd
from typing import Optional

from StockTrader.execution.candidate_store import CandidateStore
from StockTrader.execution.order_builder import OrderBuilder, OrderRequest
from StockTrader.execution.order_executor import OrderExecutor
from StockTrader.settings import logger


class OrderProcessor:
    def __init__(
        self,
        store: Optional[CandidateStore] = None,
        builder: Optional[OrderBuilder] = None,
        # executor:Optional[OrderExecutor]=None
    ):
        self.store = store or CandidateStore()
        self.builder = builder or OrderBuilder()
        # self.executor = executor

    def process(self, mart: str, date_str: str, trader, dry_run: bool = False) -> str:
        df = self.store.read_candidates(mart, date_str)
        logger.info(f"Loaded n={len(df)} candidates from {mart}/{date_str}.parquet")

        orders: list[OrderRequest] = self.builder.build(mart, df)
        logger.info(f"Built n={len(orders)} order reqs")

        executor = OrderExecutor(trader)
        results = executor.execute(orders, dry_run=dry_run)

        df["order_submitted"] = False
        df["order_id"] = None
        df["order_error"] = None
        df["submitted_time"] = None

        for res in results:
            idx = res.df_idx
            df.at[idx, "order_submitted"] = res.is_submitted
            df.at[idx, "order_id"] = res.tradier_order_id
            df.at[idx, "order_error"] = res.error
            df.at[idx, "submitted_time"] = res.submitted_time

        output_name = date_str.replace("-", "")
        output_path = self.store.write_order_results(df, output_name)
        logger.info(f"Order results persisted to: {output_path}")

        return output_path
