#
# FILE: `StockTrader/src/StockTrader/execution/orchestrator.py`
#

import pandas as pd
from StockTrader.settings import logger
from StockTrader.execution.order_iface import (
	DataLoader,
	OrderBuilder,
	OrderExecutor,
	ResultPersister
)
from StockTrader.execution.dto import ExecutionResult

class OrderPipe:
	def __init__(
		self,
		loader: DataLoader,
		builder: OrderBuilder,
		executor: OrderExecutor,
		persister: ResultPersister
	):
		self.loader = loader
		self.builder = builder
		self.executor = executor
		self.persister = persister

	def run (self, dry_run:bool=False, **load_kwargs) -> str:
		#
		# Five Steps:
		# 	1. Load Candidate Data
		# 	2. Build Orders
		# 	3. Execute Order
		# 	4. Augment Input DF with Order Results
		# 	5. Persist to S3 Storage
		#

		df = self.loader.load(**load_kwargs)
		if df.empty:
			raise ValueError("No data loaded [orchestratpr]")

		orders = self.builder.build(df)
		if not orders:
			raise ValueError("No orders built [orchestrator]")

		results = self.executor.execute(orders, dry_run=dry_run)

		df = self._augment_dataframe(df,results)

		fpath_output = self.persister.write(df, ctx=load_kwargs)
		return fpath_output

	def _augment_dataframe (self, df:pd.DataFrame, results:list[ExecutionResult]) -> pd.DataFrame:

		df["order_submitted"] = False
		df["order_id"] = None
		df["order_error"] = None
		df["submit_ts"] = None

		n_bad = 0

		for r in results:
			idx = r.request.df_idx
			if idx is None or idx not in df.index:
				n_bad += 1
				logger.warning(
					f"ExecutionResult has no valid df_idx (idx={idx}); "
					f"result excluded from persisted df"
				)
				continue
			df.at[idx, "order_submitted"] = r.success
			df.at[idx, "order_id"] = r.order_id
			df.at[idx, "order_error"] = r.error
			df.at[idx, "submit_ts" ] = r.submit_ts

		if n_bad > 0:
			logger.warning(f"Dropped n={n_bad} results b/c of missing df")

		return df