#
# FILE: `StockTrader/src/StockTrader/execution/order_iface.py`
#

import pandas as pd

from abc import ABC, abstractmethod
from typing import List, Union
from StockTrader.execution.dto import (
	CancelRequest,
	CancelResult,
	ExecutionResult,
	OrderRequestUnion
)

class OrderBuilder (ABC):
	@abstractmethod
	def build (self, data:pd.DataFrame) -> List[OrderRequestUnion]:
		pass

class OrderExecutor (ABC):
	@abstractmethod
	def execute (self, orders: List[OrderRequestUnion], dry_run: bool=False) -> List[ExecutionResult]:
		pass
	def cancel (self, cancels: List[CancelRequest], dry_run: bool=False) -> List[CancelResult]:
		raise NotImplementedError("this executor does not support cancellation")

class DataLoader (ABC):
	@abstractmethod
	def load (self, **kwargs) -> pd.DataFrame:
		pass

class ResultPersister (ABC):
	@abstractmethod
	def write (self, df:pd.DataFrame, ctx: dict) -> str:
		pass

