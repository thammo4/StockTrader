#
# FILE: `StockTrader/src/StockTrader/execution/executor.py`
#

from datetime import datetime, timezone
from typing import List, Union

from StockTrader.settings import logger
from StockTrader.execution.dto import (
	AssetType,
	ExecutionResult,
	MLegOrderRequest,
	MLegOrderType,
	OrderLeg,
	OrderRequest,
	OrderRequestUnion,
	OrderSide,
	OrderType
)

from StockTrader.execution.order_iface import OrderExecutor

class TraderAdapter:
	def __init__ (self, options_client, equities_client):
		self._options = options_client
		self._equities = equities_client

	def place_order (self, order:  OrderRequestUnion) -> dict:
		if isinstance (order, MLegOrderRequest):
			return self._place_multileg_order(order)
		elif order.asset_type == AssetType.OPTION:
			return self._place_option_order(order)
		else:
			if self._equities is None:
				raise RuntimeError(
					"TraderAdapter received equity order without equities client"
				)
			return self._place_equity_order(order)

	def _place_option_order (self, order: OrderRequest) -> dict:
		return self._options.options_order(
			occ_symbol = order.occ,
			order_type = order.order_type.value,
			side = order.side.value,
			quantity = order.quantity,
			underlying = order.symbol,
			limit_price = order.limit_price,
			stop_price = order.stop_price,
			duration = order.duration
		)
	def _place_equity_order (self, order: OrderRequest) -> dict:
		return self._equities.order(
			symbol = order.symbol,
			side = order.side.value,
			quantity = order.quantity,
			order_type = order.order_type.value,
			limit_price = order.limit_price,
			stop_price = order.stop_price,
			duration = order.duration
		)

	def _place_multileg_order(self, order: MLegOrderRequest) -> dict:
		occs = [l.occ for l in order.legs]
		sides = [l.side.value for l in order.legs]
		quantities = [l.quantity for l in order.legs]

		return self._options.multileg_order(
			occ_symbols = occs,
			sides = sides,
			quantities = quantities,
			order_type = order.order_type.value,
			limit_price = order.limit_price,
			duration = order.duration,
			underlying = order.symbol
		)


class SimpleOrderExecutor (OrderExecutor):
	def __init__ (self, trader_adapter: TraderAdapter):
		self._adapter = trader_adapter

	def execute (self, orders: List[OrderRequestUnion], dry_run: bool = False) -> List[ExecutionResult]:
		run_mode = "DRY_RUN" if dry_run else "LIVE"
		logger.info(f"{run_mode} SimpleOrderExecutor.execute: n_orders={len(orders)}")

		# results = []
		results: List[ExecutionResult] = []
		for order in orders:
			results.append(self._dry_run_result(order)) if dry_run else results.append(self._execute_one(order))
		self._log_summary(results)
		return results

	def _execute_one (self, order: OrderRequestUnion) -> ExecutionResult:
		try:
			r = self._adapter.place_order(order)
			order_id = r.get("order", {}).get("id") if isinstance(r,dict) else None
			return ExecutionResult(
				request = order,
				success = order_id is not None,
				order_id = order_id,
				submit_ts = datetime.now(timezone.utc).isoformat()
			)
		except Exception as e:
			logger.error(f"Order failed: {order}: {str(e)}")
			return ExecutionResult(
				request = order,
				success = False,
				error = str(e),
				submit_ts = datetime.now(timezone.utc).isoformat()
			)

	def _dry_run_result (self, order: OrderRequestUnion) -> ExecutionResult:
		logger.info(f"DRY_RUN: {order}")
		return ExecutionResult(
			request = order,
			success=True,
			order_id = -1,
			submit_ts = datetime.now(timezone.utc).isoformat()
		)

	def _log_summary (self, results: List[ExecutionResult]):
		n_submit = sum(1 for x in results if x.success)
		n_failed = len(results) - n_submit
		logger.info(f"Exec summary: n_submit={n_submit}, n_fail={n_failed}")

