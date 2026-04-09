#
# FILE: `StockTrader/src/StockTrader/execution/order_executor.py`
#

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from StockTrader.settings import logger
from StockTrader.execution.order_builder import OrderRequest


@dataclass
class OrderResult:
    df_idx: int
    submitted_time: str
    is_submitted: bool
    tradier_order_id: int | None
    error: str | None


class OrderExecutor:
    def __init__(self, trader):
        self._trader = trader

    def execute(self, orders: list[OrderRequest], dry_run: bool = False) -> list[OrderResult]:
        results = []
        for order in orders:
            results.append(self._execute_single(order, dry_run))

        submitted = sum(1 for r in results if r.is_submitted)
        failed = len(results) - submitted
        logger.info(f"Exec summary: n_submitted={submitted}, n_failed={failed}")

        return results

    def _execute_single(self, order: OrderRequest, dry_run: bool) -> OrderResult:
        rn = datetime.now(timezone.utc).isoformat()

        if dry_run:
            logger.info(f"DRY_RUN: {order.occ} {order.side}" f" x{order.quantity} @ {order.limit_price}")

            return OrderResult(
                df_idx=order.df_idx, submitted_time=rn, is_submitted=True, tradier_order_id=-1, error=None
            )

        try:
            r = self._trader.options_order(
                occ_symbol=order.occ,
                order_type=order.order_type,
                side=order.side,
                quantity=order.quantity,
                limit_price=order.limit_price,
                duration=order.duration,
            )

            order_id = None
            if isinstance(r, dict) and "order" in r:
                order_id = r["order"].get("id")

            logger.info(
                f"SUBMITTED: {order.occ} {order.side}"
                f" x{order.quantity} @ {order.limit_price}"
                f" order_id={order_id}"
            )

            return OrderResult(
                df_idx=order.df_idx,
                submitted_time=rn,
                is_submitted=order_id is not None,
                tradier_order_id=order_id,
                error=None,
            )

        except Exception as e:
            logger.error(f"FAILED: {order.occ} -> {e}")

            return OrderResult(
                df_idx=order.df_idx, submitted_time=rn, is_submitted=False, tradier_order_id=None, error=str(e)
            )
