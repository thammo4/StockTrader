#
# FILE: `StockTrader/src/StockTrader/execution/order_builder.py`
#

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

import pandas as pd


@dataclass(frozen=True)
class OrderRequest:
    occ: str
    order_type: str
    side: str
    quantity: int
    limit_price: float
    duration: str
    df_idx: int


class OrderBuilder:

    def __init__(self) -> None:
        self._builders: dict[str, Callable[[pd.DataFrame], list[OrderRequest]]] = {
            "mart_vrp__high_ivp_candidates": self._build_vrp_orders
        }

    def build(self, mart: str, candidates: pd.DataFrame) -> list[OrderRequest]:
        builder = self._builders.get(mart)
        if builder is None:
            raise ValueError(f"No registered builder for mart: {mart}")
        return builder(candidates)

    def _build_vrp_orders(self, candidates: pd.DataFrame) -> list[OrderRequest]:
        orders = []

        for idx, row in candidates.iterrows():
            orders.append(
                OrderRequest(
                    occ=row["occ"],
                    order_type="limit",
                    side="sell_to_open",
                    quantity=1,
                    limit_price=float(row["mid_price"]),
                    duration="day",
                    df_idx=idx,
                )
            )

        return orders
