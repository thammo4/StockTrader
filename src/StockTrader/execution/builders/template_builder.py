#
# FILE: `StockTrader/src/StockTrader/execution/builders/template_builder.py`
#

import yaml
import numpy as np
import pandas as pd
from pathlib import Path
from typing import List, Optional, Union

from StockTrader.execution.dto import (
    AssetType,
    MLegOrderRequest,
    MLegOrderType,
    OrderLeg,
    OrderRequest,
    OrderRequestUnion,
    OrderSide,
    OrderType,
)

from StockTrader.execution.order_iface import OrderBuilder


class TemplateOrderBuilder(OrderBuilder):
    def __init__(self, template_path: str):
        with open(template_path, "r") as f:
            self.template = yaml.safe_load(f)
        self.is_multi_leg = "legs" in self.template

    def build(self, data: pd.DataFrame) -> List[OrderRequestUnion]:
        if data.empty:
            return []
        return self._build_multi_leg(data) if self.is_multi_leg else self._build_single_leg(data)

    def _build_single_leg(self, data: pd.DataFrame) -> List[OrderRequest]:
        orders = []
        for idx, row in data.iterrows():
            orders.append(self._build_single_order(row, idx))
        return orders

    def _build_single_order(self, row: pd.Series, idx) -> OrderRequest:
        t = self.template
        return OrderRequest(
            asset_type=AssetType(self._resolve(t["asset_type"], row)),
            symbol=self._resolve(t["symbol"], row),
            occ=self._resolve_optional_str(t.get("occ"), row),
            side=OrderSide(self._resolve(t["side"], row)),
            order_type=OrderType(self._resolve(t["order_type"], row)),
            quantity=int(self._resolve(t["quantity"], row)),
            limit_price=self._resolve_optional_float(t.get("limit_price"), row),
            stop_price=self._resolve_optional_float(t.get("stop_price"), row),
            duration=self._resolve(t.get("duration", "day"), row),
            # mdata = {"df_idx": idx}
            df_idx=int(idx) if idx is not None else None,
        )

    def _build_multi_leg(self, data: pd.DataFrame) -> List[MLegOrderRequest]:
        orders = []
        for idx, row in data.iterrows():
            legs = []
            for t_leg in self.template["legs"]:
                legs.append(
                    OrderLeg(
                        asset_type=AssetType(self._resolve(t_leg["asset_type"], row)),
                        symbol=self._resolve(t_leg["symbol"], row),
                        occ=self._resolve_optional_str(t_leg.get("occ"), row),
                        side=OrderSide(self._resolve(t_leg["side"], row)),
                        quantity=int(self._resolve(t_leg["quantity"], row)),
                    )
                )
            orders.append(
                MLegOrderRequest(
                    legs=legs,
                    order_type=MLegOrderType(self._resolve(self.template["order_type"], row)),
                    symbol=(
                        str(self._resolve(self.template["symbol"], row))
                        if "symbol" in self.template
                        else legs[0].symbol
                    ),
                    limit_price=self._resolve_optional_float(self.template.get("limit_price"), row),
                    duration=self._resolve(self.template.get("duration", "day"), row),
                    df_idx=int(idx) if idx is not None else None,
                )
            )
        return orders

    @staticmethod
    def _resolve(value, row: pd.Series):
        """replace {col} placeholders with values from rows"""
        if isinstance(value, str) and value.startswith("{") and value.endswith("}"):
            col = value[1:-1]
            # return row[col]
            resolved = row[col]
        else:
            resolved = value

        if isinstance(resolved, np.generic):
            return resolved.item()
        return resolved

    @staticmethod
    def _resolve_optional_str(value, row: pd.Series) -> Optional[str]:
        if value is None:
            return None
        resolved = TemplateOrderBuilder._resolve(value, row)
        if resolved is None or (isinstance(resolved, float) and pd.isna(resolved)):
            return None
        return str(resolved)

    @staticmethod
    def _resolve_optional_float(value, row: pd.Series) -> Optional[float]:
        if value is None:
            return None
        resolved = TemplateOrderBuilder._resolve(value, row)
        if resolved is None or (isinstance(resolved, float) and pd.isna(resolved)):
            return None
        return float(resolved)
