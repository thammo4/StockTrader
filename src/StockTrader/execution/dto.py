#
# FILE: `StockTrader/src/StockTrader/execution/dto.py`
#

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import List, Optional, Union


class AssetType(Enum):
    EQUITY = "equity"
    OPTION = "option"


class OrderSide(Enum):
    BUY_TO_OPEN = "buy_to_open"
    SELL_TO_OPEN = "sell_to_open"
    BUY_TO_CLOSE = "buy_to_close"
    SELL_TO_CLOSE = "sell_to_close"
    BUY = "buy"
    BUY_TO_COVER = "buy_to_cover"
    SELL = "sell"
    SELL_SHORT = "sell_short"


class MLegOrderType(Enum):
    MARKET = "market"
    DEBIT = "debit"
    CREDIT = "credit"
    EVEN = "even"


class OrderType(Enum):
    MARKET = "market"
    LIMIT = "limit"
    STOP = "stop"
    STOP_LIMIT = "stop_limit"


@dataclass(frozen=True)
class CancelRequest:
    order_id: int
    asset_type: AssetType
    df_idx: Optional[int] = None


@dataclass
class CancelResult:
    request: CancelRequest
    success: bool
    error: Optional[str] = None
    cancel_ts: Optional[str] = None


@dataclass(frozen=True)
class OrderLeg:
    asset_type: AssetType
    symbol: str
    side: OrderSide
    quantity: int
    occ: Optional[str] = None


@dataclass(frozen=True)
class OrderRequest:
    asset_type: AssetType
    symbol: str
    side: OrderSide
    quantity: int
    order_type: OrderType
    occ: Optional[str] = None
    limit_price: Optional[float] = None
    stop_price: Optional[float] = None
    duration: str = "day"
    df_idx: Optional[int] = None
    mdata: Optional[dict] = field(default_factory=dict)


@dataclass(frozen=True)
class MLegOrderRequest:
    legs: List[OrderLeg]
    order_type: MLegOrderType
    symbol: str
    limit_price: Optional[float] = None
    duration: str = "day"
    df_idx: Optional[int] = None
    mdata: Optional[dict] = field(default_factory=dict)


@dataclass
class ExecutionResult:
    request: "OrderRequestUnion"
    success: bool
    order_id: Optional[int] = None
    error: Optional[str] = None
    submit_ts: Optional[str] = None


OrderRequestUnion = Union[OrderRequest, MLegOrderRequest]
