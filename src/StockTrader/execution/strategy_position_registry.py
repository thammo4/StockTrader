#
# FILE: `StockTrader/src/StockTrader/execution/strategy_position_registry.py`
#

from datetime import datetime, timezone

import pandas as pd

from utils.minio_store import MinioStore


REGISTRY_BUCKET = "strategy-positions"

PRICE_POINTS = {
	"ask",
	"bid",
	"mid",
}

REGISTRY_COLUMNS = [
	"strategy",
	"occ",
	"symbol",
	"capture_threshold",
	"price_point",
	"enabled",
	"entry_order_id",
	"created_ts",
	"updated_ts",
]


class StrategyPositionRegistry:

	def __init__(
		self,
		store: MinioStore,
		bucket: str = REGISTRY_BUCKET
	):
		self._store = store
		self._bucket = bucket

	#
	# Normalize / Validate Registry Fields
	#

	@staticmethod
	def _normalize_strategy(strategy: str) -> str:
		if not strategy:
			raise ValueError(
				"strategy required [StrategyPositionRegistry]"
			)

		strategy = str(strategy).strip().lower()

		if not strategy:
			raise ValueError(
				"strategy required [StrategyPositionRegistry]"
			)

		if "/" in strategy:
			raise ValueError(
				f"Bad strategy={strategy!r}: '/' not allowed "
				f"[StrategyPositionRegistry]"
			)

		return strategy

	@staticmethod
	def _normalize_occ(occ: str) -> str:
		if not occ:
			raise ValueError(
				"occ required [StrategyPositionRegistry]"
			)

		occ = str(occ).strip().upper()

		if not occ:
			raise ValueError(
				"occ required [StrategyPositionRegistry]"
			)

		if "/" in occ:
			raise ValueError(
				f"Bad occ={occ!r}: '/' not allowed "
				f"[StrategyPositionRegistry]"
			)

		return occ

	@staticmethod
	def _normalize_symbol(symbol: str) -> str:
		if not symbol:
			raise ValueError(
				"symbol required [StrategyPositionRegistry]"
			)

		symbol = str(symbol).strip().upper()

		if not symbol:
			raise ValueError(
				"symbol required [StrategyPositionRegistry]"
			)

		return symbol

	@staticmethod
	def _validate_policy(
		capture_threshold: float,
		price_point: str
	):

		if not 0 < capture_threshold <= 1:
			raise ValueError(
				f"capture_threshold={capture_threshold} not in (0,1] "
				f"[StrategyPositionRegistry]"
			)

		if price_point not in PRICE_POINTS:
			raise ValueError(
				f"price_point={price_point!r} not in "
				f"{sorted(PRICE_POINTS)} "
				f"[StrategyPositionRegistry]"
			)

	#
	# Registry Object Address
	#

	@staticmethod
	def _object_name(
		strategy: str,
		occ: str
	) -> str:
		return f"{strategy}/{occ}.parquet"

	#
	# Get One Registered Position
	#

	def get(
		self,
		strategy: str,
		occ: str
	) -> pd.Series | None:

		strategy = self._normalize_strategy(strategy)
		occ = self._normalize_occ(occ)

		#
		# Registry bucket may not exist until first registration
		#

		if self._bucket not in self._store.list_buckets():
			return None

		obj_name = self._object_name(
			strategy=strategy,
			occ=occ
		)

		try:
			df = self._store.read_parquet(
				bucket=self._bucket,
				obj_name=obj_name
			)

		except FileNotFoundError:
			return None

		if df.empty:
			return None

		return df.iloc[0]

	#
	# Register / Update Position
	#

	def register(
		self,
		strategy: str,
		occ: str,
		symbol: str,
		capture_threshold: float = 0.85,
		price_point: str = "mid",
		entry_order_id: int = None,
		enabled: bool = True
	) -> str:

		strategy = self._normalize_strategy(strategy)
		occ = self._normalize_occ(occ)
		symbol = self._normalize_symbol(symbol)

		price_point = str(price_point).strip().lower()
		capture_threshold = float(capture_threshold)

		self._validate_policy(
			capture_threshold=capture_threshold,
			price_point=price_point
		)

		#
		# Preserve Original Registry Creation Time on Update
		#

		existing = self.get(
			strategy=strategy,
			occ=occ
		)

		now = datetime.now(timezone.utc).isoformat()

		if existing is not None and pd.notna(existing.get("created_ts")):
			created_ts = str(existing["created_ts"])
		else:
			created_ts = now

		#
		# Construct Current Registry State
		#

		row = {
			"strategy": strategy,
			"occ": occ,
			"symbol": symbol,
			"capture_threshold": capture_threshold,
			"price_point": price_point,
			"enabled": bool(enabled),
			"entry_order_id": entry_order_id,
			"created_ts": created_ts,
			"updated_ts": now
		}

		df = pd.DataFrame(
			[row],
			columns=REGISTRY_COLUMNS
		)

		obj_name = self._object_name(
			strategy=strategy,
			occ=occ
		)

		return self._store.write_parquet(
			bucket=self._bucket,
			obj_name=obj_name,
			df=df,
			ensure_bucket=True
		)

	#
	# Enable / Disable Registry Position
	#

	def set_enabled(
		self,
		strategy: str,
		occ: str,
		enabled: bool
	) -> str:

		strategy = self._normalize_strategy(strategy)
		occ = self._normalize_occ(occ)

		existing = self.get(
			strategy=strategy,
			occ=occ
		)

		if existing is None:
			raise KeyError(
				f"No registered position "
				f"(strategy={strategy}, occ={occ}) "
				f"[StrategyPositionRegistry]"
			)

		entry_order_id = None

		if (
			"entry_order_id" in existing.index
			and pd.notna(existing["entry_order_id"])
		):
			entry_order_id = int(existing["entry_order_id"])

		return self.register(
			strategy=strategy,
			occ=occ,
			symbol=str(existing["symbol"]),
			capture_threshold=float(
				existing["capture_threshold"]
			),
			price_point=str(
				existing["price_point"]
			),
			entry_order_id=entry_order_id,
			enabled=enabled
		)

	def enable(
		self,
		strategy: str,
		occ: str
	) -> str:

		return self.set_enabled(
			strategy=strategy,
			occ=occ,
			enabled=True
		)

	def disable(
		self,
		strategy: str,
		occ: str
	) -> str:

		return self.set_enabled(
			strategy=strategy,
			occ=occ,
			enabled=False
		)

	#
	# Load Registry
	#

	def load(
		self,
		strategy: str = None,
		enabled_only: bool = False
	) -> pd.DataFrame:

		#
		# Empty Registry
		#

		if self._bucket not in self._store.list_buckets():
			return pd.DataFrame(
				columns=REGISTRY_COLUMNS
			)

		#
		# Optional Strategy Prefix
		#

		if strategy is not None:
			strategy = self._normalize_strategy(strategy)
			prefix = f"{strategy}/"
		else:
			prefix = ""

		try:
			df = self._store.read_prefix(
				bucket=self._bucket,
				prefix=prefix,
				fmt="parquet"
			)

		except FileNotFoundError:
			return pd.DataFrame(
				columns=REGISTRY_COLUMNS
			)

		if df.empty:
			return pd.DataFrame(
				columns=REGISTRY_COLUMNS
			)

		#
		# Normalize Loaded Registry
		#

		for col in REGISTRY_COLUMNS:
			if col not in df.columns:
				df[col] = None

		df = df[REGISTRY_COLUMNS].copy()

		if enabled_only:
			df = df[
				df["enabled"].fillna(False).astype(bool)
			]

		return (
			df
			.sort_values(
				["strategy", "symbol", "occ"]
			)
			.reset_index(drop=True)
		)

	#
	# Load Enabled Positions for One Strategy
	#

	def load_active(
		self,
		strategy: str
	) -> pd.DataFrame:

		return self.load(
			strategy=strategy,
			enabled_only=True
		)
