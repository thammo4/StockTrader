#
# FILE: `StockTrader/src/StockTrader/execution/filters/premium_capture.py`
#

import numpy as np
import pandas as pd

from StockTrader.settings import logger
from StockTrader.execution.order_iface import DataLoader


class PremiumCaptureLoader(DataLoader):

	PRICE_COLUMNS = {
		"mid": "mid_price",
		"bid": "bid_price",
		"ask": "ask_price",
	}

	def __init__(
		self,
		inner_loader: DataLoader,
		capture_threshold: float = 0.85,
		price_point: str = "mid",
		eligible_occs=None,
	):
		if not 0.0 < capture_threshold <= 1.0:
			raise ValueError(
				"capture_threshold must be in (0,1] [PremiumCaptureLoader]"
			)

		if price_point not in self.PRICE_COLUMNS:
			raise ValueError(
				f"price_point must be in {self.PRICE_COLUMNS} [PremiumCaptureLoader]"
			)

		self._inner = inner_loader
		self._capture_threshold = capture_threshold
		self._price_point = price_point
		self._eligible_occs = (
			set(eligible_occs)
			if eligible_occs is not None
			else None
		)

	def load(self, **kwargs) -> pd.DataFrame:

		df = self._inner.load(**kwargs)

		if df.empty:
			return df

		required = {
			"symbol",
			"occ",
			"quantity",
			"cost_basis",
			"market_value",
			"mid_price",
			"bid_price",
			"ask_price",
		}

		missing = required - set(df.columns)

		if missing:
			raise ValueError(
				f"Portfolio snapshot missing cols: "
				f"{sorted(missing)} [PremiumCaptureLoader]"
			)

		df = df.copy()

		#
		# Short Options Only
		#
		df = df[
			(pd.to_numeric(df["quantity"], errors="coerce") < 0)
			& (pd.to_numeric(df["cost_basis"], errors="coerce") < 0)
		]

		if df.empty:
			return df

		#
		# Restrict to Positions Managed by Premium Capture
		#
		if self._eligible_occs is not None:
			df = df[df["occ"].isin(self._eligible_occs)]

		if df.empty:
			return df

		#
		# Premium Capture
		#
		df["premium_received"] = df["cost_basis"].abs()
		df["premium_remaining"] = df["market_value"].abs()

		df["premium_capture"] = (
			1.0
			- (
				df["premium_remaining"]
				/ df["premium_received"]
			)
		).round(4)

		df["premium_capture_pct"] = (
			100.0 * df["premium_capture"]
		)

		#
		# Close Order Fields
		#
		price_col = self.PRICE_COLUMNS[self._price_point]

		df["close_quantity"] = (
			df["quantity"]
			.abs()
			.astype(int)
		)

		df["close_price"] = pd.to_numeric(
			df[price_col],
			errors="coerce",
		)

		df["capture_threshold"] = self._capture_threshold
		df["close_price_point"] = self._price_point

		#
		# Filter Eligible Closing Orders
		#
		is_valid_price = (
			np.isfinite(df["close_price"])
			& (df["close_price"] > 0)
		)

		is_ok_capture = (
			df["premium_capture"]
			>= self._capture_threshold
		)

		n0 = len(df)

		df = df[
			is_valid_price
			& is_ok_capture
		]

		logger.info(
			f"PremiumCaptureLoader: "
			f"threshold={self._capture_threshold:.2%}, "
			f"price={self._price_point}, "
			f"n0={n0}, "
			f"n_close={len(df)} "
			f"[premium_capture]"
		)

		return df
