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
          if not 0.0 < capture_threshold < 1.0:
               raise ValueError(
                    "capture_threshold must be in (0,1) [PremiumCaptureLoader]"
               )

          if price_point not in self.PRICE_COLUMNS:
               raise ValueError(
                    f"price_point must be in {sorted(self.PRICE_COLUMNS)} [PremiumCaptureLoader]"
               )

          self._inner = inner_loader
          self._capture_threshold = capture_threshold
          self._price_point = price_point
          self._eligible_occs = (
               set(eligible_occs)
               if eligible_occs is not None
               else None
          )

     #
     # Floor Limit Price to Valid Option Tick
     #
     # $0.05 increments below $3.00 and $0.10 at/above $3.00 are valid for both
     # penny-program and non-penny option classes. Flooring (never rounding up)
     # a buy limit guarantees any fill realizes capture >= limit_capture.
     #

     @staticmethod
     def _floor_tick(price: pd.Series) -> pd.Series:
          tick = np.where(price < 3.0, 0.05, 0.10)
          return (np.floor(price / tick + 1e-9) * tick).round(2)

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
               "n_contracts"
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
          ].copy()

          if df.empty:
               return df

          #
          # Restrict to Positions Managed by Premium Capture
          #
          if self._eligible_occs is not None:
               df = df[df["occ"].isin(self._eligible_occs)].copy()

          if df.empty:
               return df

          #
          # Resolve Per-Position Exit Policy
          #
          # Registry policy columns attached by ManagedPositionLoader take
          # precedence; runtime capture_threshold / price_point are defaults
          # for positions without a policy value.
          #

          if "policy_capture_threshold" in df.columns:
               capture_threshold = (
                    pd.to_numeric(df["policy_capture_threshold"], errors="coerce")
                    .fillna(self._capture_threshold)
               )
          else:
               capture_threshold = pd.Series(
                    self._capture_threshold,
                    index=df.index,
                    dtype=float
               )

          if "policy_price_point" in df.columns:
               price_point = (
                    df["policy_price_point"]
                    .where(df["policy_price_point"].notna(), self._price_point)
                    .astype(str)
                    .str.strip()
                    .str.lower()
               )
          else:
               price_point = pd.Series(
                    self._price_point,
                    index=df.index,
                    dtype=object
               )

          is_bad_policy = (
               ~((capture_threshold > 0) & (capture_threshold < 1))
               | ~price_point.isin(list(self.PRICE_COLUMNS))
          )

          if is_bad_policy.any():
               bad = df.loc[is_bad_policy, ["occ"]].assign(
                    capture_threshold=capture_threshold[is_bad_policy],
                    price_point=price_point[is_bad_policy]
               )

               raise ValueError(
                    f"Invalid position exit policy:\n"
                    f"{bad.to_string(index=False)} "
                    f"[PremiumCaptureLoader]"
               )

          #
          # Premium Capture (Marked at Snapshot Mid)
          #
          df["premium_received"] = df["cost_basis"].abs()
          df["premium_remaining"] = df["market_value"].abs()

          premium_capture = 1.0 - (
               df["premium_remaining"]
               / df["premium_received"]
          )

          #
          # Close Order Fields
          #
          # Limit source price selected per row by resolved price_point,
          # then floored to a valid tick.
          #

          bid = pd.to_numeric(df["bid_price"], errors="coerce")
          ask = pd.to_numeric(df["ask_price"], errors="coerce")
          mid = pd.to_numeric(df["mid_price"], errors="coerce")

          price_matrix = np.column_stack([
               mid.to_numpy(),
               bid.to_numpy(),
               ask.to_numpy()
          ])

          price_idx = (
               price_point
               .map({"mid": 0, "bid": 1, "ask": 2})
               .astype(int)
               .to_numpy()
          )

          df["close_quantity"] = (
               df["quantity"]
               .abs()
               .astype(int)
          )

          df["close_price"] = self._floor_tick(
               pd.Series(
                    price_matrix[np.arange(len(df)), price_idx],
                    index=df.index
               )
          )

          df["close_debit_est"] = (df["close_price"] * df["close_quantity"] * df["n_contracts"])

          limit_capture = 1.0 - (
               df["close_debit_est"]
               / df["premium_received"]
          )

          df["premium_capture"] = premium_capture.round(4)
          df["premium_capture_pct"] = 100.0 * df["premium_capture"]
          df["limit_capture"] = limit_capture.round(4)
          df["limit_capture_pct"] = 100.0 * df["limit_capture"]

          df["capture_threshold"] = capture_threshold
          df["close_price_point"] = price_point

          #
          # Filter Eligible Closing Orders
          #
          # Gates evaluated on unrounded capture values.
          # Quote validity mirrors staging is_valid_price (bid > 0, ask > 0, ask >= bid).
          #

          is_valid_quote = (
               (bid > 0)
               & (ask > 0)
               & (ask >= bid)
          )

          is_valid_price = (
               np.isfinite(df["close_price"])
               & (df["close_price"] > 0)
          )

          is_ok_capture = (
               (premium_capture >= capture_threshold)
               & (limit_capture >= capture_threshold)
          )

          keep = is_valid_quote & is_valid_price & is_ok_capture

          logger.info(
               f"PremiumCaptureLoader: "
               f"default_threshold={self._capture_threshold:.2%}, "
               f"default_price={self._price_point}, "
               f"n0={len(df)}, "
               f"n_bad_quote={int((~is_valid_quote).sum())}, "
               f"n_bad_price={int((is_valid_quote & ~is_valid_price).sum())}, "
               f"n_below_threshold={int((is_valid_quote & is_valid_price & ~is_ok_capture).sum())}, "
               f"n_close={int(keep.sum())} "
               f"[premium_capture]"
          )

          return df[keep]
