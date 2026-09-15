#
# FILE: `StockTrader/src/StockTrader/execution/filters/open_order_exclusion.py`
#

import pandas as pd

from StockTrader.settings import logger
from StockTrader.execution.order_iface import DataLoader


class OpenOrderExclusionLoader(DataLoader):

     #
     # Tradier order statuses that can still fill
     #

     WORKING_STATUSES = {
          "open",
          "partially_filled",
          "pending",
     }

     def __init__(
          self,
          inner_loader: DataLoader,
          account_client
     ):
          self._inner = inner_loader
          self._account = account_client

     def load(self, **kwargs) -> pd.DataFrame:

          df = self._inner.load(**kwargs)

          if df.empty:
               return df

          #
          # Load Account Orders
          #
          # Fail closed: API exceptions propagate so no orders are submitted
          # without known order state.
          #

          df_orders = self._account.get_orders()

          if df_orders is None:
               return df

          if not isinstance(df_orders, pd.DataFrame):
               raise TypeError(
                    f"Expected DataFrame from get_orders, got {type(df_orders).__name__} "
                    f"[OpenOrderExclusionLoader]"
               )

          if df_orders.empty:
               return df

          missing = {"status", "option_symbol"} - set(df_orders.columns)

          if missing:
               raise ValueError(
                    f"Orders payload missing cols: {sorted(missing)} "
                    f"[OpenOrderExclusionLoader]"
               )

          #
          # Exclude OCCs With Any Working Order
          #
          # Prevents resubmitting a close every monitor cycle while a prior
          # limit is unfilled or partially filled.
          #

          is_working = (
               df_orders["status"]
               .astype(str)
               .str.strip()
               .str.lower()
               .isin(self.WORKING_STATUSES)
          )

          working_occs = set(
               df_orders.loc[is_working, "option_symbol"]
               .dropna()
               .astype(str)
               .str.strip()
               .str.upper()
          )

          is_excluded = (
               df["occ"]
               .astype(str)
               .str.strip()
               .str.upper()
               .isin(working_occs)
          )

          if is_excluded.any():
               logger.info(
                    f"OpenOrderExclusionLoader: "
                    f"excluded_occs={sorted(df.loc[is_excluded, 'occ'])} "
                    f"[open_order_exclusion]"
               )

          logger.info(
               f"OpenOrderExclusionLoader: "
               f"n0={len(df)}, "
               f"n_working_occs={len(working_occs)}, "
               f"n_close={int((~is_excluded).sum())} "
               f"[open_order_exclusion]"
          )

          return df.loc[~is_excluded]
