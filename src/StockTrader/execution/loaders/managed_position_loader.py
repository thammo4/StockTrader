#
# FILE: `StockTrader/src/StockTrader/execution/loaders/managed_position_loader.py`
#

import pandas as pd

from StockTrader.settings import logger
from StockTrader.execution.order_iface import DataLoader
from StockTrader.execution.strategy_position_registry import StrategyPositionRegistry


class ManagedPositionLoader(DataLoader):

     def __init__(
          self,
          inner_loader: DataLoader,
          registry: StrategyPositionRegistry,
          strategy: str
     ):
          self._inner = inner_loader
          self._registry = registry
          self._strategy = strategy

     def load(self, **kwargs) -> pd.DataFrame:

          #
          # Load Current Portfolio Snapshot
          #

          df = self._inner.load(**kwargs)

          if df.empty:
               return df

          if "occ" not in df.columns:
               raise ValueError(
                    "Portfolio snapshot missing occ column "
                    "[ManagedPositionLoader]"
               )

          #
          # Load Enabled Strategy Positions
          #

          df_registry = self._registry.load_active(
               strategy=self._strategy
          )

          if df_registry.empty:
               logger.info(
                    f"No active registered positions: "
                    f"strategy={self._strategy} "
                    f"[managed_position_loader]"
               )

               return df.iloc[0:0].copy()

          #
          # Validate Registry Columns
          #

          required = {
               "occ",
               "symbol",
               "strategy",
               "capture_threshold",
               "price_point",
               "entry_order_id"
          }

          missing = required - set(df_registry.columns)

          if missing:
               raise ValueError(
                    f"Strategy registry missing cols: "
                    f"{sorted(missing)} "
                    f"[ManagedPositionLoader]"
               )

          #
          # Normalize OCC Keys
          #

          df = df.copy()
          df_registry = df_registry.copy()

          df["occ"] = (
               df["occ"]
               .astype(str)
               .str.strip()
               .str.upper()
          )

          df_registry["occ"] = (
               df_registry["occ"]
               .astype(str)
               .str.strip()
               .str.upper()
          )

          #
          # Registry OCC Must Be Unique Within Strategy
          #

          if df_registry["occ"].duplicated().any():
               duplicates = (
                    df_registry.loc[
                         df_registry["occ"].duplicated(keep=False),
                         "occ"
                    ]
                    .unique()
                    .tolist()
               )

               raise ValueError(
                    f"Duplicate registered OCCs: {duplicates} "
                    f"[ManagedPositionLoader]"
               )

          #
          # Prepare Strategy Policy Columns
          #

          df_registry = df_registry[
               [
                    "occ",
                    "symbol",
                    "strategy",
                    "capture_threshold",
                    "price_point",
                    "entry_order_id"
               ]
          ].rename(
               columns={
                    "symbol": "registry_symbol",
                    "capture_threshold": "policy_capture_threshold",
                    "price_point": "policy_price_point"
               }
          )

          #
          # Restrict Snapshot to Positions Registered for Strategy
          #

          n_positions = len(df)

          df = df.merge(
               df_registry,
               on="occ",
               how="inner",
               validate="many_to_one"
          )

          #
          # Exclude Managed Positions Without Quote Data
          #
          # PositionQuotes left-joins quotes onto positions; a quote miss leaves
          # the quote-derived symbol null. Excluding here prevents one missing
          # quote from failing symbol validation for the whole batch.
          #

          n_unquoted = 0

          if not df.empty and "symbol" in df.columns:
               is_unquoted = df["symbol"].isna()
               n_unquoted = int(is_unquoted.sum())

               if n_unquoted > 0:
                    logger.warning(
                         f"Excluding unquoted managed positions: "
                         f"{df.loc[is_unquoted, 'occ'].tolist()} "
                         f"[managed_position_loader]"
                    )

                    df = df.loc[~is_unquoted].copy()

          #
          # Validate Registry Symbol Against Current Position
          #

          if not df.empty and "symbol" in df.columns:
               is_symbol_match = (
                    df["symbol"]
                    .astype(str)
                    .str.strip()
                    .str.upper()
                    == df["registry_symbol"]
                    .astype(str)
                    .str.strip()
                    .str.upper()
               )

               if not is_symbol_match.all():
                    bad = df.loc[
                         ~is_symbol_match,
                         [
                              "occ",
                              "symbol",
                              "registry_symbol"
                         ]
                    ]

                    raise ValueError(
                         f"Registry symbol mismatch:\n"
                         f"{bad.to_string(index=False)} "
                         f"[ManagedPositionLoader]"
                    )

          #
          # Registry Symbol Is Only Used for Validation
          #

          df.drop(
               columns=["registry_symbol"],
               inplace=True
          )

          logger.info(
               f"ManagedPositionLoader: "
               f"strategy={self._strategy}, "
               f"n_positions={n_positions}, "
               f"n_unquoted={n_unquoted}, "
               f"n_managed={len(df)} "
               f"[managed_position_loader]"
          )

          return df
