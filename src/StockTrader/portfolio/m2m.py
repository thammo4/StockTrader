#
# FILE: `StockTrader/src/StockTrader/portfolio/m2m.py`
#

import pandas as pd
import numpy as np
from StockTrader.settings import logger


class M2MCalc:
    @staticmethod
    def compute_upl(df_enriched: pd.DataFrame) -> pd.DataFrame:
        if df_enriched.empty:
            return df_enriched

        df = df_enriched.copy()

        df["market_value"] = df["mid_price"] * df["quantity"] * df["n_contracts"]
        df["upl"] = df["market_value"] - df["cost_basis"]

        df["upl_pct"] = df["upl"] / df["cost_basis"].abs() * 100

        if "acq_date" in df.columns:
            df["acq_date_tz"] = pd.to_datetime(df["acq_date"]).dt.tz_localize(None)
            df["days_held"] = (pd.Timestamp.now() - df["acq_date_tz"]).dt.days

        df["upl"] = np.round(df["upl"], 2)
        df["upl_pct"] = np.round(df["upl_pct"], 2)

        logger.info(f"Computed P/L for n={len(df)} positions [m2m]")

        cols = [
            "symbol",
            "occ",
            "option_type",
            "expiry_date",
            "expiry_type",
            "n_contracts",
            "strike_price",
            "mid_price",
            "bid_price",
            "ask_price",
            "volume",
            "open_interest",
            "bid_size",
            "ask_size",
            "quantity",
            "cost_basis",
            "market_value",
            "upl",
            "upl_pct",
            "days_held",
            "acq_date",
            "acq_time",
            "tradier_id",
        ]

        return df[cols]

    @staticmethod
    def portfolio_summary(df: pd.DataFrame) -> dict:
        if df.empty:
            return {"market_value": 0.0, "upl": 0.0, "abs_cost": 0.0}

        return {
            "market_value": df["market_value"].sum(),
            "upl": df["upl"].sum(),
            "abs_cost": df["cost_basis"].abs().sum(),
            "n": len(df),
            "n_win": (df["upl"] > 0).sum(),
            "n_lose": (df["upl"] < 0).sum(),
        }
