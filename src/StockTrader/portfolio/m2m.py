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

        df["quantity"] = pd.to_numeric(df["quantity"])
        df["cost_basis"] = pd.to_numeric(df["cost_basis"])
        df["mid_price"] = pd.to_numeric(df["mid_price"])
        # df["contract_size"] = pd.to_numeric(df.get("contract_size", 100)).fillna(100)
        df["contract_size"] = pd.to_numeric(df["contract_size"])

        df["market_value"] = df["mid_price"] * df["quantity"] * df["contract_size"]
        df["upl"] = df["market_value"] - df["cost_basis"]

        df["upl_pct"] = df["upl"] / df["cost_basis"].abs() * 100

        if "date_acquired" in df.columns:
            # df["date_acquired"] = pd.to_datetime(df["date_acquired"])
            df["date_acquired"] = pd.to_datetime(df["date_acquired"]).dt.tz_localize(None)
            df["days_held"] = (pd.Timestamp.now() - df["date_acquired"]).dt.days

        df["upl"] = np.round(df["upl"], 2)
        df["upl_pct"] = np.round(df["upl_pct"], 2)

        logger.info(f"Computed P/L for n={len(df)} positions [m2m]")

        return df

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
