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

        n = len(df)

        n_call = sum(df["option_type"] == "call")
        n_put = sum(df["option_type"] == "put")

        n_standard_type = sum(df["expiry_type"] == "standard")
        n_weekly_type = sum(df["expiry_type"] == "weeklys")
        n_other_type = n - (n_standard_type + n_weekly_type)

        n_standard_pct = n_standard_type / n
        n_weekly_pct = n_weekly_type / n
        n_other_pct = 1 - (n_standard_pct + n_weekly_pct)

        n_win = sum(df["upl"] > 0)
        n_lose = sum(df["upl"] < 0)
        win_pct = n_win / n

        market_value = sum(df["market_value"])
        upl = sum(df["upl"])
        upl_pct_mean = float(df["upl_pct"].mean())

        abs_cost = sum(df["cost_basis"].abs())
        cost_basis_mean = float(df["cost_basis"].mean())
        cost_basis_sd = float(df["cost_basis"].std())

        days_held_max = max(df["days_held"])
        days_held_min = min(df["days_held"])
        days_held_mean = float(df["days_held"].mean())

        return {
            "n": n,
            "n_call": n_call,
            "n_put": n_put,
            "n_std": n_standard_type,
            "n_wkly": n_weekly_type,
            "n_other": n_other_type,
            "n_std_pct": round(n_standard_pct, 4),
            "n_wkly_pct": round(n_weekly_pct, 4),
            "n_other_pct": round(n_other_pct, 4),
            "n_win": n_win,
            "n_lose": n_lose,
            "win_pct": round(win_pct, 4),
            "market_value": round(market_value, 2),
            "upl": round(upl, 2),
            "upl_pct_mean": round(upl_pct_mean, 4),
            "abs_cost": round(abs_cost, 2),
            "cost_basis_mean": round(cost_basis_mean, 2),
            "cost_basis_sd": round(cost_basis_sd, 2),
            "days_held_max": days_held_max,
            "days_held_min": days_held_min,
            "days_held_mean": round(days_held_mean, 2),
        }
