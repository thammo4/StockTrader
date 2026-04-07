#
# FILE: `StockTrader/src/StockTrader/execution/portfolio.py`
#

import pandas as pd
from pathlib import Path

from StockTrader.settings import logger



class TradierPositionMonitor:
	""" Reconcile broker records with local records """
    def __init__(self, acct, order_log_path: Path):
        self._acct = acct
        self._log_path = order_log_path

    def refresh(self) -> pd.DataFrame:
        positions = self._acct.get_positions()

        if positions.empty:
            return pd.DataFrame()

        positions = positions.rename(columns={"symbol": "occ"})

        if self._log_path.exists():
            log = pd.read_parquet(self._log_path)
            filled = log[log["status"] == "FILLED"]
            positions = positions.merge(
                filled[
                    [
                        "occ",
                        "entry_credit",
                        "fill_price",
                        "filled_at",
                        "entry_iv",
                        "entry_sigma",
                        "entry_vrp_spread",
                        "entry_vrp_ratio",
                        "entry_iv_cdf",
                        "entry_delta",
                        "entry_theta",
                        "entry_spot_price",
                        "entry_dte",
                    ]
                ],
                on="occ",
                how="left",
            )

        return positions


class BasicM2M:
	"""Valuation of outstanding positions - pull quotes, compute upl"""
    def __init__(self, quotes):
        self._quotes = quotes

    def mark(self, positions: pd.DataFrame) -> pd.DataFrame:
        if positions.empty:
            return positions

        occs = positions["occ"].tolist()
        df_live = self._quotes.get_quote_data(occs).rename(columns={"symbol": "occ"})

        df_marked = positions.merge(df_live[["occ", "bid", "ask", "last"]], on="occ", how="left")

        qty = df_marked["quantity"].abs()

        df_marked["close_cost_mid"] = 50 * (df_marked["bid"] + df_marked["ask"]) * qty
        df_marked["close_cost_bid"] = 100 * df_marked["bid"] * qty
        df_marked["close_cost_ask"] = 100 * df_marked["ask"] * qty

        df_marked["upl_mid"] = df_marked["entry_credit"] - df_marked["close_cost_mid"]
        df_marked["upl_ask"] = df_marked["entry_credit"] - df_marked["close_cost_ask"]

        df_marked["upl_pct"] = df_marked["upl_ask"] / df_marked["entry_credit"]

        return df_marked


class ThresholdExitEvaluator:
    def __init__(self, profit_target_pct: float, stop_loss_pct: float, min_dte: int):
        self._profit_target = profit_target_pct
        self._stop_loss = stop_loss_pct
        self._min_dte = min_dte

    def evaluate(self, marked_positions: pd.DataFrame) -> pd.DataFrame:
        exits = []

        for _, pos in marked_positions.iterrows():
            reason = None
            priority = 0

            pnl_pct = pos.get("upl_pct", 0)
            dte = pos.get("entry_dte")

            if pnl_pct <= self._stop_loss:
                reason = "stop_loss"
                priority = 1

            elif dte is not None and dte <= self._min_dte:
                reason = "dte_threshold"
                priority = 2

            elif pnl_pct >= self._profit_target:
                reason = "profit_target"
                priority = 3

            if reason:
                row = pos.to_dict()
                row["exit_reason"] = reason
                row["exit_priority"] = priority
                exits.append(row)

        return pd.DataFrame(exits).sort_values("exit_priority") if exits else pd.DataFrame()
