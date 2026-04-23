#
# FILE: `StockTrader/src/StockTrader/execution/filters/filtered_loader.py`
#

import numpy as np
import pandas as pd
from typing import List

from StockTrader.settings import logger
from StockTrader.execution.order_iface import DataLoader
from StockTrader.execution.filters.portfolio_state import PortfolioState
from StockTrader.execution.filters.margin import estimate_margin_reg_t
from StockTrader.execution.filters.ksack import solve_ksack01
from StockTrader.portfolio.position_loader import PositionLoader


class FilteredCandidateLoader(DataLoader):
    def __init__(
        self,
        inner_loader: DataLoader,
        account_client,
        max_position_frac: float = 0.05,
        max_portfolio_margin_frac: float = 0.80,
        min_credit_frac: float = 0.005,
        ok_duplicate_symbols: bool = False,
    ):
        self._inner = inner_loader
        self._account = account_client
        self._max_position_frac = max_position_frac
        self._max_portfolio_margin_frac = max_portfolio_margin_frac
        self._min_credit_frac = min_credit_frac
        self._ok_duplicate_symbols = ok_duplicate_symbols

    def load(self, **kwargs) -> pd.DataFrame:
        df = self._inner.load(**kwargs)
        if df.empty:
            return df

        n0 = len(df)

        l = PositionLoader(self._account)
        df_positions = l.load_options_positions()
        portfolio_state = PortfolioState.build(self._account, df_positions)

        logger.info(
            f"FilteredCandidateLoader: n0={n0}, equity={portfolio_state.total_equity:.2f}, optionbp={portfolio_state.option_buying_power:.2f}, req={portfolio_state.current_requirement:.2f}"
        )

        #
        # 1. Symbol DeDup
        #

        if not self._ok_duplicate_symbols:
            open_symbols = portfolio_state.open_symbols
            if open_symbols:
                n_prior = len(df)
                df = df[~df["symbol"].isin(open_symbols)]
                logger.info(f"DuplicateFilter: {n_prior} -> {len(df)} (removed={n_prior-len(df)})[filtered_loader]")

        if df.empty:
            return df

        #
        # 2. Attach Reg T Margin Per Candidate
        #

        df = df.copy()
        df["margin_est"] = estimate_margin_reg_t(df)

        #
        # 3. Apply Per-Position Margin Capacity
        #

        max_position_margin = portfolio_state.option_buying_power * self._max_position_frac
        n_prior = len(df)
        df = df[df["margin_est"] <= max_position_margin]
        logger.info(
            f"PositionSizeCap: max={max_position_margin:.2f}, Δn={n_prior}-{len(df)}={n_prior-len(df)} [filtered_loader]"
        )

        if df.empty:
            return df

        #
        # 4. Apply portfolio margin-cap credit max ksack
        #

        portfolio_margin_cap = portfolio_state.total_equity * self._max_portfolio_margin_frac
        remaining_cap = max(0.0, portfolio_margin_cap - portfolio_state.current_requirement)
        min_credit = portfolio_state.total_equity * self._min_credit_frac

        credits = df["credit_price"].values.astype(float)
        margins = df["margin_est"].values.astype(float)

        selected = solve_ksack01(credits, margins, remaining_cap, min_credit)

        df = df[selected]

        # df = df.drop(columns=["margin_est"])

        logger.info(f"FilteredCandidateLoader: n0={n0}, n1={len(df)} [filtered_loader]")

        return df
