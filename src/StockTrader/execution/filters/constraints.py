#
# FILE: `StockTrader/src/StockTrader/execution/filters/constraints.py`
#

import pandas as pd
from abc import ABC, abstractmethod
from StockTrader.settings import logger
from StockTrader.execution.filters.portfolio_state import PortfolioState


class CandidateConstraint(ABC):
    @abstractmethod
    def apply(self, df: pd.DataFrame, state: PortfolioState) -> pd.DataFrame:
        pass


class PositionSizeConstraint(CandidateConstraint):
    def __init__(self, max_position_proportion: float = 0.05):
        self.max_position_proportion = max_position_proportion

    def apply(self, df: pd.DataFrame, state: PortfolioState) -> pd.DataFrame:
        if df.empty:
            return df


class MinCreditConstraint(CandidateConstraint):
    def __init__(self, min_credit_proportion: float = 0.005):
        self.min_credit_proportion = min_credit_proportion

    def apply(self, df: pd.DataFrame, state: PortfolioState) -> pd.DataFrame:
        if df.empty:
            return df


class DuplicatePositionConstraint(CandidateConstraint):
    def apply(self, df: pd.DataFrame, state: PortfolioState) -> pd.DataFrame:
        if df.empty:
            return df
