#
# FILE: `StockTrader/src/StockTrader/execution/filters/portfolio_state.py`
#

# >>> acct.get_account_balance(True)
# option_short_value                                               -19784.0
# total_equity                                                  151677.9018
# account_type                                                       margin
# close_pl                                                            -42.0
# current_requirement                                             121048.65
# equity                                                                  0
# long_market_value                                                       0
# market_value                                                     -19784.0
# open_pl                                                            3245.0
# option_long_value                                                       0
# option_requirement                                              121048.65
# pending_orders_count                                                    0
# short_market_value                                               -19784.0
# stock_long_value                                                        0
# total_cash                                                    171461.9018
# uncleared_funds                                                         0
# pending_cash                                                            0
# margin                  {'fed_call': 0, 'maintenance_call': 0, 'option...
# dtype: object
# >>> acct.get_account_balance(True)['margin']
# {'fed_call': 0, 'maintenance_call': 0, 'option_buying_power': 46969.1518, 'stock_buying_power': 93938.3036, 'stock_short_value': 0, 'sweep': 0}

import pandas as pd
from dataclasses import dataclass
from utils.parse_occ import parse_occ

@dataclass
class PortfolioState:
	option_short_value: float
	total_equity: float
	close_pl: float
	current_requirement: float
	equity: float
	long_market_value: float
	market_value: float
	open_pl: float
	option_long_value: float
	option_requirement: float
	pending_orders_count: int
	short_market_value: float
	stock_long_value: float
	total_cash: float
	margin_maintenance_call: float
	option_buying_power: float
	stock_buying_power: float
	stock_short_value: float
	open_positions: pd.DataFrame

	@property
	def n_open_positions (self) -> int:
		return len(self.open_positions)

	@property
	def open_positions_cost_basis (self) -> float:
		if self.open_positions.empty:
			return 0.0
		return float(self.open_positions["cost_basis"].astype(float).abs().sum())

	@property
	def open_symbols (self) -> set:
		if self.open_positions.empty:
			return set()
		return set(
			self.open_positions["occ"].apply(lambda x: parse_occ(x)["root"])
		)

	@staticmethod
	def build (account_client, df_positions: pd.DataFrame) -> "PortfolioState":
		bal = account_client.get_account_balance(return_as_series=True)

		return PortfolioState(
			option_short_value = bal["option_short_value"],
			total_equity = bal["total_equity"],
			close_pl = bal["close_pl"],
			current_requirement = bal["current_requirement"],
			equity = bal["equity"],
			long_market_value = bal["long_market_value"],
			market_value = bal["market_value"],
			open_pl = bal["open_pl"],
			option_long_value = bal["option_long_value"],
			option_requirement = bal["option_requirement"],
			pending_orders_count = bal["pending_orders_count"],
			short_market_value = bal["short_market_value"],
			stock_long_value = bal["stock_long_value"],
			total_cash = bal["total_cash"],
			margin_maintenance_call = bal["margin"]["maintenance_call"],
			option_buying_power = bal["margin"]["option_buying_power"],
			stock_buying_power = bal["margin"]["stock_buying_power"],
			stock_short_value = bal["margin"]["stock_short_value"],
			open_positions = df_positions
		)
