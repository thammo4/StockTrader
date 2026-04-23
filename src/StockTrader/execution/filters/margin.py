#
# FILE: `StockTrader/src/StockTrader/execution/filters/margin.py`
#

import pandas as pd
from StockTrader.settings import logger

def estimate_margin_reg_t (df: pd.DataFrame) -> pd.Series:
	spot_prices = df["spot_price"]
	strike_prices = df["strike_price"]
	premium_prices = df["entry_price"]
	is_call = df["option_type"] == "call"

	otm_call = (strike_prices - spot_prices).clip(lower=0)
	otm_put = (spot_prices - strike_prices).clip(lower=0)
	otm_amt = otm_call.where(is_call, otm_put)

	f_a = 0.20 * spot_prices - otm_amt + premium_prices
	f_b = 0.10 * spot_prices.where(is_call, strike_prices) + premium_prices

	margin_per_share = pd.concat([f_a, f_b], axis=1).max(axis=1)
	margin_per_contract = 100.0 * margin_per_share

	return margin_per_contract
