#
# FILE: `StockTrader/utils/vol_estimate.py`
#
# For a given input symbol, this function will:
# 	• Retrieve M-weeks' of historical closing price data preceding today's date
# 	• Compute daily log-return
# 	• Use the log-return to estimate the annualized N-day window rolling volatility
#

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import numpy as np
from datetime import datetime, timedelta
from StockTrader.tradier import quotes, quotesL

def vol_estimate (symbol, weeks=2, window_days=9, is_live=False):
	"""
	Compute annualized volatility estimate for a stock symbol per historical closing price data.

	This function retrieves historical closing price data for given symbol, computes daily log-returns, then computes
	annualized volatility estimate using a rolling window standard deviation.

	
	Params
	-----------
	symbol: str
		Stock ticker symbol

	weeks: int, default=2
		Number of weeks of historical data to retrieve prior to today's date

	window_days: int, default=9
		Size of rolling window (e.g. number of trading days' closing prices used to compute volatility)

	is_live: bool, default=False
		Tradier Account - live or paper

	
	Returns
	-----------
	float:
		Annualized volatility estimate per recent window_days of data

	
	Reference
	-----------
	'Options, Futures, and Other Derivatives' by John C. Hull [Chapter: The Black-Scholes-Merton Model > Volatility]
	"""
	q = quotesL if is_live else quotes
	df = quotes.get_historical_quotes(
		symbol = symbol,
		start_date = (datetime.today() - timedelta(weeks=weeks)).strftime("%Y-%m-%d")
	)
	σ_hat = np.log(df["close"]).diff().rolling(window=9).std().iloc[-1] # index -1 will be the most recent trading day's volatility
	return float(σ_hat * np.sqrt(252))