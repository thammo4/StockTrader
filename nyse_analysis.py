import os, dotenv;
import random;
import pandas as pd;
import numpy as np;
import yfinance as yf;
from datetime import datetime, timedelta;
import matplotlib.pyplot as plt;

from uvatradier import Account, Quotes, EquityOrder;
from nyse_sectors import nyse_sectors; # gives access to sector-sorted dictionary of NYSE symbols

import warnings;
warnings.filterwarnings('ignore');

dotenv.load_dotenv();

tradier_acct = os.getenv("tradier_acct");
tradier_token = os.getenv("tradier_token");

acct = Account(tradier_acct, tradier_token);
quotes = Quotes(tradier_acct, tradier_token);
equity_order = EquityOrder(tradier_acct, tradier_token);

today = datetime.today().strftime("%Y-%m-%d");

nyse_sector_names = list(nyse_sectors.keys());

# >>> nyse_sector_names
# [
# 	'Healthcare', 'Basic Materials', 'Financial Services', 'Industrials', 'Consumer Cyclical', 'Real Estate',
# 	'Consumer Defensive', 'Technology', 'Utilities', 'Unknown', 'Energy', 'Communication Services'
# ]


#
# Produce start/end dates for a desired trading window size
#

def window_to_dates (window_size, start_date=None, end_date=None):
	#
	# Perform some date / window_size checks
	#

	if isinstance(start_date, str):
		start_date = datetime.strptime(start_date, "%Y-%m-%d");

	if isinstance(end_date, str):
		end_date = datetime.strptime(end_date, "%Y-%m-%d");

	# if start_date is None and end_date is None and window_size is None:
	# 	raise ValueError("cmon");

	if start_date and window_size and end_date is None:
		end_date = start_date + timedelta(days=window_size);

	elif end_date and window_size and start_date is None:
		start_date = end_date - timedelta(days=window_size);

	elif window_size and start_date is None and end_date is None:
		end_date = datetime.now();
		start_date = end_date - timedelta(days=window_size);

	elif start_date and end_date and window_size:
		if (end_date - start_date).days != window_size:
			raise ValueError("Date arguments make no sense");

	if start_date is None or end_date is None:
		raise ValueError("Cant figure out start/end dates");

	start_date_string = start_date.strftime("%Y-%m-%d");
	end_date_string = end_date.strftime("%Y-%m-%d");

	return [start_date_string, end_date_string];



#
# Retrieve past historical adjusted closing prices from Yahoo Finance API
#

def get_sector_adjClose (sector, start_date, end_date):
	nyse_sector_symbols = nyse_sectors[sector];
	nyse_sector_adjClose = yf.download(nyse_sector_symbols, start=start_date, end=end_date)['Adj Close'];

	if nyse_sector_adjClose.empty:
		raise ValueError(f"No data found for {sector}: {start_date_string} -> {end_date_string}");

	return nyse_sector_adjClose



#
# Compute intra-sector Z-scores of log-returns
#

def nyse_sector_Zreturns (sector_data):
	stocks_daily_returns = np.log(sector_data / sector_data.shift(1));

	stocks_agg_mean_returns = stocks_daily_returns.mean();
	stocks_agg_std_returns = stocks_daily_returns.std();

	sector_mean_return = stocks_agg_mean_returns.mean();
	sector_std_return = stocks_agg_std_returns.mean();
	sector_volatility = stocks_agg_std_returns.std();

	stocks_Zreturns = (stocks_agg_mean_returns - sector_mean_return) / sector_std_return;

	return stocks_Zreturns;



#
# Examine Series of Z-scores of log-returns and purcahse top 25% of returns in sector
#

def buy_sector_stocks (Zscores):
	bought_symbols = [];
	df_Zscores = Zscores.reset_index();
	df_Zscores.columns = ['Ticker', 'Zscore'];

	for row in df_Zscores.itertuples(index=False):
		if row.Zscore > .6745:
			print(f"Submitting market order for 10 shares {row.Ticker}.");
			equity_order.order(symbol=row.Ticker, side='buy', quantity=10, order_type='market');
			bought_symbols.append(row.Ticker);

	return bought_symbols;




#
# Identify stocks to purchase
# 	• Iterate over each nyse sector
# 	• Fetch historical adjusted closing price data for a given past trading window
# 	• Compute intra-sector Z-scores for log-returns of each stock in the sector
# 	• Iterate over each stock in the sector and buy those whose log-returns have Z-score > .6745 (prob of log-returns ≈ .750)
#

def Zscore_strategy():
	bought_sector_stocks = {sector: [] for sector in nyse_sector_names};
	window90 = window_to_dates(window_size=90);
	for nyse_sector in nyse_sector_names:
		print(f"COMPUTING Z-SCORES FOR SECTOR: {nyse_sector}.");
		sector_data = get_sector_adjClose(sector=nyse_sector, start_date=window90[0], end_date=window90[1]);
		sector_Zscores = nyse_sector_Zreturns(sector_data);
		stocks_to_buy = buy_sector_stocks(sector_Zscores);
		bought_sector_stocks[nyse_sector] = stocks_to_buy;
		print(f"Bought: {stocks_to_buy}");
		print("\n");


#
# OUTPUT
#

# COMPUTING Z-SCORES FOR SECTOR: Healthcare.
# [*********************100%%**********************]  118 of 118 completed
# Bought: []
#
# COMPUTING Z-SCORES FOR SECTOR: Basic Materials.
# [*********************100%%**********************]  149 of 149 completed
#
# 1 Failed download:
# ['SAND          ']: Exception('%ticker%: No timezone found, symbol may be delisted')
# Bought: []
#
#
# COMPUTING Z-SCORES FOR SECTOR: Financial Services.
# [*********************100%%**********************]  622 of 622 completed
#
# 1 Failed download:
# ['ETY']: Exception('%ticker%: No price data found, symbol may be delisted (1d 2024-03-29 -> 2024-06-27)')
# Bought: []
#
#
# COMPUTING Z-SCORES FOR SECTOR: Industrials.
# [*********************100%%**********************]  309 of 309 completed
# Bought: []
#
#
# COMPUTING Z-SCORES FOR SECTOR: Consumer Cyclical.
# [*********************100%%**********************]  257 of 257 completed
# Bought: []
#
#
# COMPUTING Z-SCORES FOR SECTOR: Real Estate.
# [*********************100%%**********************]  186 of 186 completed
# Bought: []
#
#
# COMPUTING Z-SCORES FOR SECTOR: Consumer Defensive.
# [*********************100%%**********************]  101 of 101 completed
# Bought: []
#
#
# COMPUTING Z-SCORES FOR SECTOR: Technology.
# [*********************100%%**********************]  200 of 200 completed
# Bought: []
#
#
# COMPUTING Z-SCORES FOR SECTOR: Utilities.
# [*********************100%%**********************]  76 of 76 completed
# Bought: []
#
#
# COMPUTING Z-SCORES FOR SECTOR: Unknown.
# [*********************100%%**********************]  142 of 142 completed
#
# 5 Failed downloads:
# ['ETX           ', 'ECC           ']: Exception('%ticker%: No timezone found, symbol may be delisted')
# ['SFB']: Exception("%ticker%: Period 'max' is invalid, must be one of ['1d', '5d']")
# ['EMP', 'EAI']: Exception('%ticker%: No price data found, symbol may be delisted (1d 2024-03-29 -> 2024-06-27)')
# Submitting market order for 10 shares TPTA.
# Bought: ['TPTA']
#
#
# COMPUTING Z-SCORES FOR SECTOR: Energy.
# [*********************100%%**********************]  178 of 178 completed
# Bought: []
#
#
# COMPUTING Z-SCORES FOR SECTOR: Communication Services.
# [*********************100%%**********************]  78 of 78 completed
# Bought: []
#
#
# >>> bought_sector_stocks
# {'Healthcare': [], 'Basic Materials': [], 'Financial Services': [], 'Industrials': [], 'Consumer Cyclical': [], 'Real Estate': [], 'Consumer Defensive': [], 'Technology': [], 'Utilities': [], 'Unknown': ['TPTA'], 'Energy': [], 'Communication Services': []}