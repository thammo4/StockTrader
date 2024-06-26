import os, dotenv;
import random;
import pandas as pd;
import numpy as np;
import yfinance as yf;
from datetime import datetime;
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

# >>> list(nyse_sectors.keys())
# [
# 	'Healthcare', 'Basic Materials', 'Financial Services', 'Industrials',
# 	'Consumer Cyclical', 'Real Estate', 'Consumer Defensive', 'Technology',
# 	'Utilities', 'Unknown', 'Energy', 'Communication Services'
# ]

def nyse_sector_Zreturns (sector, start_date, end_date):
	nyse_sector_symbols = nyse_sectors[sector];
	nyse_sector_data = yf.download(nyse_sector_symbols, start=start_date, end=end_date)['Adj Close'];

	#
	# Compute daily log-returns for stocks
	#

	stocks_daily_returns = np.log(nyse_sector_data / nyse_sector_data.shift(1));
	print(f'DAILY LOG-RETURNS [Stocks]\n{stocks_daily_returns}\n');

	#
	# Compute mean, stddev of daily returns for each stock
	#

	stocks_agg_mean_returns = stocks_daily_returns.mean();
	stocks_agg_std_returns = stocks_daily_returns.std();
	print(f"AGGREGATE MEAN LOG-RETURNS [Stocks]\n{stocks_agg_mean_returns}\n");
	print(f"AGGREGATE STDDEV LOG-RETURNS [Stocks]\n{stocks_agg_std_returns}\n");


	#
	# Compute mean, stddev for sector
	#

	sector_mean_return = stocks_agg_mean_returns.mean();
	sector_std_return = stocks_agg_std_returns.std();
	print(f"MEAN LOG-RETURN [Sector]\n{sector_mean_return}\n");
	print(f"STDDEV LOG-RETURN [Sector]\n{sector_std_return}\n");


	#
	# Compute Z-Scores for Stocks' Returns
	#

	stocks_Z_returns = (stocks_agg_mean_returns - sector_mean_return) / sector_std_return;
	print(f"Z-SCORE LOG-RETURNS [Stocks]\n{stocks_Z_returns}\n");

	return stocks_Z_returns;


nyse_industrials_Z = nyse_sector_Zreturns(sector='Industrials', start_date='2024-01-01', end_date=today);





#
# OUTPUT
#

# [*********************100%%**********************]  309 of 309 completed
# DAILY LOG-RETURNS [Stocks]
# Ticker           AAN       ABM       ACA      ACCO      ACHR       ACM       ADT  ...       WTS       XPO       XYL       ZIM       ZIP       ZTO       ZWS
# Date                                                                              ...                                                                      
# 2024-01-02       NaN       NaN       NaN       NaN       NaN       NaN       NaN  ...       NaN       NaN       NaN       NaN       NaN       NaN       NaN
# 2024-01-03 -0.015118 -0.021089 -0.037004 -0.001596 -0.038601 -0.024263 -0.087919  ... -0.029180 -0.027471 -0.018737  0.091322 -0.004431  0.017242 -0.032469
# 2024-01-04  0.017763 -0.002307 -0.007492 -0.006410  0.010676 -0.004271 -0.003120  ... -0.008547 -0.006317  0.006878  0.104463  0.011042 -0.027438  0.004956
# 2024-01-05 -0.003527 -0.019826 -0.001148  0.028528 -0.021468 -0.000789  0.018576  ... -0.007289  0.002556 -0.001604  0.069532  0.010921 -0.026708 -0.013867
# 2024-01-08  0.012292  0.002118  0.012049  0.001561  0.003610  0.003825  0.021245  ...  0.008102  0.013882  0.006222 -0.065133  0.028553 -0.015152  0.004287
# ...              ...       ...       ...       ...       ...       ...       ...  ...       ...       ...       ...       ...       ...       ...       ...
# 2024-06-18 -0.002987  0.002765 -0.002482 -0.010582 -0.035317 -0.000452 -0.006998  ... -0.001812 -0.015558  0.005931  0.008983 -0.044300 -0.015741 -0.012423
# 2024-06-20 -0.004998  0.013128 -0.012982  0.000000  0.019418  0.005187  0.008392  ... -0.005403  0.016796 -0.008155  0.058735  0.002320 -0.028041 -0.003295
# 2024-06-21 -0.007039  0.024422  0.005141  0.000000  0.159630  0.009738  0.076393  ... -0.004139  0.001236 -0.004030 -0.025623  0.018370 -0.006079  0.006251
# 2024-06-24  0.000000 -0.026370  0.002263  0.006363  0.021622  0.014596  0.003864  ...  0.011940  0.001804 -0.002383  0.030077  0.001137  0.032306 -0.012874
# 2024-06-25  0.005033 -0.009800 -0.014140 -0.030045 -0.021622 -0.020404 -0.012937  ... -0.026534 -0.005423 -0.010245  0.022462 -0.013730 -0.021575 -0.012705

# [121 rows x 309 columns]

# AGGREGATE MEAN LOG-RETURNS [Stocks]
# Ticker
# AAN    -0.000788
# ABM     0.001173
# ACA     0.000094
# ACCO   -0.002360
# ACHR   -0.003851
#           ...   
# XYL     0.001597
# ZIM     0.005220
# ZIP    -0.003724
# ZTO     0.000588
# ZWS     0.000214
# Length: 309, dtype: float64

# AGGREGATE STDDEV LOG-RETURNS [Stocks]
# Ticker
# AAN     0.041819
# ABM     0.014553
# ACA     0.017536
# ACCO    0.021582
# ACHR    0.036269
#           ...   
# XYL     0.011563
# ZIM     0.052612
# ZIP     0.024067
# ZTO     0.023966
# ZWS     0.015151
# Length: 309, dtype: float64

# MEAN LOG-RETURN [Sector]
# 0.0002734030523337587

# STDDEV LOG-RETURN [Sector]
# 0.013917301442236232

# Z-SCORE LOG-RETURNS [Stocks]
# Ticker
# AAN    -0.076244
# ABM     0.064672
# ACA    -0.012872
# ACCO   -0.189240
# ACHR   -0.296349
#           ...   
# XYL     0.095138
# ZIM     0.355414
# ZIP    -0.287201
# ZTO     0.022624
# ZWS    -0.004249
# Length: 309, dtype: float64