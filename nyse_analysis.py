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

# >>> list(nyse_sectors.keys())
# [
# 	'Healthcare', 'Basic Materials', 'Financial Services', 'Industrials',
# 	'Consumer Cyclical', 'Real Estate', 'Consumer Defensive', 'Technology',
# 	'Utilities', 'Unknown', 'Energy', 'Communication Services'
# ]

def nyse_sector_Zreturns (sector, window_size=None, start_date=None, end_date=None):
	#
	# Perform some date / window_size checks
	#

	if isinstance(start_date, str):
		start_date = datetime.strptime(start_date, "%Y-%m-%d");

	if isinstance(end_date, str):
		end_date = datetime.strptime(end_date, "%Y-%m-%d");

	if start_date is None and end_date is None and window_size is None:
		raise ValueError("cmon");

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


	#
	# Fetch Adjusted Closing Prices for Sector Stocks
	#

	nyse_sector_symbols = nyse_sectors[sector];
	nyse_sector_data = yf.download(nyse_sector_symbols, start=start_date_string, end=end_date_string)['Adj Close'];

	if nyse_sector_data.empty:
		raise ValueError(f"No data found for {sector}: {start_date_string} -> {end_date_string}");

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

# >>> nyse_sector_Zreturns('Technology', window_size=50)
# [*********************100%%**********************]  200 of 200 completed
# DAILY LOG-RETURNS [Stocks]
# Ticker           ACN      AEVA        AI      ALIT      ANET       APH       ARW  ...      YALA      YEXT       YMM       YOU      ZEPP      ZETA       ZUO
# Date                                                                              ...                                                                      
# 2024-05-07       NaN       NaN       NaN       NaN       NaN       NaN       NaN  ...       NaN       NaN       NaN       NaN       NaN       NaN       NaN
# 2024-05-08  0.004272  0.002894 -0.013418 -0.177382  0.062531  0.015040  0.003693  ... -0.031091 -0.041188 -0.013937 -0.051060 -0.053377  0.012278  0.004746
# 2024-05-09 -0.017395  0.008633 -0.004924 -0.011636  0.014973  0.007867 -0.015015  ...  0.027001  0.016320  0.002336  0.040496  0.049981  0.014664 -0.004746
# 2024-05-10 -0.000914 -0.040941 -0.017428 -0.002604  0.058893 -0.000313  0.006666  ... -0.033336 -0.016320  0.001166 -0.053384 -0.022937  0.000000 -0.028960
# 2024-05-13  0.003519  0.020680  0.043414 -0.030446 -0.013336 -0.003219  0.017329  ...  0.023038  0.000000  0.026454  0.045528  0.002318 -0.001267 -0.003925
# 2024-05-14 -0.001497  0.102725  0.050792  0.009365  0.012157  0.010950  0.002872  ...  0.012346  0.048181 -0.003411  0.002813  0.024015  0.032422  0.009785
# 2024-05-15  0.005102 -0.051432  0.012869  0.018470  0.038680  0.026103  0.007491  ...  0.002043 -0.012270  0.001138 -0.007896 -0.028655  0.043224  0.011617
# 2024-05-16 -0.001687 -0.057158 -0.014776  0.042233 -0.018387 -0.001896 -0.001617  ...  0.002039  0.019215  0.028044 -0.013109  0.000000  0.004689  0.004801
# 2024-05-17 -0.014422  0.000000  0.007226  0.008734 -0.000625  0.001517  0.013471  ...  0.000000 -0.005204  0.041176 -0.008065  0.034289  0.018540 -0.001917
# 2024-05-20  0.006926  0.020379  0.004537  0.014797 -0.001564  0.010932  0.010964  ... -0.004082 -0.040822 -0.001062  0.001734  0.049325  0.011416 -0.015474
# 2024-05-21 -0.006761 -0.050232 -0.013673 -0.008605 -0.007543  0.015181 -0.007321  ... -0.041760 -0.016439 -0.018231 -0.013954 -0.113115  0.034582 -0.002928
# 2024-05-22  0.011363  0.044452 -0.049785  0.019561 -0.001547 -0.006149  0.012197  ...  0.035606 -0.016713 -0.001083  0.001170 -0.078447 -0.005498 -0.036838
# 2024-05-23 -0.003850 -0.050531 -0.038514  0.010837 -0.048819  0.013507 -0.024161  ...  0.012270 -0.036229 -0.033043 -0.019487  0.010309 -0.056121  0.033902
# 2024-05-24 -0.016279  0.038869  0.004168 -0.019348  0.016876 -0.001174  0.008246  ... -0.014330  0.001940  0.002237 -0.013205 -0.012903  0.005814 -0.022807
# 2024-05-28 -0.010890 -0.032790  0.002493 -0.036051  0.003062 -0.016728 -0.009934  ... -0.010363 -0.013659  0.004459  0.023882 -0.002601 -0.014599 -0.040947
# 2024-05-29 -0.013390 -0.043350 -0.007497 -0.034775  0.002663 -0.006815 -0.012907  ... -0.004175 -0.036004 -0.006696  0.011730 -0.037140  0.007618  0.015552
# 2024-05-30 -0.031010  0.031155  0.177642 -0.015852 -0.015197  0.000000  0.015438  ...  0.002090  0.018164  0.008919 -0.030189 -0.016349 -0.039898 -0.001029
# 2024-05-31 -0.008852 -0.009245  0.034403  0.031457 -0.019990 -0.005274  0.005805  ... -0.018968  0.009950 -0.019048  0.014912  0.044332 -0.007929  0.044317
# 2024-06-03 -0.001879  0.024466 -0.003727  0.005148 -0.004242 -0.004467  0.008946  ...  0.000000 -0.011952 -0.004535  0.022828  0.048728 -0.022918 -0.041233
# 2024-06-04  0.022113 -0.068777  0.007103 -0.050010 -0.010889 -0.013215 -0.020667  ...  0.000000 -0.016162  0.000000 -0.009302 -0.060625 -0.019615 -0.011358
# 2024-06-05  0.010223 -0.009756  0.029556  0.004040  0.014829  0.024533  0.022024  ...  0.012685 -0.014359  0.012422  0.011614 -0.013387  0.055311  0.023603
# 2024-06-06  0.002471  0.032157 -0.020831  0.017322 -0.003333 -0.018324 -0.005669  ... -0.021232 -0.025106  0.010050  0.024524  0.017369 -0.009721  0.015098
# 2024-06-07 -0.011515 -0.012739  0.002336 -0.009290  0.000742 -0.001683 -0.001745  ... -0.004301  0.087188 -0.028171  0.030515 -0.082806  0.021142 -0.017129
# 2024-06-10  0.007014 -0.003210  0.044336 -0.002670  0.002658  0.024200  0.008619  ... -0.004320 -0.021591  0.002283  0.003291 -0.005772 -0.021753  0.007089
# 2024-06-11  0.012965 -0.039349 -0.001596 -0.006707  0.023874  0.004399 -0.002563  ...  0.006473 -0.001986  0.005685 -0.016566  0.001446  0.007910  0.014028
# 2024-06-12 -0.029280  0.000000  0.009852 -0.001347  0.021296  0.021707  0.007370  ... -0.010811  0.031314 -0.007968 -0.002230  0.000000  0.051960 -0.003988
# 2024-06-13 -0.012006 -0.069232 -0.036391 -0.014936  0.063287  0.004358 -0.008956  ...  0.012959 -0.007737  0.044700 -0.000558  0.014348 -0.056211 -0.023245
# 2024-06-14  0.015430 -0.010811 -0.048725 -0.016552 -0.009604 -0.010491 -0.026350  ... -0.015135 -0.023577 -0.015419 -0.001676 -0.015794 -0.024023 -0.017535
# 2024-06-17 -0.004124  0.000000  0.005837  0.009689  0.034644  0.012952  0.009425  ... -0.022027  0.007921 -0.006682  0.024311  0.001446  0.001869 -0.011512
# 2024-06-18 -0.000631  0.003617 -0.017266  0.015038  0.000764  0.009800 -0.033382  ... -0.011198 -0.015905  0.003346  0.011937 -0.002894 -0.029683 -0.015915
# 2024-06-20  0.070391 -0.078840 -0.044879  0.022804 -0.000441 -0.018673  0.005233  ... -0.006780  0.000000 -0.013453  0.013394 -0.007273 -0.009662 -0.010753
# 2024-06-21  0.009169  0.015504 -0.008782  0.009241 -0.008354  0.004228  0.002527  ...  0.009029  0.002002 -0.006795 -0.002131 -0.037179  0.010943  0.009683
# 2024-06-24 -0.005745 -0.007722  0.020732 -0.009241 -0.024455 -0.021472 -0.009352  ...  0.015608 -0.020203 -0.013730 -0.003740 -0.029210  0.021533 -0.009683
# 2024-06-25 -0.000130 -0.019570 -0.011222 -0.033719  0.016031  0.010205 -0.020432  ...  0.017544 -0.016461 -0.013921  0.013295 -0.064434  0.027805 -0.007596

# [34 rows x 200 columns]

# AGGREGATE MEAN LOG-RETURNS [Stocks]
# Ticker
# ACN    -0.000342
# AEVA   -0.009399
# AI      0.003147
# ALIT   -0.007347
# ANET    0.006050
#           ...   
# YMM    -0.000387
# YOU     0.001558
# ZEPP   -0.011727
# ZETA    0.001964
# ZUO    -0.004100
# Length: 200, dtype: float64

# AGGREGATE STDDEV LOG-RETURNS [Stocks]
# Ticker
# ACN     0.017403
# AEVA    0.039022
# AI      0.040385
# ALIT    0.036907
# ANET    0.024720
#           ...   
# YMM     0.017140
# YOU     0.022128
# ZEPP    0.039337
# ZETA    0.027277
# ZUO     0.019784
# Length: 200, dtype: float64

# MEAN LOG-RETURN [Sector]
# -0.0013964258289065562

# STDDEV LOG-RETURN [Sector]
# 0.016312729100261925

# Z-SCORE LOG-RETURNS [Stocks]
# Ticker
# ACN     0.064616
# AEVA   -0.490550
# AI      0.278546
# ALIT   -0.364751
# ANET    0.456452
#           ...   
# YMM     0.061884
# YOU     0.181125
# ZEPP   -0.633286
# ZETA    0.206021
# ZUO    -0.165734
# Length: 200, dtype: float64

# Ticker
# ACN     0.064616
# AEVA   -0.490550
# AI      0.278546
# ALIT   -0.364751
# ANET    0.456452
#           ...   
# YMM     0.061884
# YOU     0.181125
# ZEPP   -0.633286
# ZETA    0.206021
# ZUO    -0.165734
# Length: 200, dtype: float64