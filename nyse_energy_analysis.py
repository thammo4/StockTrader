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


#
# Extract energy sector stocks
#

nyse_energy = nyse_sectors['Energy'];


#
# Download Energy data from Yahoo Finance
#

# >>> nyse_energy_data
# Ticker           AESI         AM  AMPY         AR       AROC      BORR  ...        WKC        WMB       WTI         XOM       XPRO        YPF
# Date                                                                    ...                                                                  
# 2024-01-02  16.235895  12.191319  5.99  22.620001  14.903190  7.152606  ...  22.669893  34.656094  3.212875  100.600334  15.850000  16.520000
# 2024-01-03  16.735310  12.123698  6.07  22.770000  14.962095  7.133064  ...  22.166557  35.426880  3.282289  101.445564  15.920000  16.770000
# 2024-01-04  16.186934  11.911170  5.93  22.389999  14.824649  7.015808  ...  22.028387  34.948795  3.103796  100.561028  15.580000  16.230000
# 2024-01-05  16.382780  12.027095  6.13  23.250000  15.021002  7.006037  ...  22.245514  34.685360  3.153378  100.865700  16.799999  16.700001
# 2024-01-08  16.147762  11.901510  6.09  23.010000  14.736289  6.605413  ...  22.245514  34.782928  3.054215   99.185089  16.250000  16.709999
# ...               ...        ...   ...        ...        ...       ...  ...        ...        ...       ...         ...        ...        ...
# 2024-06-18  19.090000  14.520000  5.82  32.799999  19.090000  6.280000  ...  25.320000  41.810001  2.010000  109.379997  21.790001  21.010000
# 2024-06-20  19.400000  14.540000  6.14  32.470001  18.990000  6.360000  ...  25.709999  42.400002  2.120000  111.739998  22.469999  20.760000
# 2024-06-21  19.440001  14.450000  6.20  32.430000  18.959999  6.220000  ...  25.549999  42.060001  2.120000  110.760002  23.180000  20.299999
# 2024-06-24  19.900000  14.660000  6.46  33.490002  19.559999  6.200000  ...  26.190001  43.110001  2.190000  114.050003  23.690001  20.770000
# 2024-06-25  20.200001  14.900000  6.53  33.509998  19.620001  6.130000  ...  25.940001  42.889999  2.130000  114.370003  23.549999  20.820000

# [121 rows x 178 columns]

nyse_energy_data = yf.download(nyse_energy, start='2024-01-01', end=today)['Adj Close'];


#
# Compute Individual/Sector mean log-returns
#

log_returns_stocks = np.log(nyse_energy_data / nyse_energy_data.shift(1));
log_returns_sector = log_returns_stocks.mean(axis=1);

#
# Stocks/Sector cumulative returns
#

cumulative_returns_stocks = np.exp(log_returns_stocks.cumsum());
cumulative_returns_sector = np.exp(log_returns_sector.cumsum());


#
# Mean, StdDev daily log-returns for stocks
#

log_returns_stocks_mean = log_returns_stocks.mean();
log_returns_stocks_std = log_returns_stocks.std();


#
# Mean, StdDev daily log-returns for sector
#

log_returns_sector_mean = log_returns_sector.mean();
log_returns_sector_std = log_returns_sector.std();


#
# Put Stocks/Metrics data into dataframe for comparison
#

# >>> metrics_df
#         stocks_mean  stocks_std  sector_mean  sector_std
# Ticker                                                  
# AESI       0.001820    0.022800     0.000487    0.010747
# AM         0.001672    0.011311     0.000487    0.010747
# AMPY       0.000719    0.033458     0.000487    0.010747
# AR         0.003275    0.023067     0.000487    0.010747
# AROC       0.002291    0.018986     0.000487    0.010747
# ...             ...         ...          ...         ...
# WMB        0.001776    0.010677     0.000487    0.010747
# WTI       -0.003425    0.029210     0.000487    0.010747
# XOM        0.001069    0.011494     0.000487    0.010747
# XPRO       0.003300    0.023181     0.000487    0.010747
# YPF        0.001928    0.029385     0.000487    0.010747
#
# [178 rows x 4 columns]

metrics_df = pd.DataFrame({
	'stocks_mean': log_returns_stocks_mean,
	'stocks_std': log_returns_stocks_std,
	'sector_mean': log_returns_sector_mean,
	'sector_std': log_returns_sector_std
});


#
# Identify overperforming/underperforming energy stocks ~ mean log-returns
#

overperforming = metrics_df[metrics_df['stocks_mean'] > metrics_df['sector_mean']];
underperforming = metrics_df[metrics_df['stocks_mean'] < metrics_df['sector_mean']];


#
# Group stocks by relative volatility ~ sector volatility
#

less_volatile = metrics_df[metrics_df['stocks_std'] < metrics_df['sector_std']];
more_volatile = metrics_df[metrics_df['stocks_std'] > metrics_df['sector_std']];


#
# Identify potential buy opportunities
#

high_returns = metrics_df['stocks_mean'] > metrics_df['sector_mean'];
low_volatility = metrics_df['stocks_std'] < metrics_df['sector_std'];

stocks_to_buy = list(metrics_df[high_returns & low_volatility].index);

#
# Identify possible sell opportunities (if owned)
#

low_returns = metrics_df['stocks_mean'] < metrics_df['sector_mean'];
high_volatility = metrics_df['stocks_std'] > metrics_df['sector_std'];

stocks_to_sell = list(metrics_df[low_returns & high_volatility].index);


#
# Print buy/sell opportunities to standard output
#

print(f"Sector Wide Mean Log-Returns: {np.round(log_returns_sector_mean, 6)}.");
print(f"Sector Wide Std Dev Log-Returns: {np.round(log_returns_sector_std, 6)}.");
print("\n");
print(f'Stock buying opportunities:\n{stocks_to_buy}\n');
print(f'Stock selling opportunities:\n{stocks_to_sell}\n');

# Sector Wide Mean Log-Returns: 0.000487.
# Sector Wide Std Dev Log-Returns: 0.010747.
# Stock buying opportunities:
# ['DTM', 'EPD', 'ET', 'HESM', 'KMI', 'MPLX', 'OKE', 'PBA', 'SHEL', 'WMB']

# Stock selling opportunities:
# ['BORR', 'BP', 'BPT', 'BSM', 'BTE', 'BTU', 'CAPL', 'CEIX', 'CLCO', 'COP', 'CQP', 'CRC', 'CRGY', 'CRT', 'CSAN', 'CVI', 'DEC', 'DINO', 'DK', 'DKL', 'DRQ', 'DVN', 'E', 'EOG', 'EQNR', 'EQT', 'FET', 'FLNG', 'GRNT', 'HAL', 'HES', 'HP', 'ICD', 'KOS', 'LNG', 'LPG', 'MTDR', 'MTR', 'MUR', 'MVO', 'NBR', 'NC', 'NE', 'NGL', 'NINE', 'NOA', 'NOG', 'NOV', 'NRP', 'NRT', 'NXE', 'OIS', 'OXY', 'PARR', 'PBF', 'PBR', 'PBT', 'PHX', 'PRT', 'PUMP', 'PVL', 'RES', 'RIG', 'RNGR', 'SBR', 'SDRL', 'SGU', 'SJT', 'SLB', 'STR', 'SUN', 'SWN', 'TALO', 'TRP', 'TS', 'TTE', 'TTI', 'UGP', 'USAC', 'VAL', 'VET', 'VOC', 'VTLE', 'WDS', 'WTI']


#
# Buy stocks (uncomment as needed)
#

# for s in stocks_to_buy:
# 	equity_order.order(symbol=s, quantity=10, side='buy', order_type='market', duration='day');





