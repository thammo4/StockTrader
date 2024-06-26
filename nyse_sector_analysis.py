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
# Compute metrics for Adjusted Closing Prices
#	• Daily log-returns
# 	• Cumulative log-returns
# 	• Mean Daily log-returns
# 	• SD(log-returns)
#

# Mean Daily Log Return:
# Ticker
# AESI    0.001820
# AM      0.001672
# AMPY    0.000719
# AR      0.003275
# AROC    0.002291
#           ...   
# WMB     0.001776
# WTI    -0.003425
# XOM     0.001069
# XPRO    0.003300
# YPF     0.001928
# Length: 178, dtype: float64
# Standard Deviation Daily Log Returns:
# Ticker
# AESI    0.022800
# AM      0.011311
# AMPY    0.033458
# AR      0.023067
# AROC    0.018986
#           ...   
# WMB     0.010677
# WTI     0.029210
# XOM     0.011494
# XPRO    0.023181
# YPF     0.029385
# Length: 178, dtype: float64

energy_log_returns = np.log(nyse_energy_data / nyse_energy_data.shift(1));
energy_cum_returns = np.exp(energy_log_returns.cumsum());
energy_mean_log_returns = energy_log_returns.mean();
energy_stddev_log_returns = energy_log_returns.std();


print(f"Mean Daily Log Return:\n{energy_mean_log_returns}");
print(f"Standard Deviation Daily Log Returns:\n{energy_stddev_log_returns}");


#
# Plot Cumulative Returns
#

energy_cum_returns.plot(title="Cumulative Log Returns - NYSE Energy Stocks");
plt.show();






