#
# Volatility Modeling and Forecasting
# 	• Retrieve historical stock data for DuPont from Yahoo Finance
# 	• Compute daily adjusted return, realized historical volatility
# 	• Estimate Volatility of returns using Exponentially Weighted Moving Average Model (EWMA)
# 	• Fit GARCH(1,1) model to data and forecast future volatility
# 	• Define plotting functions for current volatility estimate and future volatility forecast
#

from config import *


df = yf.download("DD", start='2021-07-01', end='2024-07-01');

df['Returns'] = df['Adj Close'].pct_change();
df.dropna(inplace=True);

df['HVol'] = df['Returns'].rolling(window=21).std()*np.sqrt(252);


def ewma_volatility (returns, lambda_param = .940):
	return pd.Series(returns).ewm(alpha=1-lambda_param).std() * np.sqrt(252);


df['EWMAVol'] = ewma_volatility(df['Returns']);

# Maybe:
# df.dropna(inplace=True);


def fit_garch(returns):
	model = arch_model(returns, vol='Garch', p=1, q=1);
	results = model.fit(disp='off');
	return results;

garch_model = fit_garch(df['Returns']);
df['GARCHVol'] = garch_model.conditional_volatility * np.sqrt(252);


def plot_vol_model ():
	plt.figure(figsize=(12,6));
	plt.plot(df.index, df['HVol'], label='Historical');
	plt.plot(df.index, df['EWMAVol'], label='EWMA');
	plt.plot(df.index, df['GARCHVol'], label='GARCH');
	plt.title('Volatility Estimation');
	plt.legend();
	plt.show();



time_horizon = 30;
forecast = garch_model.forecast(horizon=time_horizon);
forecast_vol = forecast.variance.iloc[-1] * np.sqrt(252);



print(f"Forecasted Horizon: {time_horizon}");
print(forecast_vol);



def plot_vol_forecast ():
	plt.figure(figsize=(12,6));
	plt.plot(range(1, time_horizon+1), forecast_vol, label='Volatility Forecast');
	plt.title('GARCH Volatility Forecast');
	plt.xlabel('Forecast Horizon [Days]');
	plt.ylabel('Annualized Volatility');
	plt.legend();
	plt.show();



# >>> df
#                  Open       High        Low      Close  Adj Close   Volume   Returns      HVol   EWMAVol  GARCHVol
# Date                                                                                                              
# 2021-07-02  78.610001  78.970001  77.839996  78.790001  74.372643  2032000  0.002417       NaN       NaN  0.265759
# 2021-07-06  78.239998  78.489998  76.410004  77.080002  72.758522  3148700 -0.021703       NaN  0.270750  0.267888
# 2021-07-07  76.650002  77.760002  76.330002  77.750000  73.390938  1884700  0.008692       NaN  0.256550  0.271518
# 2021-07-08  76.510002  77.400002  75.830002  76.620003  72.324310  3422900 -0.014534       NaN  0.225419  0.272997
# 2021-07-09  77.860001  78.849998  77.360001  78.480003  74.080032  2273900  0.024276       NaN  0.298174  0.274811
# ...               ...        ...        ...        ...        ...      ...       ...       ...       ...       ...
# 2024-06-24  80.110001  82.300003  80.000000  81.080002  81.080002  2906100  0.015658  0.164381  0.190451  0.280980
# 2024-06-25  80.989998  81.500000  80.209999  80.459999  80.459999  1842100 -0.007647  0.167125  0.187938  0.281617
# 2024-06-26  80.000000  80.180000  79.150002  80.029999  80.029999  1822200 -0.005344  0.137568  0.183798  0.281523
# 2024-06-27  80.400002  80.400002  79.589996  79.980003  79.980003  2420700 -0.000625  0.130041  0.178257  0.281327
# 2024-06-28  80.110001  80.639999  80.019997  80.489998  80.489998  3635600  0.006377  0.128108  0.174288  0.281047

# [752 rows x 10 columns]




# >>> arch_model(df['Returns'], vol='GARCH', p=1, q=1)
# Constant Mean(constant: yes, no. of exog: 0, volatility: GARCH(p: 1, q: 1), distribution: Normal distribution), id: 0x145c98110

# >>> garch_model
#                      Constant Mean - GARCH Model Results                      
# ==============================================================================
# Dep. Variable:                Returns   R-squared:                       0.000
# Mean Model:             Constant Mean   Adj. R-squared:                  0.000
# Vol Model:                      GARCH   Log-Likelihood:                1950.65
# Distribution:                  Normal   AIC:                          -3893.30
# Method:            Maximum Likelihood   BIC:                          -3874.81
#                                         No. Observations:                  752
# Date:                Tue, Jul 16 2024   Df Residuals:                      751
# Time:                        22:08:28   Df Model:                            1
#                                   Mean Model                                 
# =============================================================================
#                  coef    std err          t      P>|t|       95.0% Conf. Int.
# -----------------------------------------------------------------------------
# mu         3.0409e-04  7.226e-04      0.421      0.674 [-1.112e-03,1.720e-03]
#                                Volatility Model                              
# =============================================================================
#                  coef    std err          t      P>|t|       95.0% Conf. Int.
# -----------------------------------------------------------------------------
# omega      4.6785e-05  3.345e-06     13.985  1.911e-44  [4.023e-05,5.334e-05]
# alpha[1]   8.2185e-03  3.675e-02      0.224      0.823 [-6.381e-02,8.025e-02]
# beta[1]        0.8490  3.858e-02     22.008 2.422e-107      [  0.773,  0.925]
# =============================================================================

# Covariance estimator: robust
# ARCHModelResult, id: 0x13668e4b0