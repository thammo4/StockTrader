import os, dotenv;
import random;
import pandas as pd;
import numpy as np;
import yfinance as yf;
import QuantLib as ql;
import random;
import cmath;
import matplotlib.pyplot as plt;
import requests;
import json;
from datetime import datetime, timedelta;
from scipy.interpolate import interp1d;
from scipy.optimize import brentq;
from scipy.optimize import minimize_scalar;
from scipy.optimize import minimize;
from scipy.optimize import root_scalar;
from scipy.optimize import bisect;
from scipy.stats import multivariate_normal;
from scipy.stats import norm;
from scipy import stats;
from scipy import integrate;
from arch import arch_model;
from collections import defaultdict;
from hmmlearn import hmm;
from filterpy.kalman import KalmanFilter;

from fredapi import Fred;
from uvatradier import Account, Quotes, EquityOrder, OptionsOrder, OptionsData, Stream;
from nyse_sectors import nyse_sectors; # gives access to sector-sorted dictionary of NYSE symbols
from nyse_dividend_stocks import nyse_dividend_stocks; # list of ≈ 1000 NYSE-listed stocks that pay dividends (https://dividendhistory.org/nyse/)
from dow30 import DOW30;



import warnings;
warnings.filterwarnings('ignore');

dotenv.load_dotenv();

tradier_acct = os.getenv("tradier_acct");
tradier_token = os.getenv("tradier_token");

tradier_acct_live = os.getenv("tradier_acct_live");
tradier_token_live = os.getenv("tradier_token_live");

fred_api_key = os.getenv("fred_api_key");
polygon_api_key = os.getenv("polygon_api_key");

acct = Account(tradier_acct, tradier_token);
quotes = Quotes(tradier_acct, tradier_token);
equity_order = EquityOrder(tradier_acct, tradier_token);
options_order = OptionsOrder(tradier_acct, tradier_token);
options_data = OptionsData(tradier_acct, tradier_token);
stream = Stream(tradier_acct_live, tradier_token_live, True);

fred = Fred(api_key = fred_api_key);

today = datetime.today().strftime("%Y-%m-%d");

nyse_sector_names = list(nyse_sectors.keys());
nyse_stocks_all = [k for sector_stocks in nyse_sectors.values() for k in sector_stocks];

# NYSE symbols with confirmed dividend data in Tradier's Fundamentals API
nyse_dividend_sample = ['LYB', 'MFA', 'MSM', 'MSCI', 'MDT', 'MCK', 'AES', 'MTN', 'VOC', 'URI', 'USAC', 'CEFD', 'USPH', 'TEN', 'TROX', 'TD', 'TTC', 'THO', 'BK', 'TT'];

options_subset_columns = ['symbol', 'last', 'bid', 'ask', 'strike', 'option_type'];



#
# Helper Function to Call Tradier API to Retrieve Dividend Data (Hoping to Deprecate Soon)
# ['share_class_id', 'dividend_type', 'ex_date', 'cash_amount', 'currency_i_d', 'declaration_date', 'frequency', 'pay_date', 'record_date']
#

def dividend_table (symbol):
	r = requests.get(
		url = 'https://api.tradier.com/beta/markets/fundamentals/dividends',
		params = {'symbols':symbol},
		headers = {'Authorization': f'Bearer {tradier_token_live}', 'Accept':'application/json'}
	);
	if r.status_code != 200:
		print(f"Failed API Request: {r.status_code}");
		return pd.DataFrame();
	try:
		dividend_dict = r.json()[0];
		if 'results' in dividend_dict and dividend_dict['results']:
			dividend_data = dividend_dict['results'][0];
			if 'tables' in dividend_data and 'cash_dividends' in dividend_data['tables']:
				cash_dividends = dividend_data['tables']['cash_dividends'];
				if cash_dividends:
					# Need to use pd.json_normalize instead of pd.DataFrame in return statement
					# If returned dataframe only has a single row, pd.DataFrame produces an error, but pd.json_normalize does not
					df_cash_dividends = pd.json_normalize(cash_dividends);
					df_cash_dividends = pd.DataFrame(df_cash_dividends, columns=['cash_amount', 'ex_date', 'frequency']);
					df_cash_dividends['symbol'] = symbol;
					# return pd.DataFrame(cash_dividends);
					return df_cash_dividends;
	except (IndexError, KeyError) as e:
		print(f"API Parse Error: {e}");
	return pd.DataFrame(columns=['cash_amount', 'ex_date', 'frequency', 'symbol']);


#
# Current Dividend Yield of a Stock
# DY = (Annual Dividend Payout) / (Share Price)
#

def dividend_yield (symbol, price_per_share=None):
	div_data = dividend_table(symbol);
	div_payout_per_year = div_data.iloc[0]['cash_amount'] * div_data.iloc[0]['frequency'];

	if price_per_share is None:
		price_per_share = quotes.get_quote_day(symbol, True);

	return np.round(div_payout_per_year/price_per_share, 4);





#
# Convenience Function to Retrieve Treasury Security Interest Rate
# • Used as risk-free rate proxy in options pricing models
# • Rates from FRED use index Y-m-01
# • NOTE - `num_weeks` definition
# 	• If #days \in month = {30,31} -> 4 weeks ago is still same month on the 30th and 31st.
#

def fred_rate (series='TB3MS'):
	num_weeks = 4 if datetime.today().day <= 28 else 5;
	fred_rate_date = (datetime.today() - timedelta(weeks=num_weeks)).replace(day=1).strftime('%Y-%m-%d');
	t_security_data = fred.get_series(series_id=series, observation_start=fred_rate_date);
	t_security_rate = .01 * t_security_data.iloc[0];
	return t_security_rate;




#
# Compute Historical Volatility ~ Past Closing Prices
#

def estimate_historical_vol (df_bars):
	if 'log_return' not in list(df_bars.columns):
		df_bars['log_return'] = np.log(df_bars['close']).diff();
	mean_log_return = df_bars['log_return'].mean();
	var_log_return = df_bars['log_return'].var(ddof=1);
	sd_log_return = np.sqrt(var_log_return);
	annualized_vol = sd_log_return * np.sqrt(252);
	return annualized_vol;