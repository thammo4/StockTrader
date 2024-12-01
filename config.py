import os, dotenv;
import time, schedule;
import logging;
import random;
import pandas as pd;
import numpy as np;
import yfinance as yf;
import QuantLib as ql;
import random;
import cmath;
import matplotlib.pyplot as plt;
import seaborn as sns;
import requests;
import json;
import re;
import warnings; warnings.filterwarnings('ignore');

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
import statsmodels.api as sm;
from arch import arch_model;
from collections import defaultdict;
from hmmlearn import hmm;
from filterpy.kalman import KalmanFilter;
import pywt;

import backtrader as bt;
from fredapi import Fred;
from uvatradier import Account, Quotes, EquityOrder, OptionsOrder, OptionsData, Stream;
from nyse_sectors import nyse_sectors; # gives access to sector-sorted dictionary of NYSE symbols
from nyse_dividend_stocks import nyse_dividend_stocks; # list of ≈ 1000 NYSE-listed stocks that pay dividends (https://dividendhistory.org/nyse/)
from nyse_large_cap import nyse_large_cap;
from ticker_symbols_with_dividends import ticker_symbols_with_dividends;
from long_symbol_list import perplexity_symbols;
from dow30 import DOW30;
from cik_sec import cik_lookup;

# List of symbols of stocks kept on etrade
etrade_tom = list(pd.read_csv("PortfolioDownloadTom.csv")['Symbol']);
etrade_phil = list(pd.read_csv("PortfolioDownloadPhil.csv")['Symbol']);

#
# Moneyness Reminder for Sanity Checks
#

MNYS = pd.DataFrame({'Call':{'ITM':'S>K', 'ATM':'S=K', 'OTM':'S<K'}, 'Put':{'ITM':'S<K', 'ATM':'S=K', 'OTM':'S>K'}})

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

#
# SEC EDGAR
#

from lxml import etree;
import xml.etree.ElementTree as ET;
from io import StringIO;


#
# Federal Reserve Economic Data (FRED)
#

fred = Fred(api_key = fred_api_key);

#
# Convenience Date for Today in YYYY-MM-DD Format
#


today = datetime.today().strftime("%Y-%m-%d");

#
# Define Lists for Each of the largecapX Text Files Needed to Compute IVP (X=1,...,9)
#

largecap1 = pd.read_csv('options/largecap1.txt', header=None)[0].to_list();
largecap2 = pd.read_csv('options/largecap2.txt', header=None)[0].to_list();
largecap3 = pd.read_csv('options/largecap3.txt', header=None)[0].to_list();
largecap4 = pd.read_csv('options/largecap4.txt', header=None)[0].to_list();
largecap5 = pd.read_csv('options/largecap5.txt', header=None)[0].to_list();
largecap6 = pd.read_csv('options/largecap6.txt', header=None)[0].to_list();
largecap7 = pd.read_csv('options/largecap7.txt', header=None)[0].to_list();
largecap8 = pd.read_csv('options/largecap8.txt', header=None)[0].to_list();
largecap9 = pd.read_csv('options/largecap9.txt', header=None)[0].to_list();
largecapALL = largecap1 + largecap2 + largecap3 + largecap4 + largecap5 + largecap6 + largecap7 + largecap8 + largecap9;

nyse_sector_names = list(nyse_sectors.keys());
nyse_stocks_all = [k for sector_stocks in nyse_sectors.values() for k in sector_stocks];

# NYSE symbols with confirmed dividend data in Tradier's Fundamentals API
nyse_dividend_sample = ['LYB', 'MFA', 'MSM', 'MSCI', 'MDT', 'MCK', 'AES', 'MTN', 'VOC', 'URI', 'USAC', 'CEFD', 'USPH', 'TEN', 'TROX', 'TD', 'TTC', 'THO', 'BK', 'TT'];

options_subset_columns = ['symbol', 'last', 'bid', 'ask', 'strike', 'option_type'];

etfs = [
    "SPY", "QQQ", "IWM", "TQQQ", "SQQQ",
    "TLT", "SLV", "GLD", "UVXY", "EEM",
    "XLF", "HYG", "KRE", "SOXL", "ARKK",
    "GDX", "EWZ", "XLE", "FXI", "EFA",
    "VXX", "KWEB", "UNG", "XLI", "DIA"
];

from large_cap_with_dividends import *

#
# StockTrader Universe
#

STOCK_UNIVERSE = [];
try:
	STOCK_UNIVERSE = list(acct.get_positions()['symbol'].values);
except:
	print('No current account positions for STOCK_UNIVERSE [config.py]');
STOCK_UNIVERSE +=nyse_dividend_sample;
STOCK_UNIVERSE += etrade_phil;
STOCK_UNIVERSE += etrade_tom;


#
# Link to symbols page to update list or create different list with elements from different tab
# https://optionalpha.com/symbols
#

# Sept 12, 2024
oa_popular = ["SPY","QQQ","IWM","TLT","GLD","XRT","XLU","XLP","DIA","EEM","XBI","XOP","FXI","AAPL","XLE","MSFT","XSP","NVDA","XLF","TSLA","AMZN","EFA","EWZ","AMD","SMH","XLY","XLK","IYR","USO","NFLX","XRT","XOP","TQQQ","TLT","SMH","GLD","XBI","XLE","MSFT","XLU","COST","QQQ","DIA","IWM","AMZN","EWZ","META","QCOM","XLP","FXI","XSP","NVDA","XLV","SPY","DIS","GOOG","AAPL","NFLX","ARKK","XOM","SPY","QQQ","MSFT","IWM","AAPL","TSLA","VXX","GLD","TLT","UVXY","NVDA","XSP","DIA","FXI","TQQQ","SLV","XLE","AMZN","EEM","SPX","XOP","SQQQ","META","XLP","XLF","NFLX","COST","MA","AMD","GOOGL","WBA","OXY","AXP","INTC","COST","META","MSFT","CF","CSCO","GILD","IBB","QCOM","ROST","SMH","ULTA","VGK","TTD","QQQ","TSM","AMZN","CSX","NEM","PFE","MRNA","SQ","AIG","ATVI","CVS","CVX","FDX","EFA","MCD","UVXY","MGM","AKAM","PG","EWW","COF","SBUX","BIDU","EEM","AAP","CPRI","DD","DHR","FITB","EXPE","HBAN","GPRO","MAR","KSS","KMI","MS","NRG","TAP","WYNN","BP","DIS","XLY","HYG","XBI","XLP","TLT","EWZ","FXI","XLU","XLF","KRE","CPB","FXE","STX","GLD","UPS","SPX","ORCL","XOP","IYR","LQD","KR","CMCSA","SCHW","ADBE","A","DISH","HOG","TBT","TXN","VFC","XLI","GDX","PEAK","KR","MRNA","DAL","WBA","FDX","WY","GPS","NKE","EIX","BK","MU","FTV","ADBE","APA","LEN","INTC","BEN","FAST","GLW","XLI","FANG","WRK","BMY","CL","JPM","COST","XLP","BA","AAL","TRV","WFC","PGR","AXP","LUV","TSLA","XRX","ACN","DRI","UNH","TMUS","USO","GM","QCOM","LKQ","CMCSA","FTI","ALK","HBAN","GPRO","GLD","SYF","AMD","MCHP","UAL","AIG","APTV","DXC","AES","SMH","IRM","USB","SWKS","OMC","EA","PEP","APH","CFG","EWW","AFL","TSM","BKR","KMI","NEM","HON","CZR","MAS","XHB","EWJ","EWY","WM","TAP","CPB","KRE","UA","XOP","SCHD","DVN","COP","LYB","BP","NWL","NRG","NEE","CNP","WAB","FITB","CCL","SLB","EW","KR","ADBE","LEN","FDX","DRI","MU","COST","ACN","CCL","NKE","PEP","FAST","DAL","WFC","UNH","PGR","JPM","C","BK","SCHW","WBA","UAL","PNC","OMC","JNJ","GS","BAC","USB","PG","NFLX","MS","LVS","KMI","CCI","CFG","ABT","TSLA","TRV","UNP","MTB","FITB","FCX","CSX","BX","ALK","AAL","TSM","SLB","RF","IPG","HBAN","AXP","ZION","CLF","XRX","SYF","WM","VZ","V","TXN","RTX","MSFT","NEE","MMM","KO","IVZ","GLW","HCA","GOOGL","GM","GOOG","GE","DOW","DHR","CNC","CB","ADM","WU","WAB","TMUS","TMO","T","NOW","META","IBM","HLT","GD","FTV","EW","CME","BKR","BA","APH","ADP","X","WY","UPS","STX","RCL","PFG"]

#
# StockTrader Logging Config
#

LOG_DIR = os.path.join(os.path.dirname(__file__), 'logs');
LOG_FILE = os.path.join(LOG_DIR, 'trading_log.log');


#
# Helper Function for Moneyness Sanity Check
#

def what_the_money (K, S, option_type):
	if option_type.lower() == 'call':
		if S > K:
			return 'ITM';
		elif S < K:
			return 'OTM';
		else:
			return 'ATM';
	elif option_type.lower() == 'put':
		if S < K:
			return 'ITM';
		elif S > K:
			return 'OTM';
		else:
			return 'ATM';
	else:
		return 'wtf';



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
				if dividend_data['tables']['cash_dividends'] is None:
					dividend_data = dividend_dict['results'][1];
				cash_dividends = dividend_data['tables']['cash_dividends'];
				if cash_dividends:
					# Need to use pd.json_normalize instead of pd.DataFrame in return statement
					# If returned dataframe only has a single row, pd.DataFrame produces an error, but pd.json_normalize does not
					df_cash_dividends = pd.json_normalize(cash_dividends);
					df_cash_dividends = pd.DataFrame(df_cash_dividends, columns=['cash_amount', 'ex_date', 'frequency']);
					df_cash_dividends['symbol'] = symbol;
					df_cash_dividends['ex_date'] = pd.to_datetime(df_cash_dividends['ex_date']);
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
	if div_data.empty:
		div_data = polygon_dividends(symbol);
	div_payout_per_year = div_data.iloc[0]['cash_amount'] * div_data.iloc[0]['frequency'];

	if price_per_share is None:
		price_per_share = quotes.get_quote_day(symbol, True);

	return np.round(div_payout_per_year/price_per_share, 6);


#
# Helper Function to Get Dividends from Polygon.io Because Tradier's Fundamentals API Is Garbage
#

def polygon_dividends (symbol):
	try:
		r = requests.get(url='https://api.polygon.io/v3/reference/dividends',params={'ticker':symbol, 'apiKey':polygon_api_key});
		r.raise_for_status();
		data = r.json();
		if not data['results']:
			print(f"No Polygon Dividend Data {symbol}");
			return pd.DataFrame(columns=['ex_date', 'cash_amount', 'frequency']);

		df_dividends = pd.json_normalize(r.json()['results']);
		df_dividends = df_dividends[['cash_amount', 'ex_dividend_date', 'frequency']];
		df_dividends.columns = ['cash_amount', 'ex_date', 'frequency'];
		df_dividends['symbol'] = symbol;
		return df_dividends;

	except requests.exceptions.HTTPError as e:
		print(f"Polygon HTTP Error {symbol}: {str(e)}");
	except requests.exceptions.RequestException as e:
		print(f"Polygon Request Error {symbol}: {str(e)}");
	except KeyError:
		print(f"Polygon Response Lacks results {symbol}");
	except Exception as e:
		print(f"Polygon who knows error {str(e)}");
	return pd.DataFrame(columns=['cash_amount', 'ex_date', 'frequency']);


#
# (Sept 16) This Should be on its Way Out
# Helper Function to Process Historical Options Chain Data Retrieved from Dolt SQL API
# 	• Assumes that data has been saved in CSV with `+` signs removed and then loaded into Python as pd df
#

def dolt_options(dolt, subset=None):
	if isinstance(dolt, str):
		filename = dolt if dolt[-3:] == 'csv' else (dolt + '.csv');
	df_dolt = pd.read_csv(filename);
	df_dolt.columns = [x.strip() for x in list(df_dolt.columns)];
	df_dolt.drop(columns=[x for x in df_dolt.columns if 'Unnamed: 0' in x], inplace=True);
	df_dolt['date'] = pd.to_datetime(df_dolt['date']);
	df_dolt['expiration'] = pd.to_datetime(df_dolt['expiration']);
	df_dolt['act_symbol'] = [x.strip() for x in df_dolt['act_symbol']];
	df_dolt['call_put'] = [x.strip() for x in df_dolt['call_put']];
	df_dolt['ttm'] = df_dolt['expiration'] - df_dolt['date'];
	df_dolt['ttm'] = [x.days for x in df_dolt['ttm']];
	# print(df_dolt);

	for x in ['strike', 'bid', 'ask', 'vol', 'delta', 'gamma', 'theta', 'vega', 'rho']:
		df_dolt[x] = pd.to_numeric(df_dolt[x]);

	df_dolt['mp'] = .5 * (df_dolt['bid'] + df_dolt['ask']);
	df_dolt = df_dolt[['date', 'act_symbol', 'expiration', 'ttm', 'strike', 'call_put', 'bid', 'ask', 'mp', 'vol', 'delta', 'gamma', 'theta', 'vega', 'rho']];

	df_dolt = df_dolt.reset_index(drop=True);
	if subset is not None:
		df_dolt = df_dolt[subset]

	return df_dolt;





#
# fred_rate - Convenience Function to Retrieve Treasury Security Interest Rate
# • Used as risk-free rate proxy in options pricing models
# • Rates from FRED use index Y-m-01
# • NOTE - `num_weeks` definition
# 	• If #days \in month = {30,31} -> 4 weeks ago is still same month on the 30th and 31st.
# • NOTE - This has issues on the first of the month BECAUSE `fred_rate_date` can get set to the previous day's month prior to the rate getting published
#


# >>> datetime.today()
# datetime.datetime(2024, 9, 2, 1, 23, 32, 837056)
# >>> fred.get_series(series_id='TB3MS')
# 1934-01-01    0.72
# 1934-02-01    0.62
# 1934-03-01    0.24
# 1934-04-01    0.15
# 1934-05-01    0.16
#               ... 
# 2024-03-01    5.24
# 2024-04-01    5.24
# 2024-05-01    5.25
# 2024-06-01    5.24
# 2024-07-01    5.20
# Length: 1087, dtype: float64
#
# >>> fred_rate()
# T-SEC Data Empty (series=TB3MS, start=2024-08-01)
# Empty DataFrame
# Columns: [cash_amount, ex_date, frequency, symbol]
# Index: []

def fred_rate (series='TB3MS'):
	num_weeks = 4 if datetime.today().day <= 28 else 5;
	todays_date = datetime.today();
	if todays_date.day == 1:
		todays_date = todays_date - timedelta(days=4);
	fred_rate_date = (todays_date - timedelta(weeks=num_weeks)).replace(day=1).strftime('%Y-%m-%d');
	t_security_data = fred.get_series(series_id=series, observation_start=fred_rate_date);
	if t_security_data.empty:
		print(f"T-SEC Data Empty (series={series}, start={fred_rate_date})");
		return pd.DataFrame(columns=['cash_amount', 'ex_date', 'frequency', 'symbol']);
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

#
# Map a list w/string ticker-symbol elements to NYSE Sector into which each ticker is categorized.
#

def symbols_to_NYSESector (symbol_list):
	u_sectors = {};
	for u in symbol_list:
		found = False;
		for sector, symbols in nyse_sectors.items():
			if u in symbols:
				u_sectors[u] = sector;
				found = True; break;
		if not found:
			u_sectors[u] = 'Unknown';
	return u_sectors;