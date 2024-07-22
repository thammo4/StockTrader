import os, dotenv;
import random;
import pandas as pd;
import numpy as np;
import yfinance as yf;
import QuantLib as ql;
import random;
import cmath;
from datetime import datetime, timedelta;
import matplotlib.pyplot as plt;
from scipy.optimize import brentq;
from scipy.optimize import minimize_scalar;
from scipy import stats;
from scipy import integrate;
from arch import arch_model;
import requests;

from fredapi import Fred;
from uvatradier import Account, Quotes, EquityOrder, OptionsOrder, OptionsData;
from nyse_sectors import nyse_sectors; # gives access to sector-sorted dictionary of NYSE symbols
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

fred = Fred(api_key = fred_api_key);

today = datetime.today().strftime("%Y-%m-%d");

nyse_sector_names = list(nyse_sectors.keys());
nyse_stocks_all = [k for sector_stocks in nyse_sectors.values() for k in sector_stocks];

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
