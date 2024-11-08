import os, dotenv;
import random;
import pandas as pd;
import numpy as np;
import QuantLib as ql;
import random;
from datetime import datetime, timedelta;
import matplotlib.pyplot as plt;
from scipy.optimize import brentq;
from scipy.optimize import minimize_scalar;
from scipy import stats;
from arch import arch_model;
import requests;

from fredapi import Fred;
from uvatradier import Account, Quotes, EquityOrder, OptionsOrder, OptionsData;
# from nyse_sectors import nyse_sectors; # gives access to sector-sorted dictionary of NYSE symbols
# from dow30 import DOW30;

import warnings;
warnings.filterwarnings('ignore');


#
# Retrieve + Setup API Keys
#

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

# nyse_sector_names = list(nyse_sectors.keys());
# nyse_stocks_all = [k for sector_stocks in nyse_sectors.values() for k in sector_stocks];

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
# Set Date as DataFrame Index + Compute Log Returns
#

def prep_data (bar_data):
	df = bar_data.set_index('date');
	df['log_returns'] = np.log(df['close'] / df['close'].shift(1));
	return df.dropna();


#
# Fit GARCH(1,1) Model to Historical Stock Log-Returns
#

def fit_garch(log_returns, p_order=1, q_order=1):
	model = arch_model(log_returns, mean='Constant', vol='GARCH', p=p_order, q=q_order);
	results = model.fit(disp='off');
	return results;

#
# Define Indicator to Identify "Bona-Fide" Volatility Movement
#	vol indicator = (α + β)
#

def vol_persistence_indicator (arch_model_results):
	alpha = arch_model_results.params['alpha[1]'];
	beta = arch_model_results.params['beta[1]'];
	return (alpha + beta);


#
# Use GARCH Model to Forecast Future Volatility
#

def mc_forecast (arch_model_results, n_days=30, n_sims=10000):
	last_vol = np.sqrt(arch_model_results.conditional_volatility[-1]);
	omega = arch_model_results.params['omega'];
	alpha = arch_model_results.params['alpha[1]'];
	beta = arch_model_results.params['beta[1]'];

	print(f'[last_vol, ω, α, β] = [{last_vol}, {omega}, {alpha}, {beta}]');
	forecasts = np.zeros(shape=(n_sims, n_days));
	for i in range(n_sims):
		# print(f'\ni = {i}');
		vols = np.zeros(shape=n_days);
		vols[0] = last_vol;
		print(f'SIM # {i+1}');
		print(f'Vol initial: {vols[0]:.4f}');
		for t in range(1, n_days):
			shock_effect = np.random.normal(0, 1);
			vol_updated = np.sqrt(omega + alpha * (vols[t-1]*shock_effect)**2 + beta*vols[t-1]**2);
			print(f'Vol updated: {vol_updated:.4f}');
			vols[t] = np.sqrt(omega + alpha * (vols[t-1]*shock_effect)**2 + beta*vols[t-1]**2);
			# vols.append(vol_updated);
		forecasts[i] = vols;
		print(f'Vol final: {vols}');
		print(100*'-');

	annualized_vols = np.sqrt(252) * np.mean(forecasts, axis=0);
	forecast_std = np.sqrt(252) * np.std(forecasts[:,-1]);
	return annualized_vols, forecast_std;


#
# Determine Greeks and Price Options with QuantLib Binomial Tree Model
#

def greeks_and_NPV (underlying, strike_price, time_to_expiry, risk_free_rate, dividend_yield, volatility, option_type):
	day_count = ql.Actual365Fixed();
	calendar = ql.UnitedStates(ql.UnitedStates.NYSE);

	todays_date = ql.Date.todaysDate();
	settlement_date = calendar.advance(todays_date, ql.Period(2, ql.Days));
	ql.Settings.instance().evaluationDate = settlement_date;

	expiry_date = settlement_date + int(time_to_expiry*365);

	underlyingH = ql.QuoteHandle(ql.SimpleQuote(underlying));
	risk_freeH = ql.YieldTermStructureHandle(ql.FlatForward(settlement_date, risk_free_rate, day_count));
	div_yieldH = ql.YieldTermStructureHandle(ql.FlatForward(settlement_date, dividend_yield, day_count));


	# NOTE FOR FUTURE:
	# • We should consider trying to use Dupire Local or Stochastic Vol model
	volH = ql.BlackVolTermStructureHandle(ql.BlackConstantVol(settlement_date, calendar, volatility, day_count));

	bsm_process = ql.BlackScholesMertonProcess(underlyingH, div_yieldH, risk_freeH, volH);

	payoff = ql.PlainVanillaPayoff(ql.Option.Call if option_type == 'call' else ql.Option.Put, strike_price);
	exercise = ql.AmericanExercise(settlement_date, expiry_date);
	option_contract = ql.VanillaOption(payoff, exercise);

	binom_engine = ql.BinomialVanillaEngine(bsm_process, 'crr', 100);
	option_contract.setPricingEngine(binom_engine);

	return {
		'NPV' 	: option_contract.NPV(),
		'Delta' : option_contract.delta(),
		'Gamma' : option_contract.gamma(),
		'Theta' : option_contract.theta()
	};	

def IV (row, stock_price, risk_free_rate, div_yield, time_to_expiry, contract_price, option_type):
	def objective_function (vol):
		try:
			return greeks_and_NPV(stock_price, row['strike'], time_to_expiry, risk_free_rate, div_yield, vol, option_type)['NPV'] - contract_price;
		except RuntimeError:
			return float('inf');

	try:
		return brentq(objective_function, 1e-6, 5);
	except ValueError:
		return np.nan;

def calc_IVs (options, stock_price, risk_free_rate, div_yield, expiry):
	options['mid'] = .5 * (options['bid'] + options['ask']);
	options['TTM'] = expiry;

	def compute_implied_vol (row):
		try:
			return IV(row, stock_price, risk_free_rate, div_yield, expiry, row['mid'], row['option_type']);
		except Exception as e:
			print(f'ERROR [compute_implied_vol]: {str(e)}');
			return np.nan;

	options['IV'] = options.apply(compute_implied_vol, axis=1);
	return options;


def identify_vol_trades (options, garch_vol, garch_vol_std):
	def z_test (implied_vol):
		z_score = (implied_vol - garch_vol) / garch_vol_std;
		p_value = 1 - stats.norm.cdf(z_score);
		return z_score, p_value;

	options['vol_z'], options['vol_p'] = zip(*options['IV'].apply(z_test));

	# options_good = options.loc[options['vol_p'] < .05];
	# return options_good;
	return options;











#
# Date Mania
#

# >>> todays_date
# datetime.datetime(2024, 7, 17, 19, 20, 8, 55645)
# >>> axp_trade_date
# Date(17,7,2024)
# >>> axp_settlement_date
# Date(19,7,2024)
#
#
# >>> axp_expiry_str
# '2024-08-16'
# >>> axp_expiry_dt
# datetime.datetime(2024, 8, 16, 0, 0)
# >>> axp_expiry_date
# Date(16,8,2024)
# >>> axp_time_to_expiry
# 0.07671232876712329
#
#
# >>> fred_rate_date
# '2024-06-01'
#
#
# >>> axp_trade_date
# Date(17,7,2024)
# >>> axp_settlement_date
# Date(19,7,2024)
# >>> ql.Settings.instance().evaluationDate
# Date(19,7,2024)

todays_date_dt = datetime.now();
todays_date_str = todays_date_dt.strftime("%Y-%m-%d");
last_year_str = (datetime.today() - timedelta(weeks=52)).strftime("%Y-%m-%d");

day_count = ql.Actual365Fixed();
calendar = ql.UnitedStates(ql.UnitedStates.NYSE);

axp_trade_date = ql.Date(todays_date_dt.day, todays_date_dt.month, todays_date_dt.year);
axp_settlement_date = calendar.advance(axp_trade_date, ql.Period(2, ql.Days));
ql.Settings.instance().evaluationDate = axp_settlement_date;

axp_expiry_str = options_data.get_closest_expiry('AXP', num_days=30);
axp_expiry_dt = datetime.strptime(axp_expiry_str, '%Y-%m-%d');
axp_expiry_date = ql.Date(axp_expiry_dt.day, axp_expiry_dt.month, axp_expiry_dt.year);
axp_time_to_expiry = ql.Actual365Fixed().yearFraction(axp_settlement_date, axp_expiry_date);

fred_rate_date 	= (datetime.today() - timedelta(weeks=4)).replace(day=1).strftime('%Y-%m-%d');

#
# Equity quote data for American Express (AXP)
#

# >>> axp_quote.T
#                                      0
# symbol                             AXP
# description        American Express Co
# exch                                 N
# type                             stock
# last                            249.96
# change                            0.33
# volume                         2471527
# open                            248.81
# high                            250.84
# low                             247.38
# close                           249.96
# bid                             250.06
# ask                              253.0
# change_percentage                 0.14
# average_volume                 3258480
# last_volume                          0
# trade_date               1721257200002
# prevclose                       249.63
# week_52_high                    249.76
# week_52_low                     140.91
# bidsize                              3
# bidexch                              P
# bid_date                 1721256949000
# asksize                              1
# askexch                              K
# ask_date                 1721252561000
# root_symbols                       AXP
#
# >>> axp_bars
#            date    open      high       low   close   volume
# 0    2021-01-04  121.30  121.8000  116.8500  118.04  3472122
# 1    2021-01-05  118.26  119.2800  117.1000  118.67  2112678
# 2    2021-01-06  121.00  124.7300  120.8100  122.63  5861455
# 3    2021-01-07  124.10  125.6873  117.3501  121.66  8696634
# 4    2021-01-08  122.07  122.1950  119.5200  121.78  2933427
# ..          ...     ...       ...       ...     ...      ...
# 884  2024-07-11  238.87  239.9500  236.9600  238.75  1868831
# 885  2024-07-12  238.97  240.2599  237.2300  238.63  2612283
# 886  2024-07-15  240.45  244.6600  240.4500  244.00  3054379
# 887  2024-07-16  244.14  249.7600  243.8000  249.63  3074640
# 888  2024-07-17  248.81  250.8400  247.3800  249.96  2471527
#
# [889 rows x 6 columns]
#
# >>> axp_price
# 249.96

axp_quote = quotes.get_quote_day('AXP');
axp_price = axp_quote.iloc[0]['last'];
# axp_bars = quotes.get_historical_quotes(
# 	symbol = 'AXP',
# 	start_date = (datetime.today() - timedelta(weeks=52)).strftime("%Y-%m-%d"),
# 	end_date = datetime.today().strftime("%Y-%m-%d")
# );
axp_bars = quotes.get_historical_quotes(symbol='AXP', start_date=todays_date_str, end_date=last_year_str);
axp_bars = quotes.get_historical_quotes('AXP', start_date=last_year_str, end_date=todays_date_str);

#
# Options data for American Express
#

# >>> axp_options
#                 symbol option_type  last     bid     ask  strike  volume  open_interest
# 0   AXP240816P00120000         put  0.02    0.00    0.95   120.0       0              3
# 1   AXP240816C00120000        call   NaN  128.45  132.20   120.0       0              0
# 2   AXP240816P00125000         put   NaN    0.00    1.27   125.0       0              0
# 3   AXP240816C00125000        call   NaN  123.35  127.20   125.0       0              0
# 4   AXP240816P00130000         put  0.05    0.00    1.27   130.0       0              1
# ..                 ...         ...   ...     ...     ...     ...     ...            ...
# 57  AXP240816C00320000        call   NaN    0.00    1.20   320.0       0              0
# 58  AXP240816P00330000         put   NaN   78.45   82.25   330.0       0              0
# 59  AXP240816C00330000        call   NaN    0.00    1.40   330.0       0              0
# 60  AXP240816P00340000         put   NaN   87.95   92.10   340.0       0              0
# 61  AXP240816C00340000        call   NaN    0.01    1.35   340.0       0              0
#
# [62 rows x 8 columns]

axp_options = options_data.get_chain_day('AXP', expiry=axp_expiry_str)[['symbol', 'option_type', 'last', 'bid', 'ask', 'strike', 'volume', 'open_interest']];

#
# Fetch 3-Month TBILL Rate from Fred as Risk-Free Approx
#

# >>> t_bill_rate
# 2024-06-01    5.24
# dtype: float64
# >>> risk_free_rate
# 0.0524

t_bill_rate = fred.get_series(series_id='TB3MS', observation_start=fred_rate_date);
risk_free_rate = .01*t_bill_rate.iloc[0];


#
# Retrieve Dividends -> Compute Dividend Yield
#

axp_dividends = dividend_table('AXP');
axp_dividends['ex_date'] = pd.to_datetime(axp_dividends['ex_date']);
axp_dividends = axp_dividends.sort_values('ex_date', ascending=False);

axp_dividend_freq = axp_dividends['frequency'][0];
axp_dividend_perannum = axp_dividends['cash_amount'][0] * axp_dividend_freq;
axp_dividend_yield = axp_dividend_perannum / axp_price;




#
# Prep Data -> Fit GARCH(1,1) Model to Log-Returns
#

# >>> axp_vol_persistence
# 0.699998938367998
# >>> axp_garch_forecast
# array([1.82914118, 1.53459103, 1.28912375, 1.08466002, 0.91463554,
#        0.77386669, 0.65772505, 0.56229011, 0.48444523, 0.42143664,
#        0.37106248, 0.33122604, 0.30019554, 0.27644875, 0.25848811,
#        0.24514921, 0.23537456, 0.22829515, 0.22317812, 0.21954599,
#        0.21699491, 0.21514791, 0.21389193, 0.21298008, 0.21234741,
#        0.21188497, 0.21158218, 0.21136714, 0.21118095, 0.21108431])
# >>> axp_garch_forecastSD
# 0.0020389508368885883

axp_bars = prep_data(axp_bars);






axp_garch = fit_garch(axp_bars['log_returns']);
axp_vol_persistence = vol_persistence_indicator(axp_garch);
axp_garch_forecast, axp_garch_forecastSD = mc_forecast(axp_garch);


#
# Compute Implied Volatilities
#

# axp_optionsIV = calc_IVs(axp_options, axp_price, risk_free_rate, axp_dividend_yield, axp_time_to_expiry);


#
# Identify Opportunities
#

# axp_trades = identify_vol_trades(axp_optionsIV, garch_model_forecast[-1], garch_model_std);



