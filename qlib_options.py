# FILE: `StockTrader/qlib_options.py`
import os, dotenv;
import random;
import pandas as pd;
import numpy as np;
import yfinance as yf;
import QuantLib as ql;
import random;
import requests;
from datetime import datetime, timedelta;
import matplotlib.pyplot as plt;
from scipy.optimize import brentq;

from fredapi import Fred;
from uvatradier import Account, Quotes, EquityOrder, OptionsOrder, OptionsData;
from nyse_sectors import nyse_sectors; # gives access to sector-sorted dictionary of NYSE symbols
from dow30 import DOW30;

import warnings;
warnings.filterwarnings('ignore');


#
# Retrieve API Keys from Working Directory `.env` file
#

dotenv.load_dotenv();

tradier_acct = os.getenv("tradier_acct");
tradier_token = os.getenv("tradier_token");

tradier_acct_live = os.getenv("tradier_acct_live");
tradier_token_live = os.getenv("tradier_token_live");

fred_api_key = os.getenv("fred_api_key");

#
# Instantiate API Class Objects for Tradier, FRED
#

acct = Account(tradier_acct, tradier_token);
quotes = Quotes(tradier_acct, tradier_token);
equity_order = EquityOrder(tradier_acct, tradier_token);
options_order = OptionsOrder(tradier_acct, tradier_token);
options_data = OptionsData(tradier_acct, tradier_token);
fred = Fred(api_key = fred_api_key);

nyse_sector_names = list(nyse_sectors.keys());


#
# Compute Implied Volatility and Greeks for a Given Options Contract
#

def greeks_and_IV (row, spot_price, risk_free_rate, dividend_yield, time_to_expiry, expiry_date):
	option_type = ql.Option.Call if row['option_type'] == 'call' else ql.Option.Put;
	mid_price = .5 * (row['bid'] + row['ask']);

	try:
		day_count = ql.Actual365Fixed();
		calendar = ql.UnitedStates(ql.UnitedStates.NYSE);
		calculation_date = ql.Settings.instance().evaluationDate;

		spot_priceH = ql.QuoteHandle(ql.SimpleQuote(spot_price));
		risk_freeTS = ql.YieldTermStructureHandle(ql.FlatForward(calculation_date, risk_free_rate, day_count));
		dividend_yieldTS = ql.YieldTermStructureHandle(ql.FlatForward(calculation_date, dividend_yield, day_count));

		forward_price = fwd_price(S=spot_price, r=risk_free_rate, q=dividend_yield, T=time_to_expiry);
		discount_factor = np.exp(-risk_free_rate * time_to_expiry);

		if mid_price <= 0 or np.isnan(mid_price):
			raise ValueError("Mid price is garbage");

		implied_sd = ql.blackFormulaImpliedStdDev(option_type, float(row['strike']), forward_price, float(mid_price), discount_factor, 0.0);
		implied_vol = (1/np.sqrt(time_to_expiry)) * implied_sd;

		payoff = ql.PlainVanillaPayoff(option_type, row['strike']);
		exercise = ql.EuropeanExercise(expiry_date);
		option_contract = ql.VanillaOption(payoff, exercise);

		volTS = ql.BlackVolTermStructureHandle(
			ql.BlackConstantVol(calculation_date, calendar, implied_vol, day_count)
		);

		bsm_process = ql.BlackScholesMertonProcess(spot_priceH, dividend_yieldTS, risk_freeTS, volTS);

		option_contract.setPricingEngine(ql.AnalyticEuropeanEngine(bsm_process));

		return pd.Series({
			'IV': implied_vol,
			'Delta': option_contract.delta(),
			'Gamma': option_contract.gamma(),
			'Vega': option_contract.vega(),
			'Theta': option_contract.theta()
		});

	except Exception as e:
		print(f"JUNK: {str(e)} [{str(row['symbol'])}]");
		return pd.Series({'IV':np.nan, 'Delta':np.nan, 'Gamma':np.nan, 'Vega':np.nan, 'Theta':np.nan, });

#
# Compute Price of (European) Option for stock following Black-Scholes-Merton Process
#

# def option_price_npv (option_type, strike_price, expiry, vol):
# 	payoff = ql.PlainVanillaPayoff(option_type, strike_price);
# 	exercise = ql.EuropeanExercise(expiry);
# 	option = ql.VanillaOption(payoff, exercise);

# 	vol_handle = ql.BlackVolTermStructureHandle(
# 		ql.BlackConstantVol(0, calendar, vol, day_count)
# 	);

# 	bsm_process = ql.BlackScholesMertonProcess(price_handle, dividend_handle, risk_free_handle, vol_handle);

# 	option.setPricingEngine(ql.AnalyticEuropeanEngine(bsm_process));

# 	return option.NPV();
def option_price_npv (row, spot_price, risk_free_rate, dividend_yield, time_to_expiry, expiry_date):
	if pd.isna(row['IV']):
		print(f"BAD IV, SKIP: {row['symbol']}");
		return np.nan;
	day_count = ql.Actual365Fixed();
	calendar = ql.UnitedStates(ql.UnitedStates.NYSE);
	calculation_date = ql.Settings.instance().evaluationDate;

	option_type = ql.Option.Call if row['option_type'] == 'call' else ql.Option.Put;

	payoff = ql.PlainVanillaPayoff(option_type, row['strike']);
	exercise = ql.EuropeanExercise(expiry_date);
	option = ql.VanillaOption(payoff, exercise);

	spot_priceH = ql.QuoteHandle(ql.SimpleQuote(spot_price));
	risk_freeTS = ql.YieldTermStructureHandle(ql.FlatForward(calculation_date, risk_free_rate, day_count));
	dividendTS = ql.YieldTermStructureHandle(ql.FlatForward(calculation_date, dividend_yield, day_count));
	volTS = ql.BlackVolTermStructureHandle(ql.BlackConstantVol(calculation_date, calendar, row['IV'], day_count));

	bsm_process = ql.BlackScholesMertonProcess(spot_priceH, dividendTS, risk_freeTS, volTS);

	option.setPricingEngine(ql.AnalyticEuropeanEngine(bsm_process));

	return option.NPV();


#
# Helper Function to Call Tradier API to Retrieve Dividend Data
#

def dividend_table(symbol):
	r = requests.get(
		url = 'https://api.tradier.com/beta/markets/fundamentals/dividends',
		params = {'symbols':symbol},
		headers = {'Authorization':f'Bearer {tradier_token_live}', 'Accept':'application/json'}
	);
	return pd.json_normalize(r.json()[0]['results'][1]['tables']['cash_dividends']);


#
# Helper Function - Compound Annual Growth Rate (CAGR)
#	• Returns the mean annualized growth rate for compounding values over a specified time period
#

def CAGR (value_initial, value_final, num_periods):
	cap_ratio = value_final / value_initial;
	cap_ratio_discounted = cap_ratio ** pow(base=num_periods, exp=-1);
	cagr = cap_ratio_discounted - 1;
	return cagr;



#
# Helper Function - Computes the Forward Price of an Asset as f(S, r, q, T)
#	• S = Spot Price
# 	• r = Risk-Free Rate
# 	• q = Dividend Yield
# 	• T = Time to Expiry (in years)
#

def fwd_price (S, r, q, T):
	F = S * np.exp((r - q)*T);
	return F;



#
# Fetch 3-Month TBILL Rate from Fred as Risk-Free Approx
#

# >>> t_bill_rate
# 2024-06-01    5.24
# dtype: float64

todays_date = datetime.now();
rate_date = (datetime.today() - timedelta(weeks=4)).replace(day=1).strftime('%Y-%m-%d');

t_bill_rate = fred.get_series(series_id='TB3MS', observation_start=rate_date);
risk_free_rate = .01*float(t_bill_rate);

print(f"Current T-Bill [3 Month] Rate: {risk_free_rate:.4f}."); # Current T-Bill [3 Month] Rate: 0.0524.

#
# Retrieve XOM Data from Tradier Market Data API
#

# >>> xom_options
#                 symbol  last  change  volume  open  high  low    bid    ask  ...  prevclose  bidsize  bidexch       bid_date  asksize  askexch       ask_date  open_interest  option_type
# 0   XOM240809P00065000   NaN     NaN       0   NaN   NaN  NaN   0.00   0.75  ...        NaN        0        D  1720792251000       79        Z  1720792515000              0          put
# 1   XOM240809C00065000   NaN     NaN       0   NaN   NaN  NaN  47.10  50.05  ...        NaN       42        Z  1720792530000       42        Z  1720792590000              0         call
# 2   XOM240809P00070000   NaN     NaN       0   NaN   NaN  NaN   0.00   0.75  ...        NaN        0        P  1720791693000       80        Z  1720792526000              0          put
# 3   XOM240809C00070000   NaN     NaN       0   NaN   NaN  NaN  41.95  45.25  ...        NaN       42        Z  1720792511000       42        Z  1720792511000              0         call
# 4   XOM240809P00075000   NaN     NaN       0   NaN   NaN  NaN   0.00   0.75  ...        NaN        0        A  1720791810000       53        Z  1720792589000              0          put
# ..                 ...   ...     ...     ...   ...   ...  ...    ...    ...  ...        ...      ...      ...            ...      ...      ...            ...            ...          ...
# 81  XOM240809C00140000   NaN     NaN       0   NaN   NaN  NaN   0.00   0.75  ...        NaN        0        D  1720792250000       79        Z  1720792447000              0         call
# 82  XOM240809P00145000   NaN     NaN       0   NaN   NaN  NaN  30.20  33.60  ...        NaN       42        Z  1720792594000       43        Z  1720791949000              0          put
# 83  XOM240809C00145000   NaN     NaN       0   NaN   NaN  NaN   0.00   0.75  ...        NaN        0        X  1720791213000       79        Z  1720792520000              0         call
# 84  XOM240809P00150000   NaN     NaN       0   NaN   NaN  NaN  35.10  38.60  ...        NaN       43        Z  1720792520000       41        Z  1720792028000              0          put
# 85  XOM240809C00150000   NaN     NaN       0   NaN   NaN  NaN   0.00   0.75  ...        NaN        0        D  1720792245000       80        Z  1720792509000              0         call
#
# [86 rows x 22 columns]

expiry_str = options_data.get_closest_expiry('XOM', 30);
expiry_dt = datetime.strptime(expiry_str, '%Y-%m-%d');

xom_quote = quotes.get_quote_day("XOM");
xom_price = float(xom_quote['last']); # spot price
xom_options = options_data.get_chain_day('XOM', expiry=expiry_str);
xom_options_subset = xom_options[['symbol', 'last', 'bid', 'ask', 'strike', 'option_type']]



#
# Retrieve dividend data from Tradier Fundamentals API
#

# >>> xom_dividends
#     share_class_id dividend_type    ex_date  cash_amount currency_i_d declaration_date  frequency    pay_date record_date
# 0       0P00000220            CD 2024-05-14       0.9500          USD       2024-04-26          4  2024-06-10  2024-05-15
# 1       0P00000220            CD 2024-02-13       0.9500          USD       2024-02-02          4  2024-03-11  2024-02-14
# 2       0P00000220            CD 2023-11-14       0.9500          USD       2023-10-27          4  2023-12-11  2023-11-15
# 3       0P00000220            CD 2023-08-15       0.9100          USD       2023-07-28          4  2023-09-11  2023-08-16
# 4       0P00000220            CD 2023-05-15       0.9100          USD       2023-04-27          4  2023-06-09  2023-05-16
# ..             ...           ...        ...          ...          ...              ...        ...         ...         ...
# 150     0P00000220            CD 1987-02-04       0.1125          USD       1987-01-28          4  1987-03-10  1987-02-10
# 151     0P00000220            CD 1986-11-05       0.1125          USD       1986-10-29          4  1986-12-10  1986-11-12
# 152     0P00000220            CD 1986-08-07       0.1125          USD       1986-07-30          4  1986-09-10  1986-08-13
# 153     0P00000220            CD 1986-05-07       0.1125          USD       1986-04-30          4  1986-06-10  1986-05-13
# 154     0P00000220            CD 1986-02-04       0.1125          USD       1986-01-29          4  1986-03-10  1986-02-10
#
# [157 rows x 9 columns]

xom_dividends = dividend_table('XOM');
xom_dividends['ex_date'] = pd.to_datetime(xom_dividends['ex_date']);
xom_dividends = xom_dividends.sort_values('ex_date', ascending=False);



#
# Compute Current Dividend Yield
#

xom_dividend_freq = xom_dividends['frequency'][0];
xom_dividend_annum = xom_dividends['cash_amount'][0] * xom_dividend_freq;
xom_dividend_yield = xom_dividend_annum / xom_price;


#
# Compute Dividend Growth Rate ~ Last 5 years of Dividend Data
#

five_years_ago = datetime.now() - pd.DateOffset(years=5);
xom_dividends_recent = xom_dividends[xom_dividends['ex_date'] > five_years_ago];
xom_dividend_CAGR = CAGR(value_initial=xom_dividends_recent.iloc[-1]['cash_amount'], value_final=xom_dividends_recent.iloc[0]['cash_amount'], num_periods = 5);


# >>> Current Div Yield: 0.0336
# >>> Dividend Growth Rate [5yr CAGR]: -0.8275
print(f"Current Div Yield: {xom_dividend_yield:.4f}");
print(f"Dividend Growth Rate [5yr CAGR]: {xom_dividend_CAGR:.4f}");


#
# QuantLib Date Handlers - Used to compute QuantLib Days-to-Expiry
#

ql_calculation_date = ql.Date(todays_date.day, todays_date.month, todays_date.year);
ql.Settings.instance().evaluationDate = ql_calculation_date;


#
# Configure QuantLib Objects
#

day_count = ql.Actual365Fixed();
calendar = ql.UnitedStates(ql.UnitedStates.NYSE);
price_handle = ql.QuoteHandle(ql.SimpleQuote(xom_price));
risk_free_handle = ql.YieldTermStructureHandle(ql.FlatForward(0, calendar, risk_free_rate, day_count));
dividend_handle = ql.YieldTermStructureHandle(ql.FlatForward(0, calendar, xom_dividend_yield, day_count));

xom_expiry_date = ql.Date(expiry_dt.day, expiry_dt.month, expiry_dt.year);
xom_time_to_expiry = day_count.yearFraction(ql_calculation_date, xom_expiry_date);




#
# Test Case for Greeks/IV Computations Using a Single Option Contract [XOM240809C00113000]
#

# OPTION TYPE: 1
# MID PRICE: 2.995
# FORWARD PRICE: 113.4640
# DISCOUNT FACTOR: 0.9960
# >>> xom_greeks_IV
# IV        0.221338
# Delta     0.537452
# Gamma     0.057018
# Vega     12.427701
# Theta   -18.920337
# dtype: float64


xom_options_row = xom_options.query("strike == 113 and option_type == 'call'");
xom_greeks_IV = greeks_and_IV(
	row = xom_options_row.iloc[0],
	spot_price = xom_price,
	risk_free_rate = risk_free_rate,
	dividend_yield = xom_dividend_yield,
	time_to_expiry = xom_time_to_expiry,
	expiry_date = xom_expiry_date
);



#
# Compute Greeks, IV for Option Chain Contracts
#

# >>> xom_options_subset
#                 symbol  last    bid    ask  strike option_type        IV     Delta     Gamma      Vega      Theta
# 0   XOM240809P00065000   NaN   0.00   0.75    65.0         put  1.118280 -0.025246  0.001678  1.849053 -13.403706
# 1   XOM240809C00065000   NaN  46.55  50.10    65.0        call  0.731177  0.995278  0.000295  0.212803  -0.610843
# 2   XOM240809P00070000   NaN   0.00   0.75    70.0         put  0.988168 -0.028456  0.002098  2.043374 -13.080325
# 3   XOM240809C00070000   NaN  41.60  45.10    70.0        call  0.659195  0.994354  0.000452  0.293752  -1.117519
# 4   XOM240809P00075000   NaN   0.00   0.75    75.0         put  0.866084 -0.032271  0.002657  2.267828 -12.713236
# ..                 ...   ...    ...    ...     ...         ...       ...       ...       ...       ...        ...
# 81  XOM240809C00140000   NaN   0.00   0.75   140.0        call  0.471596  0.061510  0.008200  3.810718 -11.825299
# 82  XOM240809P00145000   NaN  30.10  33.65   145.0         put  0.565520 -0.928860  0.007440  4.146030 -11.625075
# 83  XOM240809C00145000   NaN   0.00   0.75   145.0        call  0.531511  0.055781  0.006738  3.529275 -12.326183
# 84  XOM240809P00150000   NaN  36.10  38.70   150.0         put  0.738078 -0.894149  0.007749  5.636336 -23.242298
# 85  XOM240809C00150000   NaN   0.00   0.75   150.0        call  0.587986  0.051365  0.005706  3.306081 -12.760465
#
# [86 rows x 11 columns]

xom_options_subset = xom_options_subset.join(xom_options_subset.apply(
	lambda contract: greeks_and_IV(
		row = contract,
		spot_price = xom_price,
		risk_free_rate = risk_free_rate,
		dividend_yield = xom_dividend_yield,
		time_to_expiry = xom_time_to_expiry,
		expiry_date = xom_expiry_date
	), axis=1
));



#
# Compute Theoretical Option Prices [NPV]
#

# >>> xom_options_subset
#                 symbol  last    bid    ask  strike option_type        IV     Delta     Gamma      Vega      Theta        NPV
# 0   XOM240809P00065000   NaN   0.00   1.28    65.0         put  1.239187 -0.036313  0.002048  2.497268 -20.058965   0.639995
# 1   XOM240809C00065000   NaN  46.45  50.40    65.0        call  0.992872  0.982175  0.001233  1.204999  -7.357844  48.425001
# 2   XOM240809P00070000   NaN   0.00   1.28    70.0         put  1.097699 -0.040844  0.002544  2.748382 -19.542951   0.640005
# 3   XOM240809C00070000   NaN  41.60  45.40    70.0        call  0.912644  0.976613  0.001749  1.570797  -9.149868  43.500002
# 4   XOM240809P00075000   NaN   0.00   1.28    75.0         put  0.964900 -0.046190  0.003196  3.035217 -18.956548   0.639996
# ..                 ...   ...    ...    ...     ...         ...       ...       ...       ...       ...        ...        ...
# 81  XOM240809C00140000   NaN   0.00   1.25   140.0        call  0.529769  0.086580  0.009486  4.946358 -17.231707   0.625000
# 82  XOM240809P00145000   NaN  29.80  33.80   145.0         put  0.528478 -0.943227  0.006629  3.448002  -8.196348  31.800000
# 83  XOM240809C00145000   NaN   0.00   1.27   145.0        call  0.596103  0.079882  0.007936  4.655880 -18.226868   0.635000
# 84  XOM240809P00150000   NaN  34.80  38.80   150.0         put  0.590770 -0.945571  0.005726  3.329250  -8.872022  36.800000
# 85  XOM240809C00150000   NaN   0.00   1.35   150.0        call  0.665568  0.077087  0.006918  4.531913 -19.789032   0.675000
#
# [86 rows x 12 columns]

xom_options_subset['NPV'] = xom_options_subset.apply(
	lambda contract: option_price_npv(
		row = contract,
		spot_price = xom_price,
		risk_free_rate = risk_free_rate,
		dividend_yield = xom_dividend_yield,
		time_to_expiry = xom_time_to_expiry,
		expiry_date = xom_expiry_date
	), axis=1
);