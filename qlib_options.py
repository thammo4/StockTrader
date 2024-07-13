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
import scipy.stats as si

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
# Drift-Adjusted Log-Price Ratio
# • Expected 'moneyness' of contract at expiration
#

def d1 (S, K, r, sigma, T):
	return (np.log(S/K) + (r + .5*sigma**2)*T) / (sigma*np.sqrt(T));


#
# Compute Profitability Probabilty for a Contract Under Risk-Neutral Measure
#

def calculate_profit_prob (option_type, S, K_long, K_short, r, sigma, T):
	d1_long = d1(S,K_long, r, sigma, T);
	d1_short = d1(S,K_short, r, sigma, T);

	if option_type == 'call':
		# return si.norm.cdf(d1_short) - si.norm.cdf(d1_long);
		return si.norm.cdf(d1_short);
	else:
		# return si.norm.cdf(-d1_long) - si.norm.cdf(-d1_short);
		return si.norm.cdf(-d1_long);


#
# Use Greeks and IV to Compute Profitability Probability/Metrics for Spread Combinations
#

def analyze_vertical_spread (options, spread_width=5):
	spreads = [];
	for i in range(len(options)):
		for j in range(i+1, len(options)):
			long_option = options.iloc[i];
			short_option = options.iloc[j];
			if short_option['strike'] - long_option['strike'] == spread_width:

				#
				# Determine profitability and max loss potential
				#

				spread_price = long_option['ask'] - short_option['bid'];
				max_profit = spread_width - spread_price;
				max_loss = spread_price;

				#
				# Compute Net Greeks for the Spread
				#

				net_delta = long_option['Delta'] - short_option['Delta'];
				net_gamma = long_option['Gamma'] - short_option['Gamma'];
				net_vega = long_option['Vega'] - short_option['Vega'];
				net_theta = long_option['Theta'] - short_option['Theta'];

				#
				# Compute Net Implied Volatility
				#

				iv_diff = long_option['IV'] - short_option['IV'];


				#
				# Profitability Probability ~ Risk Neutral Measure
				#

				profit_prob = calculate_profit_prob(
					S = xom_price,
					K_long = long_option['strike'],
					K_short = short_option['strike'],
					T = xom_time_to_expiry,
					r = risk_free_rate,
					sigma = .5*(long_option['IV'] + short_option['IV']),
					option_type = long_option['option_type']
				);


				spreads.append({
					'long_strike': long_option['strike'],
					'short_strike': short_option['strike'],
					'spread_price': spread_price,
					'max_profit': max_profit,
					'max_loss': max_loss,
					'profit_potential': max_profit / spread_price,
					'risk_reward_ratio': max_profit / max_loss,
					'return_on_risk': max_loss / max_profit,
					'net_delta': net_delta,
					'net_gamma': net_gamma,
					'net_vega': net_vega,
					'net_theta': net_theta,
					'iv_diff': iv_diff,
					'profit_prob': profit_prob
				});

	return pd.DataFrame(spreads);



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



#
# Analyze Call and Put Vertical Spreads
#

xom_call_spreads = analyze_vertical_spread(xom_options_subset.query("option_type == 'call'"));
xom_put_spreads = analyze_vertical_spread(xom_options_subset.query("option_type == 'put'"));



#
# OUTPUT
#

# >>> xom_call_spreads
#     long_strike  short_strike  spread_price  max_profit  max_loss  profit_potential  risk_reward_ratio  return_on_risk  net_delta  net_gamma  net_vega  net_theta   iv_diff  profit_prob
# 0          65.0          70.0          8.80       -3.80      8.80         -0.431818          -0.431818       -2.315789   0.005562  -0.000516 -0.365798   1.792024  0.080228     0.975638
# 1          70.0          75.0          8.90       -3.90      8.90         -0.438202          -0.438202       -2.282051  -0.000443  -0.000285  0.028345  -1.331691  0.141974     0.971441
# 2          75.0          80.0          8.85       -3.85      8.85         -0.435028          -0.435028       -2.298701   0.005548  -0.000781 -0.346781   0.896426  0.088765     0.967791
# 3          80.0          85.0          8.85       -3.85      8.85         -0.435028          -0.435028       -2.298701   0.004690  -0.000973 -0.280497   0.084698  0.099891     0.960016
# 4          85.0          90.0          8.75       -3.75      8.75         -0.428571          -0.428571       -2.333333   0.011418  -0.001879 -0.643153   1.249965  0.077644     0.948500
# 5          90.0          95.0          8.85       -3.85      8.85         -0.435028          -0.435028       -2.298701   0.003490  -0.002114 -0.186959  -1.331498  0.112618     0.934712
# 6          95.0         100.0          8.55       -3.55      8.55         -0.415205          -0.415205       -2.408451   0.031134  -0.006132 -1.512760   2.221250  0.062187     0.909483
# 7          97.0         102.0          7.15       -2.15      7.15         -0.300699          -0.300699       -3.325581   0.085412  -0.009245 -3.495200   8.787765 -0.008586     0.866723
# 8          98.0         103.0          8.65       -3.65      8.65         -0.421965          -0.421965       -2.369863  -0.023327  -0.002591  1.187608  -5.250968  0.155782     0.904861
# 9          99.0         104.0          7.90       -2.90      7.90         -0.367089          -0.367089       -2.724138   0.053052  -0.010872 -2.203717   2.445174  0.062555     0.862720
# 10        100.0         105.0          7.40       -2.40      7.40         -0.324324          -0.324324       -3.083333   0.051342  -0.012354 -2.047859   1.301364  0.075789     0.848173
# 11        101.0         106.0          7.25       -2.25      7.25         -0.310345          -0.310345       -3.222222   0.070086  -0.014745 -2.607218   2.274071  0.065195     0.826892
# 12        102.0         107.0          6.10       -1.10      6.10         -0.180328          -0.180328       -5.545455   0.060453  -0.014916 -1.802294  -1.840770  0.111018     0.768243
# 13        103.0         108.0          5.75       -0.75      5.75         -0.130435          -0.130435       -7.666667   0.224645  -0.020564 -7.523369  16.437200 -0.100849     0.779218
# 14        104.0         109.0          6.35       -1.35      6.35         -0.212598          -0.212598       -4.703704   0.157640  -0.017823 -4.232939   6.454823  0.015178     0.728366
# 15        105.0         110.0          5.05       -0.05      5.05         -0.009901          -0.009901     -101.000000   0.169189  -0.023333 -4.285655   4.711572  0.031593     0.704841
# 16        106.0         111.0          4.85        0.15      4.85          0.030928           0.030928       32.333333   0.218770  -0.013345 -4.419199   8.939024 -0.026792     0.644508
# 17        107.0         112.0          4.40        0.60      4.40          0.136364           0.136364        7.333333   0.204292  -0.021180 -3.331233   2.325395  0.040084     0.602612
# 18        108.0         113.0          5.00        0.00      5.00          0.000000           0.000000             inf   0.197781  -0.014072 -2.161329   0.553313  0.043172     0.548134
# 19        109.0         114.0          4.70        0.30      4.70          0.063830           0.063830       15.666667   0.242220  -0.017640 -1.963117  -0.619126  0.044566     0.498675
# 20        110.0         115.0          3.03        1.97      3.03          0.650165           0.650165        1.538071   0.284086  -0.010015 -1.367827   0.408712  0.014014     0.437587
# 21        111.0         116.0          4.07        0.93      4.07          0.228501           0.228501        4.376344   0.263496  -0.012632  0.119473  -5.499517  0.063926     0.397525
# 22        112.0         117.0          2.65        2.35      2.65          0.886792           0.886792        1.127660   0.281765   0.002517  1.088044  -2.814281  0.010307     0.330696
# 23        113.0         118.0          3.29        1.71      3.29          0.519757           0.519757        1.923977   0.285425  -0.001529  2.503887  -8.557775  0.059003     0.293687
# 24        114.0         119.0          2.07        2.93      2.07          1.415459           1.415459        0.706485   0.279897   0.013726  3.752833  -6.865979  0.018810     0.223012
# 25        115.0         120.0          1.52        3.48      1.52          2.289474           2.289474        0.436782   0.249365   0.020685  4.378882  -6.590617  0.003677     0.181132
# 26        116.0         121.0          1.38        3.62      1.38          2.623188           2.623188        0.381215   0.191336   0.022283  3.800694  -4.306289 -0.025046     0.166569
# 27        117.0         122.0          1.21        3.79      1.21          3.132231           3.132231        0.319261   0.196397   0.023748  5.018914  -7.394255  0.000883     0.123087
# 28        118.0         123.0          0.78        4.22      0.78          5.410256           5.410256        0.184834   0.135641   0.023945  3.917951  -4.124951 -0.033227     0.103313
# 29        119.0         124.0          0.62        4.38      0.62          7.064516           7.064516        0.141553   0.094868   0.021772  3.098169  -2.343858 -0.050083     0.085050
# 30        120.0         125.0          0.62        4.38      0.62          7.064516           7.064516        0.141553   0.080295   0.018930  2.892232  -2.511327 -0.046620     0.071253
# 31        121.0         126.0          0.94        4.06      0.94          4.319149           4.319149        0.231527   0.058936   0.014008  1.996031  -0.713249 -0.064169     0.090141
# 32        122.0         127.0          0.45        4.55      0.45         10.111111          10.111111        0.098901   0.038227   0.012452  1.576822  -0.405045 -0.064011     0.055420
# 33        125.0         130.0          0.56        4.44      0.56          7.928571           7.928571        0.126126   0.065990   0.013355  3.391873  -5.820315  0.013945     0.025946
# 34        130.0         135.0          0.07        4.93      0.07         70.428571          70.428571        0.014199  -0.114537  -0.006291 -5.256569  21.518010 -0.297521     0.060472
# 35        135.0         140.0          2.15        2.85      2.15          1.325581           1.325581        0.754386   0.048464   0.003449  1.861167  -6.724710  0.004919     0.090728
# 36        140.0         145.0          1.25        3.75      1.25          3.000000           3.000000        0.333333   0.006698   0.001551  0.290478   0.995161 -0.066334     0.069412
# 37        145.0         150.0          1.27        3.73      1.27          2.937008           2.937008        0.340483   0.002795   0.001017  0.123967   1.562165 -0.069465     0.067184

# >>> xom_put_spreads
#     long_strike  short_strike  spread_price  max_profit  max_loss  profit_potential  risk_reward_ratio  return_on_risk  net_delta  net_gamma  net_vega  net_theta   iv_diff  profit_prob
# 0          65.0          70.0          1.28        3.72      1.28          2.906250           2.906250        0.344086   0.004531  -0.000496 -0.251115  -0.516014  0.141488     0.029353
# 1          70.0          75.0          1.28        3.72      1.28          2.906250           2.906250        0.344086   0.005346  -0.000652 -0.286835  -0.586403  0.132800     0.032751
# 2          75.0          80.0          1.28        3.72      1.28          2.906250           2.906250        0.344086   0.002949  -0.000758 -0.154228  -2.056346  0.145437     0.035282
# 3          80.0          85.0          1.15        3.85      1.15          3.347826           3.347826        0.298701   0.011848  -0.001378 -0.593639   0.709411  0.098644     0.039563
# 4          85.0          90.0          1.29        3.71      1.29          2.875969           2.875969        0.347709   0.032548  -0.002460 -1.455476   5.458456  0.037803     0.055381
# 5          90.0          95.0          2.03        2.97      2.03          1.463054           1.463054        0.683502  -0.006249  -0.002373  0.262071  -7.156453  0.185641     0.065124
# 6          95.0         100.0          1.27        3.73      1.27          2.937008           2.937008        0.340483  -0.003182  -0.004196  0.136405  -5.312054  0.154974     0.054224
# 7          97.0         102.0          1.33        3.67      1.33          2.759398           2.759398        0.362398  -0.055348  -0.000391  2.613387 -11.634037  0.230841     0.040552
# 8          98.0         103.0          1.35        3.65      1.35          2.703704           2.703704        0.369863  -0.054300  -0.001888  2.467734 -11.432135  0.224183     0.044671
# 9          99.0         104.0          0.08        4.92      0.08         61.500000          61.500000        0.016260   0.037667  -0.011079 -1.896086   1.705248  0.058257     0.021041
# 10        100.0         105.0          0.45        4.55      0.45         10.111111          10.111111        0.098901   0.020890  -0.010978 -0.860265  -2.351211  0.113840     0.047831
# 11        101.0         106.0          1.15        3.85      1.15          3.347826           3.347826        0.298701  -0.009031  -0.012387  0.335043  -7.496453  0.170540     0.066576
# 12        102.0         107.0         -0.17        5.17     -0.17        -30.411765         -30.411765       -0.032882   0.115270  -0.023143 -4.779479   6.258151  0.008577     0.033841
# 13        103.0         108.0         -0.27        5.27     -0.27        -19.518519         -19.518519       -0.051233   0.137092  -0.026696 -5.246271   6.496367  0.008439     0.040609
# 14        104.0         109.0         -0.61        5.61     -0.61         -9.196721          -9.196721       -0.108734   0.167054  -0.026149 -5.374844   6.908457  0.007135     0.065322
# 15        105.0         110.0         -0.72        5.72     -0.72         -7.944444          -7.944444       -0.125874   0.179837  -0.027013 -4.933251   5.384483  0.022190     0.087618
# 16        106.0         111.0         -0.89        5.89     -0.89         -6.617978          -6.617978       -0.151104   0.227443  -0.026908 -5.455569   6.778411  0.000940     0.105116
# 17        107.0         112.0         -1.01        6.01     -1.01         -5.950495          -5.950495       -0.168053   0.248563  -0.021195 -4.653430   6.150926 -0.003258     0.147100
# 18        108.0         113.0         -0.64        5.64     -0.64         -8.812500          -8.812500       -0.113475   0.272439  -0.032080 -4.056103   2.079287  0.033065     0.154084
# 19        109.0         114.0         -1.43        6.43     -1.43         -4.496503          -4.496503       -0.222395   0.282699  -0.017264 -2.697481   1.843944  0.015102     0.220716
# 20        110.0         115.0         -1.00        6.00     -1.00         -6.000000          -6.000000       -0.166667   0.323812  -0.022181 -1.370543  -2.282816  0.042732     0.249595
# 21        111.0         116.0         -1.54        6.54     -1.54         -4.246753          -4.246753       -0.235474   0.302457  -0.002279 -0.155051  -0.974311  0.005357     0.323835
# 22        112.0         117.0         -1.85        6.85     -1.85         -3.702703          -3.702703       -0.270073   0.306354  -0.000209  1.446572  -4.696318  0.026669     0.382622
# 23        113.0         118.0         -1.49        6.49     -1.49         -4.355705          -4.355705       -0.229584   0.402744   0.019198  5.524360  -9.097494  0.042692     0.430535
# 24        114.0         119.0         -2.70        7.70     -2.70         -2.851852          -2.851852       -0.350649   0.270239   0.018996  3.591649  -5.246892 -0.003038     0.506161
# 25        115.0         120.0         -2.95        7.95     -2.95         -2.694915          -2.694915       -0.371069   0.233600   0.035663  4.529119  -3.997185 -0.031748     0.578931
# 26        116.0         121.0         -1.85        6.85     -1.85         -3.702703          -3.702703       -0.270073   0.172756   0.024725  3.391775  -2.609353 -0.045360     0.613140
# 27        117.0         122.0         -2.65        7.65     -2.65         -2.886792          -2.886792       -0.346405   0.172215   0.027912  4.586796  -5.381822 -0.023175     0.683614
# 28        118.0         123.0         -2.50        7.50     -2.50         -3.000000          -3.000000       -0.333333   0.015692   0.028051  0.554704   3.995020 -0.115334     0.754666
# 29        119.0         124.0         -2.90        7.90     -2.90         -2.724138          -2.724138       -0.367089   0.095484   0.021179  3.046385  -2.535117 -0.050866     0.754032
# 30        120.0         125.0         -2.35        7.35     -2.35         -3.127660          -3.127660       -0.319728   0.083138   0.020260  3.170994  -3.352855 -0.039586     0.808176
# 31        121.0         126.0         -1.25        6.25     -1.25         -5.000000          -5.000000       -0.200000        NaN        NaN       NaN        NaN       NaN          NaN
# 32        122.0         127.0         -2.05        7.05     -2.05         -3.439024          -3.439024       -0.290780   0.015778   0.010810  0.613732   2.182254 -0.093455     0.825326
# 33        125.0         130.0         -1.20        6.20     -1.20         -5.166667          -5.166667       -0.193548   0.010650   0.006168  0.501072   0.821016 -0.075570     0.887509
# 34        130.0         135.0         -1.35        6.35     -1.35         -4.703704          -4.703704       -0.212598  -0.004033   0.002332 -0.193009   2.503619 -0.091006     0.904376
# 35        135.0         140.0         -1.15        6.15     -1.15         -5.347826          -5.347826       -0.186992   0.007974   0.002196  0.385706   0.168791 -0.061875     0.914917
# 36        140.0         145.0         -1.00        6.00     -1.00         -6.000000          -6.000000       -0.166667   0.003254   0.001286  0.162259   0.702944 -0.065054     0.925693
# 37        145.0         150.0         -1.00        6.00     -1.00         -6.000000          -6.000000       -0.166667   0.002344   0.000903  0.118751   0.675675 -0.062293     0.931870