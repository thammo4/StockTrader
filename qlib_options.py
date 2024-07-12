from config import *
import requests;


#
# Compute Price of (European) Option for stock following Black-Scholes-Merton Process
#

def option_price_npv (option_type, strike_price, expiry, vol):
	payoff = ql.PlainVanillaPayoff(option_type, strike_price);
	exercise = ql.EuropeanExercise(expiry);
	option = ql.VanillaOption(payoff, exercise);

	vol_handle = ql.BlackVolTermStructureHandle(
		ql.BlackConstantVol(0, calendar, vol, day_count)
	);

	bsm_process = ql.BlackScholesMertonProcess(price_handle, dividend_handle, risk_free_handle, vol_handle);

	option.setPricingEngine(ql.AnalyticEuropeanEngine(bsm_process));

	return option.NPV();

def greeks_and_IV (row):
	option_type = ql.Option.Call if row['option_type'] == 'call' else ql.Option.Put;
	mid_price = .5*(row['bid'] + row['ask']);

	try:
		implied_volatility = np.sqrt(time_to_expiry) * ql.blackFormulaImpliedStdDev(
			option_type,
			row['strike'],
			xom_price,
			mid_price,
			1.0,
			risk_free_rate,
			xom_dividend_yield,
			time_to_expiry
		);

		option_price = price_option(option_type, row['strike'], expiry_date, implied_volatility);
		payoff = ql.PlainVanillaPayoff(option_type, row['strike']);
		exercise = ql.EuropeanExercise(expiry_date);
		option = ql.VanillaOption(payoff, exercise);

		vol_handle = ql.BlackVolTermStructureHandle(
			ql.BlackConstantVol(0, calendar, implied_volatility, day_count)
		);
		bsm_process = ql.BlackScholesMertonProcess(xom_price, dividend_handle, risk_free_handle, vol_handle);
		option.setPricingEngine(ql.AnalyticEuropeanEngine(bsm_process));

		return pd.Series({
			'IV': implied_volatility,
			'Delta': option.delta(),
			'Gamma': option.gamma(),
			'Vega': option.vega(),
			'Theta': option.theta()
		});
	except:
		return pd.Series({'IV':np.nan, 'Delta':np.nan, 'Gamma':np.nan, 'Vega':np.nan, 'Theta':np.nan});

def dividend_table(symbol):
	r = requests.get(
		url = 'https://api.tradier.com/beta/markets/fundamentals/dividends',
		params = {'symbols':symbol},
		headers = {'Authorization':f'Bearer {tradier_token_live}', 'Accept':'application/json'}
	);
	return pd.json_normalize(r.json()[0]['results'][1]['tables']['cash_dividends']);


#
# Fetch 3-Month TBILL Rate from Fred as Risk-Free Approx
#

todays_date = datetime.now();
rate_date = (datetime.today() - timedelta(weeks=4)).replace(day=1).strftime('%Y-%m-%d');

t_bill_rate = fred.get_series(series_id='TB3MS', observation_start=rate_date);
risk_free_rate = .01*float(t_bill_rate);

print(f"Current T-Bill [3 Month] Rate: {risk_free_rate:.4f}.");

#
# Retrieve XOM Data from Tradier Market Data API
#

expiry_str = options_data.get_closest_expiry('XOM', 30);
expiry_dt = datetime.strptime(expiry_str, '%Y-%m-%d');

xom_quote = quotes.get_quote_day("XOM");
xom_price = float(xom_quote['last']); # spot price
xom_options = options_data.get_chain_day('XOM', expiry=expiry_str);

#
# Retrieve dividend data from Tradier Fundamentals API
#

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
xom_dividend_CAGW = -1 + (1/5) ** (xom_dividends_recent['cash_amount'].iloc[0] / xom_dividends_recent['cash_amount'].iloc[-1]);


# Current Div Yield: 0.0336
# Dividend Growth Rate [5yr CAGR]: -0.8275
print(f"Current Div Yield: {xom_dividend_yield:.4f}");
print(f"Dividend Growth Rate [5yr CAGR]: {xom_dividend_CAGW:.4f}");


#
# QuantLib Date Handlers
#

ql_calculation_date = ql.Date(todays_date.day, todays_date.month, todays_date.year);
ql.Settings.instance().evaluationDate = ql_calculation_date;


#
# Configure QuantLib Objects
#

day_count = ql.Actual365Fixed();
calendar = ql.UnitedStates();
price_handle = ql.QuoteHandle(ql.SimpleQuote(xom_price));
risk_free_handle = ql.YieldTermStructureHandle(ql.FlatForward(0, calendar, risk_free_rate, day_count));
dividend_handle = ql.YieldTermStructureHandle(ql.FlatForward(0, calendar, xom_dividend_yield, day_count));


expiry_date = ql.Date(expiry_dt.day, expiry_dt.month, expiry_dt.year);
time_to_expiry = day_count.yearFraction(ql_calculation_date, expiry_date);










