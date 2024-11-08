# FILE: `StockTrader/garch_vol2.py`
from config import * # this imports all necessary packages and functions referenced but not explicitly defined below


#
# Use Binomial Pricing Model to Estimate Price of American Option Contract
#

def binomQL (underlying, strike_price, time_to_expiry, risk_free_rate, dividend_yield, volatility, option_type):
	day_count = ql.Actual365Fixed();
	calendar = ql.UnitedStates(ql.UnitedStates.NYSE);
	todays_date = ql.Date.todaysDate();
	settlement_date = calendar.advance(todays_date, ql.Period(2, ql.Days));
	ql.Settings.instance().evaluationDate = settlement_date;

	expiry_date = settlement_date + int(time_to_expiry * 365);

	underlyingH = ql.QuoteHandle(ql.SimpleQuote(underlying));
	risk_freeH = ql.YieldTermStructureHandle(ql.FlatForward(settlement_date, risk_free_rate, day_count));
	div_yieldH = ql.YieldTermStructureHandle(ql.FlatForward(settlement_date, dividend_yield, day_count));
	volH = ql.BlackVolTermStructureHandle(
		ql.BlackConstantVol(settlement_date, calendar, volatility, day_count)
	);
	bsm_process = ql.BlackScholesMertonProcess(underlyingH, div_yieldH, risk_freeH, volH);
	payoff = ql.PlainVanillaPayoff(ql.Option.Call if option_type == 'call' else ql.Option.Put, strike_price);
	exercise = ql.AmericanExercise(settlement_date, expiry_date);
	option_contract = ql.VanillaOption(payoff, exercise);
	binom_engine = ql.BinomialVanillaEngine(bsm_process, 'crr', 100);
	option_contract.setPricingEngine(binom_engine);

	return option_contract;

def binomIV (midprice, underlying, strike_price, time_to_expiry, risk_free_rate, dividend_yield, option_type):
	def objectiveIV (vol):
		try:
			binom_option = binomQL(underlying, strike_price, time_to_expiry, risk_free_rate, dividend_yield, vol, option_type);
			return binom_option.NPV() - midprice;
		except RuntimeError:
			return 1e5;

	try:
		# result = root_scalar(objectiveIV, method='toms748', bracket=found_bracket, xtol=1e-6);
		result = root_scalar(objectiveIV, method='toms748', bracket=[.005, 83], xtol=1e-6);
		if result.converged:
			return result.root;
		else:
			print('FAILED: TOMS748 CONVERGE')
			return None;
	except ValueError as e:
		print(f'Root-finding failed: {str(e)}');
		return None;


# >>> obj_binom_IV(h_price,strike,h_days_to_expiry/365,h_risk_free,h_div_yield,option_type,.005)
# 0.0
# >>> obj_binom_IV(h_price,strike,h_days_to_expiry/365,h_risk_free,h_div_yield,option_type,83)
# 144.97267785483393
def obj_binom_IV (underlying, strike_price, time_to_expiry, risk_free_rate, dividend_yield, option_type, vol):
	binom = binomQL(underlying, strike_price, time_to_expiry, risk_free_rate, dividend_yield, vol, option_type);
	print(binom.NPV());



# Hyatt Price: 150.26
h_price = quotes.get_quote_day("H", True);
print(f'Hyatt Price: {h_price}');
print(105*'-');



# H Expiry String: 2024-08-16
# H Days to Maturity: 21
h_expiry_str = options_data.get_closest_expiry(symbol='H', num_days=30);
h_expiry_dt = datetime.strptime(h_expiry_str, '%Y-%m-%d');
h_days_to_expiry = (h_expiry_dt - datetime.today()).days;
print(f'H Expiry String: {h_expiry_str}');
print(f'H Days to Maturity: {h_days_to_expiry}');
print(105*'-');


# Risk Free Rate [TB3MS]: 0.0524
# H Dividend Yield: 0.004
h_risk_free = fred_rate();
h_div_yield = dividend_yield('H', h_price);
print(f'Risk Free Rate [TB3MS]: {h_risk_free}');
print(f'H Dividend Yield: {h_div_yield}');
print(105*'-');



# Near Money Options:
#               symbol  volume    bid    ask  strike  bidsize  asksize  open_interest option_type
# 28  H240816P00140000       0   1.15   1.55   140.0       44       30             97         put
# 29  H240816C00140000       0  11.80  12.50   140.0        6        6              6        call
# 30  H240816P00145000       1   2.35   2.75   145.0        7       23             85         put
# 31  H240816C00145000       1   6.80   8.60   145.0       65       20             40        call
# 32  H240816P00150000      10   4.50   4.70   150.0        6       12             81         put
# 33  H240816C00150000       2   5.10   5.60   150.0       41        1             32        call
# 34  H240816P00155000       0   7.10   7.50   155.0        3       10            327         put
# 35  H240816C00155000      13   3.00   3.40   155.0        6       31            276        call
# 36  H240816P00160000       1  10.40  11.30   160.0        9        7            211         put
# 37  H240816C00160000      27   1.45   1.80   160.0        3       23            457        call
h_options = options_data.get_chain_day("H", options_data.get_closest_expiry('H', 30));
h_options.drop(
	labels = ['last', 'change', 'open', 'high', 'low', 'change_percentage', 'last_volume', 'trade_date', 'prevclose', 'bidexch', 'bid_date', 'askexch', 'ask_date'],
	axis = 1,
	inplace= True
);
h_near_itm = h_options.loc[(h_options['strike'] >= (1-.0725)*h_price) & (h_options['strike'] <= (1+.0725)*h_price)];
print(f'Near Money Options:\n{h_near_itm}');
print(105*'-');



# Hyatt Bar Data:
#            date    open     high      low   close   volume  log_return
# 0    2023-07-27  121.37  124.360  121.075  122.30   539682         NaN
# 1    2023-07-28  123.26  125.620  123.260  125.18   591237    0.023276
# 2    2023-07-31  125.79  127.800  125.790  126.35   782804    0.009303
# 3    2023-08-01  125.75  126.350  123.455  125.03   754097   -0.010502
# 4    2023-08-02  123.55  124.720  122.300  122.34  1050326   -0.021750
# ..          ...     ...      ...      ...     ...      ...         ...
# 246  2024-07-19  153.44  154.730  152.000  154.61   444559    0.010860
# 247  2024-07-22  152.68  154.700  151.800  153.97   286304   -0.004148
# 248  2024-07-23  154.80  155.990  154.010  155.07   240798    0.007119
# 249  2024-07-24  153.79  154.645  150.270  150.31   417710   -0.031177
# 250  2024-07-25  149.79  151.560  147.950  150.26   663501   -0.000333

# [251 rows x 7 columns]
h_bars = quotes.get_historical_quotes(
	symbol = 'H',
	start_date = (datetime.today()-timedelta(weeks=52)).strftime("%Y-%m-%d"),
	end_date = datetime.today().strftime("%Y-%m-%d")
);
h_bars['log_return'] = np.log(h_bars['close']).diff();
print(f'Hyatt Bar Data:\n{h_bars}');
print(105*'-');


# H Historical Volatility: 0.2747
h_historical_vol = estimate_historical_vol(h_bars);
print(f'H Historical Volatility: {h_historical_vol:.4f}');
print(105*'-');



h_contract = h_near_itm.loc[(h_near_itm['strike'] == 145.00) & (h_near_itm['option_type'] == 'put')];
h_strike = h_contract.iloc[0]['strike'];
h_option_type = h_contract.iloc[0]['option_type'];
h_contract_midprice = .5*(h_contract.iloc[0]['bid'] + h_contract.iloc[0]['ask']);
print(f'Contract for Testing:\n{h_contract}');
print(105*'-');


# Testing Contract QuantLib Object:
# <QuantLib.QuantLib.VanillaOption; proxy of <Swig Object of type 'ext::shared_ptr< VanillaOption > *' at 0x13b73a340> >
h_contract_ql = binomQL(
	underlying = h_price,
	strike_price = h_contract.iloc[0]['strike'],
	time_to_expiry = h_days_to_expiry/365,
	risk_free_rate = h_risk_free,
	dividend_yield = h_div_yield,
	volatility = h_historical_vol,
	option_type = h_contract.iloc[0]['option_type']
);
print(f'Testing Contract QuantLib Object:\n{h_contract_ql}');
print(105*'-');


# Contract Example Midprice: 2.5500
# Contract Example NPV: 1.6792
# Contract Example Delta: -0.2700
# Contract Example Gamma: 0.0339
h_contract_npv = h_contract_ql.NPV();
h_contract_delta = h_contract_ql.delta();
h_contract_gamma = h_contract_ql.gamma();
h_contract_theta = h_contract_ql.theta();

npv_and_greeks = [h_contract_midprice, h_contract_npv, h_contract_delta, h_contract_gamma, h_contract_theta];
tmp_vars = ['Midprice', 'NPV', 'Delta', 'Gamma', 'Theta'];
for x in range(0,4):
	print(f"Contract Example {tmp_vars[x]}: {npv_and_greeks[x]:.4f}");
print(105*'-');


# Binom IV: 0.3451
sigma_IV = binomIV(
	midprice = .5*(h_contract.iloc[0]['bid'] + h_contract.iloc[0]['ask']),
	underlying = h_price,
	strike_price = h_contract.iloc[0]['strike'],
	time_to_expiry = h_days_to_expiry/365,
	risk_free_rate = h_risk_free,
	dividend_yield = h_div_yield,
	option_type = h_contract.iloc[0]['option_type']
);
print(f"Binom IV: {sigma_IV:.4f}");
print(105*'-');



# Binom Contract Price with σ_iv: 2.5500
# Broker Midprice: 2.55
h_contract_IV = binomQL(
	underlying = h_price,
	strike_price = h_strike,
	time_to_expiry = h_days_to_expiry/365,
	risk_free_rate = h_risk_free,
	dividend_yield = h_div_yield,
	volatility = sigma_IV,
	option_type = h_option_type
);
print(f"Binom Contract Price with σ_iv: {h_contract_IV.NPV():.4f}");
print(f"Broker Midprice: {h_contract_midprice}");
print(125*'-');






