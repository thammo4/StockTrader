# FILE: `StockTrader/qlib_bfly.py`
from config import * 				# config contains necessary libraries used in script


underlying = 100.0;
K1, K2, K3 = [90.0, 100.0, 110.0];
risk_free_rate = 0.05;
dividend_yield = 0.02;
volatility = 0.20;


expiry_date = ql.Date(15, 7, 2024);
settlement_date = ql.Date(3, 5, 2024);

day_count = ql.Actual365Fixed();
calendar = ql.NullCalendar();

ql.Settings.instance().evaluationDate = settlement_date;


underlyingH = ql.QuoteHandle(ql.SimpleQuote(underlying));
risk_freeTS = ql.YieldTermStructureHandle(ql.FlatForward(settlement_date, risk_free_rate, day_count));
dividendTS = ql.YieldTermStructureHandle(ql.FlatForward(settlement_date, dividend_yield, day_count));
volTS = ql.BlackVolTermStructureHandle(ql.BlackConstantVol(settlement_date, calendar, volatility, day_count));


bsm_process = ql.BlackScholesMertonProcess(underlyingH, dividendTS, risk_freeTS, volTS);


def create_option (strike, option_type):
	exercise = ql.AmericanExercise(settlement_date, expiry_date);
	payoff = ql.PlainVanillaPayoff(option_type, strike);
	option = ql.VanillaOption(payoff, exercise);
	engine = ql.BinomialVanillaEngine(bsm_process, 'crr', 100);
	option.setPricingEngine(engine);

	return option;


def get_greeks (qlib_option):
	return {
		'delta': np.round(qlib_option.delta(),4),
		'gamma': np.round(qlib_option.gamma(),4),
		'theta': np.round(qlib_option.theta(),4)
	};



# >>> get_greeks(option1)
# {'delta': 0.8985, 'gamma': 0.0193, 'theta': -6.0166}
# >>> get_greeks(option2)
# {'delta': 0.5422, 'gamma': 0.0445, 'theta': -10.3327}
# >>> get_greeks(option3)
# {'delta': 0.1691, 'gamma': 0.0282, 'theta': -6.1123}

option1 = create_option(K1, ql.Option.Call);
option2 = create_option(K2, ql.Option.Call);
option3 = create_option(K3, ql.Option.Call);



# >>> print(f"Option Prices: [K1=90, K2=100, K3=110]: {np.round([price1,price2,price3],3)}")
# Option Prices: [K1=90, K2=100, K3=110]: [10.924  3.839  0.779]
`
price1 = option1.NPV();
price2 = option2.NPV();
price3 = option3.NPV();



#
# Compute the Butterfly Spread Price
#

# >>> np.round(bfly_price, 3)
# 4.026
bfly_price = (price1+price3) - 2*price2;



#
# DataFrame of Strike, Price and Greeks
#

# >>> bfly_spread
#     delta   gamma    theta      price  strike
# 0  0.8985  0.0193  -6.0166  10.924441    90.0
# 0  0.5422  0.0445 -10.3327   3.839191   100.0
# 0  0.1691  0.0282  -6.1123   0.779464   110.0