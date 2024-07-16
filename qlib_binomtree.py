import QuantLib as ql;
import datetime as dt;




#
# Option Contract Parameters
#

underlying_price = 100.0;
strike_price = 100.0;
dividend_yield = 1/50;
risk_free_rate = 1/20;
volatility = 1/5;


#
# Define Option's Lifetime
#

expiry_date = ql.Date(10, 7, 2024);
settlement_date = ql.Date(1,2,2024);
ql.Settings.instance().evaluationDate = settlement_date;


#
# Define Option Type, Payoff and Exercise Type
#

option_type = ql.Option.Call;
payoff = ql.PlainVanillaPayoff(option_type, strike_price);
exercise = ql.AmericanExercise(settlement_date, expiry_date);
amr_option = ql.VanillaOption(payoff, exercise);

#
# Setup Market Environment
#

underlyingH = ql.QuoteHandle(ql.SimpleQuote(underlying_price));
risk_freeTS = ql.YieldTermStructureHandle(
	ql.FlatForward(0, ql.TARGET(), ql.QuoteHandle(ql.SimpleQuote(risk_free_rate)), ql.Actual365Fixed())
);
dividend_yieldTS = ql.YieldTermStructureHandle(
	ql.FlatForward(0, ql.TARGET(), ql.QuoteHandle(ql.SimpleQuote(dividend_yield)), ql.Actual365Fixed())
);
volTS = ql.BlackVolTermStructureHandle(
	ql.BlackConstantVol(0, ql.TARGET(), ql.QuoteHandle(ql.SimpleQuote(volatility)), ql.Actual365Fixed())
);

#
# Define Black-Scholes-Merton Process to Describe Price Dynamics of Option
#

bsm_process = ql.BlackScholesMertonProcess(
	underlyingH,
	dividend_yieldTS,
	risk_freeTS,
	volTS
);


#
# Define Pricing Engine - Binomial Tree Model
#

steps = 1500;
binom_engine = ql.BinomialVanillaEngine(bsm_process, 'crr', steps);
amr_option.setPricingEngine(binom_engine);



#
# Compute Option Price As Payoff Discounted Back to Present
#

price = amr_option.NPV();
print(f"AMR Option Price: {price:.4f}.");





# spot_price = 100.0;
# strike_price = 100.0;
# risk_free_rate = .05;
# volatility = .2;
# dividend_yield = .01;
# expiry = 1.0;

# calc_date = ql.Date(15,5,2023);
# ql.Settings.instance().evaluationDate = calc_date;
# expiry_date = calc_date + ql.Period(int(expiry*365), ql.Days);

# option_type = ql.Option.Call;

# exercise = ql.AmericanExercise(calc_date, expiry_date);

# payoff = ql.PlainVanillaPayoff(option_type, strike_price);

# amr_option = ql.VanillaOption(payoff, exercise);

# steps = 100;
# # binom_engine = ql.BinomialVanillaEngine(ql.ExtendedCoxRossRubinstein(), steps);
# # binom_engine = ql.BinomialVanillaEngine(ql.CoxRossRubinstein(), steps);
# # binom_engine = ql.BinomialCRRVanillaEngine


# # spot_priceH = ql.QuoteHandle(ql.SimpleQuote(spot_price));
# # risk_freeH = ql.YieldTermStructureHandle(ql.FlatForward(calc_date, risk_free_rate, ql.Actual365Fixed()));
# # div_yieldH = ql.YieldTermStructureHandle(ql.FlatForward(calc_date, dividend_yield, ql.Actual365Fixed()));
# # volH = ql.BlackVolTermStructureHandle(
# # 	ql.BlackConstantVol(calc_date, ql.NullCalendar(), volatility, ql.Actual365Fixed())
# # );

# spotH = ql.QuoteHandle(ql.SimpleQuote(spot_price));
# risk_freeTS = ql.YieldTermStructure(ql.FlatForward(calc_date, ))


# # bsm_process = ql.BlackScholesMertonProcess(spot_priceH, div_yieldH, risk_freeH, volH);


# amr_option.setPricingEngine(binom_engine);

# price = amr_option.NPV();

# print(f"Option Price: {price:.4f}");