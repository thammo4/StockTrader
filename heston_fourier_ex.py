from config import * # load necessary packages


# Contract parameters
option_type = ql.Option.Call
underlying = 100.0;
strike_price = 100.0;
expiry = 30/365; # month
risk_free_rate = 0.05;
dividend_yield = 0.02;


# Heston model parameters
v0 = 0.1;
kappa = 1.0;
theta = 0.1;
sigma = 0.1;
rho = -.50;

#
# QuantLib Date Setup
#

today_dt = datetime.now();
today_str = today_dt.strftime("%Y-%m-%d");

today_date = ql.Date(today_dt.day, today_dt.month, today_dt.year);
settlement_date = today_date + 2; # add two days for T+2 settlement
ql.Settings.instance().evaluationDate = settlement_date;

#
# QuantLib Expiry, Exercise
#

expiry_date = settlement_date + ql.Period(30, ql.Days);
exercise = ql.EuropeanExercise(expiry_date);


#
# Define Call and Put Option
#

option_call = ql.Option.Call;
option_put = ql.Option.Put;


#
# Configure Payoffs
#

payoff_call = ql.PlainVanillaPayoff(option_call, strike_price);
payoff_put = ql.PlainVanillaPayoff(option_put, strike_price);

#
# Create QuantLib Objects for: underlying, risk free rate, dividend yield
#

underlyingH = ql.QuoteHandle(ql.SimpleQuote(underlying));
risk_freeTS = ql.YieldTermStructureHandle(ql.FlatForward(settlement_date, risk_free_rate, ql.Actual365Fixed()));
dividend_yieldTS = ql.YieldTermStructureHandle(ql.FlatForward(settlement_date, dividend_yield, ql.Actual365Fixed()));

#
# Instantiate Heston Process and Heston Option Pricing Model
#

heston_process = ql.HestonProcess(risk_freeTS, dividend_yieldTS, underlyingH, v0, kappa, theta, sigma, rho);
heston_engine = ql.AnalyticHestonEngine(ql.HestonModel(heston_process));

#
# Create Options
#

option_call = ql.VanillaOption(payoff_call, exercise);
option_put = ql.VanillaOption(payoff_put, exercise);


#
# Attach Heston Pricing Engine to Options -> Compute Price as Net Present Value
#

option_call.setPricingEngine(heston_engine);
option_put.setPricingEngine(heston_engine);

price_call = option_call.NPV();
price_put = option_put.NPV();

#
# OUTPUT
#

# >>> price_call, price_put
# (3.727627716497861, 3.481760618787769)



















