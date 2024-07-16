#
# Simple Example Applying Numerical Solver (Brent's Method) to Compute Implied Volatility of
# Call Option Whose Price is Given by Black-Scholes-Merton under Risk-Neutral Measure
#	• Section 14.9-14.9 of 'Options, Futures and Derivatives' by John C. Hull
#

import numpy as np;
from scipy.stats import norm;
from scipy.optimize import brentq;


#
# Problem Parameters - (Call Price, Underlying Price, Strike Price, Risk-Free Rate, Time-to-Expiry)
#

S0 = 21;
K = 20;
r = 1/10;
T = .250

stated_price = 1.875;


#
# Define Black-Scholes-Merton Formula for Call Option Price
#

def call_price (sigma):
	d1 = (np.log(S0/K) + (r + .5*sigma**2)*T) / (sigma * np.sqrt(T));
	d2 = d1 - sigma * np.sqrt(T);

	call_option_price = S0 * norm.cdf(d1) - K * np.exp(-r*T) * norm.cdf(d2);

	return call_option_price;


#
# Define function that will be made true by correct value of σ
#

def objective_function (sigma):
	return call_price(sigma) - stated_price;



#
# Brent's Method Requires Upper/Lower Search Bounds
#

lower_sigma = -150;
upper_sigma = 150;



#
# Apply Brent's Method to Determine the Value of σ : S0*N(d1) - Ke^{-rT}N(d2) - c = 0
# • For this example, implied volatility is about 23.45%
#

# >>> np.round(implied_vol, 4)
# 0.2345
implied_vol = brentq(objective_function, lower_sigma, upper_sigma);