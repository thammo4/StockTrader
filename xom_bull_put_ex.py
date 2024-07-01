from config import *
from VerticalSpread import VerticalSpread



#
# Retrieve current Exxon Mobil Price
#

# >>> xom_price
# 115.29
xom_price = quotes.get_quote_day('XOM', last_price=True);



#
# Get target expiry for Exxon options contracts ≈ 30 days into future
#

# >>> expiry_target_date
# '2024-08-02'
expiry_target_date = options_data.get_closest_expiry(symbol='XOM', num_days=30);


#
# Define strike range for Bull Put Spread
# 	• Short put with strike: K_short < S_t
# 	• Long put with strike: K_long < K_short < S_t
#

# >>> {'lower_strike':np.round(lower_strike,4), 'upper_strike':upper_strike}
# {'lower_strike': 103.761, 'upper_strike': 109.5255}
lower_strike = .90 * xom_price;
upper_strike = .95* xom_price;


#
# Retrieve options chain for put options between strike range for Exxon ≈ 30 days into future
#

# >>> xom_options_chain
#                 symbol  last  change  volume  open  high   low   bid   ask  strike  change_percentage  last_volume     trade_date  prevclose  bidsize bidexch       bid_date  asksize askexch       ask_date  open_interest option_type
# 28  XOM240802P00104000  0.15    0.00       0   NaN   NaN   NaN  0.22  0.26   104.0               0.00            1  1719588200222       0.15      333       M  1719847652000      623       E  1719847644000             25         put
# 30  XOM240802P00105000  0.35    0.00       0   NaN   NaN   NaN  0.27  0.31   105.0               0.00          120  1719604489431       0.35      403       M  1719847652000      431       M  1719847696000            204         put
# 32  XOM240802P00106000  0.46    0.00       0   NaN   NaN   NaN  0.34  0.38   106.0               0.00            1  1719428155149       0.46      234       E  1719847694000      667       E  1719847695000             67         put
# 34  XOM240802P00107000  0.45   -0.02      30  0.45  0.45  0.45  0.26  0.46   107.0              -4.26           30  1719843927280       0.47      361       X  1719847696000      469       H  1719847696000             43         put
# 36  XOM240802P00108000  0.52   -0.31       6  0.55  0.55  0.52  0.41  0.58   108.0             -37.35            1  1719843480774       0.83      865       X  1719847693000      891       M  1719847693000              3         put
# 38  XOM240802P00109000  0.66   -0.02       7  0.56  0.66  0.56  0.65  0.71   109.0              -2.95            3  1719843514552       0.68        1       C  1719847694000      609       M  1719847694000            183         put
xom_options_chain = options_data.get_chain_day(
	symbol = 'XOM',
	expiry = expiry_target_date,
	strike_low = lower_strike,
	strike_high = upper_strike,
	option_type='put'
);


#
# Define legs of trade with rows from options chain
#

# >>> long_put
# symbol               XOM240802P00105000
# last                               0.35
# change                              0.0
# volume                                0
# open                                NaN
# high                                NaN
# low                                 NaN
# bid                                0.27
# ask                                0.31
# strike                            105.0
# change_percentage                   0.0
# last_volume                         120
# trade_date                1719604489431
# prevclose                          0.35
# bidsize                             403
# bidexch                               M
# bid_date                  1719847652000
# asksize                             431
# askexch                               M
# ask_date                  1719847696000
# open_interest                       204
# option_type                         put
# Name: 30, dtype: object
#
# >>> short_put
# symbol               XOM240802P00109000
# last                               0.66
# change                            -0.02
# volume                                7
# open                               0.56
# high                               0.66
# low                                0.56
# bid                                0.65
# ask                                0.71
# strike                            109.0
# change_percentage                 -2.95
# last_volume                           3
# trade_date                1719843514552
# prevclose                          0.68
# bidsize                               1
# bidexch                               C
# bid_date                  1719847694000
# asksize                             609
# askexch                               M
# ask_date                  1719847694000
# open_interest                       183
# option_type                         put
# Name: 38, dtype: object
long_put = xom_options_chain.iloc[1]; 	# long put has lower strike price
short_put = xom_options_chain.iloc[-1];	# short put has strike between long put and underlying


#
# Instantiate the Bull Put Spread
#

xom_spread = VerticalSpread(
	underlying_price = xom_price,
	K1 = long_put['strike'],
	K2 = short_put['strike'],
	premium1 = .5*(long_put['ask'] + long_put['bid']),
	premium2 = .5*(short_put['ask'] + short_put['bid']),
	spread_type='bull_put'
);


#
# Get relevant spread trade metrics
# 	• Breakeven Price
# 	• Maximum Loss Possible
# 	• Maximum Gain Possible
# 	• Risk-Reward Ratio
# 	• Return on Risk
#

# >>> xom_spread_metrics
# {'breakeven_price': 108.61, 'max_loss': 3.61, 'max_profit': 0.39, 'risk_reward_ratio': 9.2564, 'return_on_risk': 0.108}
xom_spread_metrics = xom_spread.metrics();



















