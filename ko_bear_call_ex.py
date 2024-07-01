from config import *
from VerticalSpread import VerticalSpread


#
# Retrieve current price of Coke
#

# >>> ko_price
# 63.415

ko_price = quotes.get_quote_day(symbol='KO', last_price=True);



#
# Determine expiry date ≈ 30 days in future
#

# >>> ko_target_expiry
# '2024-08-02'
ko_target_expiry = options_data.get_closest_expiry(symbol='KO', num_days=30);



#
# Calculate strike price target range for bear call spread
# 	• Short Call Leg with Strike: K_long <= S_t
# 	• Long Call Leg with Strike > S_t > K_short
#

# >>> [lower_strike, upper_strike]
# [61.829625, 66.58575]

lower_strike = .975 * ko_price;
upper_strike = 1.05 * ko_price;



#
# Retrieve options chain for call options in strike range with expiry ≈ 30 day in future
#


# >>> ko_options_chain
#                symbol  last  change  volume  open  high   low   bid   ask  strike  change_percentage  last_volume     trade_date  prevclose  bidsize bidexch       bid_date  asksize askexch       ask_date  open_interest option_type
# 29  KO240802C00062000  2.39    0.00       0   NaN   NaN   NaN  1.98  2.22    62.0               0.00           21  1719595106451       2.39      353       A  1719847402000      149       X  1719847402000            124        call
# 31  KO240802C00063000  1.48   -0.12      20  2.03  2.03  1.48  1.45  1.51    63.0              -7.50            8  1719847082607       1.60       10       D  1719847400000     1325       X  1719847396000             78        call
# 33  KO240802C00064000  0.95   -0.11       8  1.13  1.25  0.95  0.88  0.93    64.0             -10.38            2  1719846684441       1.06      185       D  1719847399000     1169       X  1719847397000            111        call
# 35  KO240802C00065000  0.55   -0.06       5  0.57  0.80  0.55  0.46  0.51    65.0              -9.84            1  1719845743598       0.61      281       X  1719847401000       84       X  1719847401000           2859        call
# 37  KO240802C00066000  0.25   -0.04     365  0.34  0.38  0.25  0.22  0.34    66.0             -13.80            3  1719847095066       0.29      825       X  1719847402000      271       X  1719847402000             65        call

ko_options_chain = options_data.get_chain_day(
	symbol = 'KO',
	expiry = ko_target_expiry,
	strike_low = lower_strike,
	strike_high = upper_strike,
	option_type = 'call'
);



#
# Extract legs of trade from options trade DataFrame
#

# >>> short_call
# symbol               KO240802C00063000
# last                              1.48
# change                           -0.12
# volume                              20
# open                              2.03
# high                              2.03
# low                               1.48
# bid                               1.45
# ask                               1.51
# strike                            63.0
# change_percentage                 -7.5
# last_volume                          8
# trade_date               1719847082607
# prevclose                          1.6
# bidsize                             10
# bidexch                              D
# bid_date                 1719847400000
# asksize                           1325
# askexch                              X
# ask_date                 1719847396000
# open_interest                       78
# option_type                       call
# Name: 31, dtype: object
#
# >>> long_call
# symbol               KO240802C00064000
# last                              0.95
# change                           -0.11
# volume                               8
# open                              1.13
# high                              1.25
# low                               0.95
# bid                               0.88
# ask                               0.93
# strike                            64.0
# change_percentage               -10.38
# last_volume                          2
# trade_date               1719846684441
# prevclose                         1.06
# bidsize                            185
# bidexch                              D
# bid_date                 1719847399000
# asksize                           1169
# askexch                              X
# ask_date                 1719847397000
# open_interest                      111
# option_type                       call
# Name: 33, dtype: object

short_call = ko_options_chain.iloc[1];
long_call = ko_options_chain.iloc[-3];



#
# Instantiate the VerticalSpread Bear Call Spread
#

ko_spread = VerticalSpread(
	underlying_price = ko_price,
	K1 = short_call['strike'],
	K2 = long_call['strike'],
	premium1 = .5*(short_call['bid'] + short_call['ask']),
	premium2 = .5*(long_call['bid'] + long_call['ask']),
	spread_type = 'bear_call'
);



#
# Get relevant spread trade metrics
# 	• Breakeven Price
# 	• Maximum Loss Possible
# 	• Maximum Gain Possible
# 	• Risk-Reward Ratio
# 	• Return on Risk
#

# >>> ko_spread_metrics
# {'breakeven_price': 63.575, 'max_loss': 0.425, 'max_profit': 0.575, 'risk_reward_ratio': 0.7391, 'return_on_risk': 1.3529}
ko_spread_metrics = ko_spread.metrics()













