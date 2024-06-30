from config import *
from scipy.optimize import brentq;
import matplotlib.pyplot as plt;




#
# Function to print payoff diagram
#

def bull_call_payoff_diagram (S_T, payoffs):
	plt.figure(figsize=(10,6));
	plt.plot(S_T, payoffs);
	plt.axhline(y=0, color='r', linestyle='--');
	plt.title('Bull Call Spread Payoff');
	plt.xlabel('Underlying Price at Expiry');
	plt.ylabel('Payoff');
	plt.grid(True);
	plt.show();


#
# Define function to compute the break-even point of a bull-call spread
#

def bull_call_breakeven (K1, K2, Cnet):
	'''
		Arguments:
			• K1: strike price of long call leg
			• K2: strike price of short call leg
			• Cnet: net debit paid to enter the trade
		Returns:
			• S_T: Break even price for the underlying stock
	'''

	def objective_function (S_T):
		return (max(0, S_T-min(K1,K2)) - max(0, S_T-max(K1,K2)) - Cnet);

	lower_bound = min(K1, K2);
	upper_bound = max(K1, K2) + Cnet;

	try:
		breakeven = brentq(objective_function, lower_bound, upper_bound);
		return breakeven;
	except ValueError:
		print("No breakeven point found");
		return None;


#
# Define function to compute payoff for various prices of underlying at expiry
#

def bull_call_payoff(K1, K2, Cnet, S_T):
	'''
		Arguments:
			• K1: strike price, long call leg
			• K2: strike price, short call leg
			• Cnet: net debit paid to enter trade
			• S_T: list of underlying prices as expiry t=T
		Returns:
			• numpy array of payoffs for each S_T
	'''

	S_T = np.array(S_T);

	payoff = np.maximum(0, S_T-min(K1,K2)) - np.maximum(0, S_T-max(K1,K2)) - Cnet;

	return payoff;



#
# Determine today's date
#

# >>> today
# '2024-06-29'


#
# Get current price of Exxon Mobil
#

# >>> xom_price
# 115.12
xom_price = quotes.get_quote_day(symbol='XOM', last_price=True);


#
# Define strike range to be plus/minus 10 dollars apropos current price and convert to integers.
#

# >>> xom_strike_range
# array([105.12, 125.12])
xom_strike_range = xom_price + np.array([-10, 10]);

# >>> xom_strike_range
# [105, 125]
xom_strike_range = [int(x) for x in xom_strike_range];


#
# Retrieve expiration dates for Exxon options
#

# >>> xom_options_expiries
# ['2024-07-05', '2024-07-12', '2024-07-19', '2024-07-26', '2024-08-02', '2024-08-09', '2024-08-16', '2024-09-20', '2024-10-18', '2024-12-20', '2025-01-17', '2025-03-21', '2025-06-20', '2025-12-19', '2026-01-16', '2026-12-18']
xom_options_expiries = options_data.get_expiry_dates(symbol='XOM');


#
# Fetch options chain for Exxon contracts with July 26 expiry
#


# >>> xom_options726.columns
# Index(['symbol', 'last', 'change', 'volume', 'open', 'high', 'low', 'close',
#        'bid', 'ask', 'strike', 'change_percentage', 'last_volume',
#        'trade_date', 'prevclose', 'bidsize', 'bidexch', 'bid_date', 'asksize',
#        'askexch', 'ask_date', 'open_interest', 'option_type'],
#       dtype='object')
# >>> xom_options726
#                 symbol  last  change  volume  open  high   low  close    bid    ask  strike  change_percentage  last_volume     trade_date  prevclose  bidsize bidexch       bid_date  asksize askexch       ask_date  open_interest option_type
# 30  XOM240726P00105000  0.21   -0.04      14  0.18  0.21  0.18   0.21   0.11   0.49   105.0             -16.00            3  1719600898334       0.25      264       E  1719604782000      241       E  1719604783000            142         put
# 31  XOM240726C00105000  5.89    0.00       0   NaN   NaN   NaN    NaN  10.20  11.60   105.0               0.00            1  1718637168453       5.89       16       H  1719604784000       55       N  1719604704000              2        call
# 32  XOM240726P00106000  0.24   -0.25       3  0.24  0.24  0.24   0.24   0.24   1.30   106.0             -51.02            3  1719590750380       0.49       84       C  1719604783000       57       X  1719604748000             35         put
# 33  XOM240726C00106000   NaN     NaN       0   NaN   NaN   NaN    NaN   9.30  10.70   106.0                NaN            0              0        NaN       17       H  1719604784000       43       N  1719604748000              0        call
# 34  XOM240726P00107000  0.38   -0.04      11  0.37  0.38  0.29   0.38   0.21   0.48   107.0              -9.53            4  1719604272021       0.42      415       E  1719604748000      275       M  1719604748000             19         put
# 35  XOM240726C00107000  8.50    0.00       0   NaN   NaN   NaN    NaN   8.35   9.10   107.0               0.00            1  1719430095734       8.50       17       H  1719604784000       10       H  1719604784000             14        call
# 36  XOM240726P00108000  0.34   -0.19      12  0.41  0.41  0.33   0.34   0.35   1.61   108.0             -35.85            1  1719591645480       0.53      143       E  1719604782000      244       E  1719604782000            108         put
# 37  XOM240726C00108000  7.25    0.00       0   NaN   NaN   NaN    NaN   7.25   8.05   108.0               0.00            1  1719340009200       7.25      293       E  1719604783000       18       Z  1719604782000             27        call
# 38  XOM240726P00109000  0.40   -0.28       3  0.40  0.40  0.40   0.40   0.51   1.09   109.0             -41.18            3  1719581955259       0.68       79       P  1719604782000       17       N  1719604784000            165         put
# 39  XOM240726C00109000  6.35    0.00       0   NaN   NaN   NaN    NaN   6.80   8.00   109.0               0.00           15  1719420044049       6.35      200       E  1719604746000      121       A  1719604747000             33        call
# 40  XOM240726P00110000  0.60   -0.24      27  0.58  0.60  0.50   0.60   0.65   0.75   110.0             -28.58           10  1719595356087       0.84       97       T  1719604746000       89       E  1719604742000            263         put
# 41  XOM240726C00110000  6.25    0.70       6  6.50  6.65  6.00   6.25   5.85   6.75   110.0              12.62            1  1719604092960       5.55      104       C  1719604783000       92       C  1719604784000            112        call
# 42  XOM240726P00111000  0.76   -0.30      10  0.76  0.76  0.76   0.76   0.80   1.00   111.0             -28.31           10  1719592846931       1.06      214       E  1719604783000       93       E  1719604784000             23         put
# 43  XOM240726C00111000  5.44    0.19      39  5.65  5.65  5.41   5.44   5.10   5.90   111.0               3.62            7  1719598717736       5.25      246       E  1719604783000      106       E  1719604742000            239        call
# 44  XOM240726P00112000  1.12   -0.20      25  1.09  1.12  0.89   1.12   1.07   1.21   112.0             -15.16            1  1719599362433       1.32      153       E  1719604782000       13       N  1719604784000             74         put
# 45  XOM240726C00112000  4.60    0.00       0   NaN   NaN   NaN    NaN   3.75   4.70   112.0               0.00            1  1719518392324       4.60      328       E  1719604784000       17       P  1719604784000            237        call
# 46  XOM240726P00113000  1.19   -0.51      17  0.85  1.27  0.85   1.19   1.25   1.50   113.0             -30.00            2  1719594496345       1.70      202       E  1719604783000      132       E  1719604782000             27         put
# 47  XOM240726C00113000  3.85    0.05       8  4.35  4.70  3.85   3.85   3.80   4.00   113.0               1.32            1  1719604722525       3.80      132       E  1719604783000       47       X  1719604782000            253        call
# 48  XOM240726P00114000  1.73   -0.45       5  1.76  1.76  1.73   1.73   1.68   1.90   114.0             -20.65            3  1719596021267       2.18      203       E  1719604782000       82       E  1719604784000             81         put
# 49  XOM240726C00114000  3.05   -0.20     196  4.20  4.25  3.05   3.05   3.20   3.35   114.0              -6.16            4  1719604611998       3.25       53       B  1719604783000       45       X  1719604784000            198        call
# 50  XOM240726P00115000  2.13   -0.38      15  1.80  2.13  1.77   2.13   2.04   2.46   115.0             -15.14            3  1719596021267       2.51       72       B  1719604784000      401       E  1719604783000             53         put
# 51  XOM240726C00115000  2.65    0.12     511  3.25  3.55  2.51   2.65   2.63   2.77   115.0               4.75            1  1719604722525       2.53        4       W  1719604784000       22       B  1719604783000           1435        call
# 52  XOM240726P00116000  2.60   -0.51       9  2.41  2.60  2.25   2.60   2.36   2.97   116.0             -16.40            1  1719596121063       3.11       52       U  1719604749000      479       E  1719604784000              4         put
# 53  XOM240726C00116000  2.12    0.13     205  2.80  2.86  2.03   2.12   1.90   2.37   116.0               6.54            1  1719603553427       1.99      680       E  1719604783000      413       C  1719604782000            443        call
# 54  XOM240726P00117000  3.25   -0.45      94  3.25  3.25  3.25   3.25   2.94   5.00   117.0             -12.17           18  1719584531769       3.70      740       E  1719604744000      195       X  1719604745000              5         put
# 55  XOM240726C00117000  1.70   -0.05       9  2.35  2.35  1.70   1.70   1.51   1.99   117.0              -2.86            1  1719601236751       1.75      573       E  1719604782000      801       E  1719604782000            126        call
# 56  XOM240726P00118000  3.65   -5.51       1  3.65  3.65  3.65   3.65   2.99   4.95   118.0             -60.16            1  1719590150471       9.16      451       E  1719604783000      327       E  1719604783000              4         put
# 57  XOM240726C00118000  1.25    0.03      61  1.77  1.77  1.25   1.25   1.30   1.97   118.0               2.46            1  1719604669948       1.22       82       E  1719604783000      756       E  1719604783000            567        call
# 58  XOM240726P00119000  6.25    0.00       0   NaN   NaN   NaN    NaN   3.55   5.35   119.0               0.00            1  1717686834278       6.25      660       E  1719604782000      582       E  1719604782000              2         put
# 59  XOM240726C00119000  1.03    0.08      77  1.36  1.37  1.03   1.03   0.85   1.12   119.0               8.43            1  1719601198765       0.95      176       E  1719604784000       38       P  1719604783000           1177        call
# 60  XOM240726P00120000  5.88    0.00       0   NaN   NaN   NaN    NaN   4.95   6.40   120.0               0.00            1  1719424063426       5.88      292       E  1719604783000      346       E  1719604783000              3         put
# 61  XOM240726C00120000  0.75   -0.08     208  1.09  1.15  0.75   0.75   0.57   1.22   120.0              -9.64            1  1719600968384       0.83      856       A  1719604745000      796       E  1719604748000            881        call
# 62  XOM240726P00121000   NaN     NaN       0   NaN   NaN   NaN    NaN   5.90   6.70   121.0                NaN            0              0        NaN      232       E  1719604782000      163       E  1719604782000              0         put
# 63  XOM240726C00121000  0.62    0.03      16  0.74  0.74  0.58   0.62   0.58   0.68   121.0               5.09            1  1719604719637       0.59       39       E  1719604783000        1       D  1719604748000            108        call
# 64  XOM240726P00122000   NaN     NaN       0   NaN   NaN   NaN    NaN   6.90   7.65   122.0                NaN            0              0        NaN       18       C  1719604782000       55       Z  1719604783000              0         put
# 65  XOM240726C00122000  0.44    0.10     198  0.67  0.67  0.44   0.44   0.33   0.62   122.0              29.42            1  1719600677863       0.34       51       Z  1719604784000      100       B  1719604749000            507        call
# 66  XOM240726P00123000   NaN     NaN       0   NaN   NaN   NaN    NaN   7.90   8.55   123.0                NaN            0              0        NaN       17       H  1719604783000       19       H  1719604783000              0         put
# 67  XOM240726C00123000  0.39    0.05       1  0.39  0.39  0.39   0.39   0.31   0.40   123.0              14.71            1  1719590301514       0.34       84       E  1719604782000        1       H  1719604742000             11        call
# 68  XOM240726P00124000   NaN     NaN       0   NaN   NaN   NaN    NaN   8.10  10.10   124.0                NaN            0              0        NaN       47       N  1719604655000       43       Z  1719604704000              0         put
# 69  XOM240726C00124000  0.23    0.00       0   NaN   NaN   NaN    NaN   0.22   1.31   124.0               0.00            1  1719325843778       0.23      262       E  1719604744000      493       E  1719604746000              5        call
# 70  XOM240726P00125000   NaN     NaN       0   NaN   NaN   NaN    NaN   8.30  11.40   125.0                NaN            0              0        NaN       44       Z  1719604748000       44       Z  1719604748000              0         put
# 71  XOM240726C00125000  0.18    0.01     104  0.30  0.31  0.18   0.18   0.16   0.27   125.0               5.89           29  1719601376696       0.17      256       B  1719604742000       66       Q  1719604783000            248        call

xom_options726 = options_data.get_chain_day(symbol='XOM', expiry='2024-07-26', strike_low=xom_strike_range[0], strike_high=xom_strike_range[1]);


# >>> bull_call_legs
#                 symbol  last  change  volume  open  high   low  close   bid   ask  strike  change_percentage  last_volume     trade_date  prevclose  bidsize bidexch       bid_date  asksize askexch       ask_date  open_interest option_type
# 53  XOM240726C00116000  2.12    0.13     205  2.80  2.86  2.03   2.12  1.90  2.37   116.0               6.54            1  1719603553427       1.99      680       E  1719604783000      413       C  1719604782000            443        call
# 61  XOM240726C00120000  0.75   -0.08     208  1.09  1.15  0.75   0.75  0.57  1.22   120.0              -9.64            1  1719600968384       0.83      856       A  1719604745000      796       E  1719604748000            881        call
example_symbols = ['XOM240726C00116000', 'XOM240726C00120000'];
bull_call_legs = xom_options726.query("symbol in @example_symbols");


#
# Isolate the two legs
#

# >>> long_call_leg
# symbol               XOM240726C00116000
# last                               2.12
# change                             0.13
# volume                              205
# open                                2.8
# high                               2.86
# low                                2.03
# close                              2.12
# bid                                 1.9
# ask                                2.37
# strike                            116.0
# change_percentage                  6.54
# last_volume                           1
# trade_date                1719603553427
# prevclose                          1.99
# bidsize                             680
# bidexch                               E
# bid_date                  1719604783000
# asksize                             413
# askexch                               C
# ask_date                  1719604782000
# open_interest                       443
# option_type                        call
# Name: 53, dtype: object
#
# >>> short_call_leg
# symbol               XOM240726C00120000
# last                               0.75
# change                            -0.08
# volume                              208
# open                               1.09
# high                               1.15
# low                                0.75
# close                              0.75
# bid                                0.57
# ask                                1.22
# strike                            120.0
# change_percentage                 -9.64
# last_volume                           1
# trade_date                1719600968384
# prevclose                          0.83
# bidsize                             856
# bidexch                               A
# bid_date                  1719604745000
# asksize                             796
# askexch                               E
# ask_date                  1719604748000
# open_interest                       881
# option_type                        call
# Name: 61, dtype: object

long_call_leg = bull_call_legs.iloc[0];
short_call_leg = bull_call_legs.iloc[1];



#
# Compute bid-ask midpoint price
#

# >>> [mp_long_call, mp_short_call]
# [2.135, 0.895]

mp_long_call = .5*(long_call_leg['bid'] + long_call_leg['ask']);
mp_short_call = .5*(short_call_leg['bid'] + short_call_leg['ask']);



#
# Compute net premium paid to enter position
#

# >>> net_premium_paid
# 1.24
net_premium_paid = np.round(mp_long_call - mp_short_call, 3);



#
# Compute max loss and max profit
#

# >>> [max_loss, max_profit]
# [1.24, 2.76]
max_loss = net_premium_paid;
max_profit = np.abs(long_call_leg['strike'] - short_call_leg['strike']) - net_premium_paid;


#
# Determine break-even price
#

underlying_breakeven_price = bull_call_breakeven(K1=long_call_leg['strike'], K2=short_call_leg['strike'], Cnet=net_premium_paid);



#
# Determine payoff for various prices of underlying at expiry
#

payoff_values = bull_call_payoff(K1=long_call_leg['strike'], K2=short_call_leg['strike'], Cnet=net_premium_paid, S_T=np.linspace(105, 126, 90));



#
# Evaluate trade per return and risk
# 	• risk reward ratio: (max loss) / (max profit)
# 	• return on risk: (max profit) / (max loss)
#

risk_reward_ratio = max_loss / max_profit;
return_on_risk = 1 / risk_reward_ratio;




#
# Print Results Summary
#

# Long Call Leg:                 symbol  last change volume open  high   low close  bid   ask strike change_percentage last_volume     trade_date prevclose bidsize bidexch       bid_date asksize askexch       ask_date open_interest option_type
# 53  XOM240726C00116000  2.12   0.13    205  2.8  2.86  2.03  2.12  1.9  2.37  116.0              6.54           1  1719603553427      1.99     680       E  1719604783000     413       C  1719604782000           443        call.
# Short Call Leg:                 symbol  last change volume  open  high   low close   bid   ask strike change_percentage last_volume     trade_date prevclose bidsize bidexch       bid_date asksize askexch       ask_date open_interest option_type
# 61  XOM240726C00120000  0.75  -0.08    208  1.09  1.15  0.75  0.75  0.57  1.22  120.0             -9.64           1  1719600968384      0.83     856       A  1719604745000     796       E  1719604748000           881        call.


# Current Underlying: $115.12.
# Net Premium Paid: $1.24.
# Max Loss: $1.24.
# Max Profit: $2.76.
# Underlying Breakeven Price: $117.24.
# Risk Reward Ratio: 0.45.
# Return on Risk: 2.23.

print(f'Long Call Leg: {pd.DataFrame(long_call_leg).T}.');
print(f'Short Call Leg: {pd.DataFrame(short_call_leg).T}.');
print('\n');
print(f'Current Underlying: ${xom_price}.');
print(f'Net Premium Paid: ${net_premium_paid}.');
print(f'Max Loss: ${max_loss}.');
print(f'Max Profit: ${max_profit}.');
print(f'Underlying Breakeven Price: ${underlying_breakeven_price}.');
print(f'Risk Reward Ratio: {risk_reward_ratio:.2f}.');
print(f'Return on Risk: {return_on_risk:.2f}.');



#
# Plot Payoff Diagram
#

S_T=np.linspace(105, 126, 90);

bull_call_payoff_diagram(S_T=S_T, payoffs=payoff_values);