# FILE: `kbr_bull_put_example.py`
from config import *
import matplotlib.pyplot as plt;
from scipy.optimize import brentq;


#
# Function to print payoff diagram
#

def bull_put_payoff_diagram (S_T, payoffs):
	plt.figure(figsize=(10,6));
	plt.plot(S_T, payoffs);
	plt.axhline(y=0, color='r', linestyle='--');
	plt.title('Bull Put Spread Payoff');
	plt.xlabel('Underlying Price at Expiry');
	plt.ylabel('Payoff');
	plt.grid(True);
	plt.show();


#
# Define function to determine the break-even point for a bull-put spread
#

def bull_put_breakeven (K1, K2, Pnet):
	'''
		Arguments:
			• K1: strike price of short put leg with higher strike
			• K2: strike price of long put leg with lower strike
			• Pnet: net credit received to enter the trade
		Returns:
			• S_T: Break even price for the underlying stock
	'''

	def objective_function (S_T):
		return (max(0, min(K1,S_T)-K2) - (K1-K2) + Pnet);

	lower_bound = K2;
	upper_bound = K1;

	try:
		breakeven = brentq(objective_function, lower_bound, upper_bound);
		return breakeven;
	except ValueError:
		print('No breakeven found');
		return None;



#
# Define function to compute payoff for various underlying prices at expiry
#

def bull_put_payoff (K1, K2, Pnet, S_T):
	'''
		Arguments:
			• K1: strike price, short put leg
			• K2: strike price, long put leg
			• Pnet: net credit received to enter trade
			• S_T: list of underlying prices as expiry t=T
		Returns:
			• numpy array of payoffs for each S_T
	'''
	S_T = np.array(S_T);

	payoff = np.maximum(0, np.minimum(K1,S_T) - K2) - (K1-K2) + Pnet;
	return payoff;


# CURRENT KBR PRICE:
#
# >>> kbr_price
# 64.14
kbr_price = quotes.get_quote_day('KBR', last_price=True);

# >>> kbr_expiries
# ['2024-07-19', '2024-08-16', '2024-09-20', '2024-11-15', '2024-12-20']
kbr_expiries = options_data.get_expiry_dates('KBR');


lower_strike, upper_strike = kbr_price + np.array([-12,12]);


# >>> kbr_options816
#                 symbol  last   bid    ask  strike  last_volume     trade_date  ...  bidexch       bid_date asksize  askexch       ask_date open_interest  option_type
# 14  KBR240816P00055000   NaN  0.15   0.30    55.0            0              0  ...        Z  1719604797000     146        C  1719604683000             0          put
# 15  KBR240816C00055000   NaN  7.80  11.40    55.0            0              0  ...        H  1719604789000       1        A  1719604791000             0         call
# 16  KBR240816C00057500  7.00  5.30   8.90    57.5            3  1719413943533  ...        X  1719604790000      10        X  1719604790000            14         call
# 17  KBR240816P00057500   NaN  0.35   0.50    57.5            0              0  ...        B  1719604538000      41        X  1719604791000             0          put
# 18  KBR240816P00060000  0.90  0.70   0.90    60.0            1  1718983804099  ...        B  1719604795000      21        M  1719604799000             1          put
# 19  KBR240816C00060000   NaN  5.10   5.50    60.0            0              0  ...        B  1719604789000      14        M  1719604794000             0         call
# 20  KBR240816C00062500   NaN  3.30   3.70    62.5            0              0  ...        H  1719604786000      40        X  1719604788000             0         call
# 21  KBR240816P00062500  1.35  1.35   1.65    62.5            1  1719249246881  ...        B  1719604786000      23        X  1719604795000             4          put
# 22  KBR240816P00065000   NaN  2.35   2.75    65.0            0              0  ...        B  1719604636000      11        B  1719604776000             0          put
# 23  KBR240816C00065000  1.85  2.00   2.25    65.0            1  1719412142093  ...        H  1719604794000      20        X  1719604793000             2         call
# 24  KBR240816C00067500  1.45  0.95   1.20    67.5            1  1719249406965  ...        W  1719604794000       3        A  1719604773000            56         call
# 25  KBR240816P00067500   NaN  2.40   5.00    67.5            0              0  ...        B  1719604798000       3        N  1719604799000             0          put
# 26  KBR240816P00070000   NaN  5.20   7.30    70.0            0              0  ...        P  1719604795000      12        B  1719604786000             0          put
# 27  KBR240816C00070000  0.65  0.55   0.70    70.0            3  1719517815002  ...        X  1719604777000      38        E  1719604780000             5         call
# 28  KBR240816C00072500   NaN  0.25   0.40    72.5            0              0  ...        P  1719604790000       5        H  1719604790000             0         call
# 29  KBR240816P00072500   NaN  6.30  10.50    72.5            0              0  ...        X  1719604798000       1        C  1719604797000             0          put
# 30  KBR240816P00075000   NaN  9.30  12.90    75.0            0              0  ...        H  1719604790000      20        X  1719604787000             0          put
# 31  KBR240816C00075000   NaN  0.05   0.25    75.0            0              0  ...        C  1719604785000      20        X  1719604785000             0         call

# [18 rows x 16 columns]
kbr_options816 = options_data.get_chain_day(symbol='KBR', expiry='2024-08-16', strike_low=lower_strike, strike_high=upper_strike);



bull_put_occs = ['KBR240816P00062500', 'KBR240816P00060000'];
bull_put_legs = kbr_options816.query("symbol in @bull_put_occs");


#
# Isolate the two legs of the bull put spread
#	• Short put
# 	• Long put
#

# short_put_leg = bull_put_legs.iloc[0];
# long_put_leg = bull_put_legs.iloc[1];
# short_put_leg = bull_put_legs.query("strike == 62.5");
# long_put_leg = bull_put_legs.query("strike == 60.0");

short_put_leg = bull_put_legs.loc[bull_put_legs['strike'].idxmax()];
long_put_leg = bull_put_legs.loc[bull_put_legs['strike'].idxmin()];

short_put_strike = short_put_leg['strike'];
long_put_strike = long_put_leg['strike'];



#
# Compute bid-ask midpoint price to use as each contract's premium
#

mp_short_put = .5*(short_put_leg['bid'] + short_put_leg['ask']);
mp_long_put = .5*(long_put_leg['bid'] + long_put_leg['ask']);



#
# Compute net premium received to enter position
#

net_premium_received = mp_short_put - mp_long_put;



#
# Compute max loss and max profit
#

max_loss = np.abs(short_put_strike - long_put_strike) - net_premium_received;
max_profit = net_premium_received;


#
# Determine breakeven price
#

underlying_breakeven_price = bull_put_breakeven(K1=short_put_strike, K2=long_put_strike, Pnet=net_premium_received);


#
# Compute payoffs for various underlying prices at expiry
#

# S_T = np.linspace(52, 75, 90);
underlying_prices = np.linspace(52, 75, 90);

payoff_values = bull_put_payoff(K1=short_put_strike, K2=long_put_strike, Pnet=net_premium_received, S_T=underlying_prices);


#
# Evaluate risk and return
# 	• risk reward ratio: max_loss / max_profit
# 	• return on risk: max_profit / max_loss
#

risk_return_ratio = max_loss / max_profit;
return_on_risk = 1 / risk_return_ratio;




print(f"Short Put Leg: {pd.DataFrame(short_put_leg).T}");
print(f"Long Put Leg: {pd.DataFrame(long_put_leg).T}");
print('\n');
print(f'Current Underlying: ${kbr_price}');
print(f'Net Premium Received: ${net_premium_received}');
print(f'Max Loss: ${max_loss}');
print(f'Max Profit: ${max_profit}');
print(f'Underlying Breakeven Price: ${underlying_breakeven_price}');
print(f'Risk Reward Ratio: {risk_return_ratio:.2f}');
print(f'Return on Risk: {return_on_risk:.2f}');



#
# Plot Payoff Diagram
#

bull_put_payoff_diagram(S_T=underlying_prices, payoffs=payoff_values);