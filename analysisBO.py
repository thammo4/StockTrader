# FILE: `StockTrader/analysisBO.py`

#
# Analyze profit/loss profiles of potential options trades using recommendations from BinomialOptions output
#

from config import *

#
# Analyze Bull Call Spreads (DEBIT)
#

def analyze_bull_calls (df):
	calls = df[df['option_type'] == 'call'].copy();
	spreads = [];

	for _, long_call in calls.iterrows():
		for _, short_call in calls.iterrows():
			if long_call['strike'] < short_call['strike']:
				net_debit = long_call['ask'] - short_call['bid'];
				spread = short_call['strike'] - long_call['strike'];
				max_profit = spread - net_debit;
				max_risk = net_debit;
				breakeven = long_call['strike'] + net_debit;
				net_delta = long_call['Delta'] - short_call['Delta'];
				net_theta = long_call['Theta'] - short_call['Theta'];
				profit_prob = 1 - long_call['Delta'];

				spread_data = {
					'long_strike': long_call['strike'],
					'short_strike': short_call['strike'],
					'spread': spread,
					'debit': net_debit,
					'max_profit': max_profit,
					'max_risk': max_risk,
					'risk_reward_ratio': abs(max_risk / max_profit) if max_profit > 0 else float('inf'),
					'breakeven': breakeven,
					'net_delta': net_delta,
					'net_theta': net_theta,
					'profit_prob': profit_prob,
					'long_iv': long_call['IV'],
					'short_iv': short_call['IV'],
					'long_ivp': long_call['IVP'],
					'short_IVP': short_call['IVP'],
					'days_to_expiry': long_call['days_to_expiry'],
					'underlying': long_call['underlying'],
					'underlying_price': long_call['underlying_price']
				};
				spreads.append(spread_data);

	df_spreads = pd.DataFrame(spreads);
	df_spreads = df_spreads.loc[(df_spreads['debit'] > 0) & (df_spreads['risk_reward_ratio'].between(0,10,'both'))];

	return df_spreads;


#
# Analyze Bear Call Spreads (CREDIT)
#

def analyze_bear_calls (df):
	calls = df[df['option_type'] == 'call'].copy();
	spreads = [];

	for _, short_call in calls.iterrows():
		for _, long_call in calls.iterrows():
			if long_call['strike'] > short_call['strike']:
				net_credit = short_call['bid'] - long_call['ask'];
				spread = long_call['strike'] - short_call['strike'];
				max_risk = spread - net_credit;
				max_profit = net_credit;
				breakeven = short_call['strike'] + net_credit;
				net_delta = short_call['Delta'] - long_call['Delta'];
				net_theta = short_call['Theta'] - long_call['Theta'];
				profit_prob = 1 - short_call['Delta'];
				spread_data = {
					'short_strike': short_call['strike'],
					'long_strike': long_call['strike'],
					'spread': spread,
					'credit': net_credit,
					'max_risk': max_risk,
					'max_profit': max_profit,
					'risk_reward_ratio': abs(max_risk / max_profit) if max_profit > 0 else float('inf'),
					'breakeven': breakeven,
					'net_delta': net_delta,
					'net_theta': net_theta,
					'profit_prob': profit_prob,
					'short_iv': short_call['IV'],
					'long_iv': long_call['IV'],
					'short_ivp': short_call['IVP'],
					'long_ivp': long_call['IVP'],
					'days_to_expiry': short_call['days_to_expiry'],
					'underlying': short_call['underlying'],
					'underlying_price': short_call['underlying_price']
				};
				spreads.append(spread_data);

	df_spreads = pd.DataFrame(spreads);
	df_spreads = df_spreads.loc[(df_spreads['credit'] > 0) & (df_spreads['risk_reward_ratio'].between(0,10,'both'))];

	return df_spreads;


#
# Analyze Bull Put Spreads (CREDIT)
#

def analyze_bull_puts (df):
	puts = df[df['option_type'] == 'put'].copy();
	spreads = [];

	for _, short_put in puts.iterrows():
		for _, long_put in puts.iterrows():
			if short_put['strike'] > long_put['strike']:
				if short_put['strike'] > long_put['strike']:
					net_credit = short_put['bid'] - long_put['ask'];
					spread = short_put['strike'] - long_put['strike'];
					max_risk = spread - net_credit;
					max_profit = net_credit;
					breakeven = short_put['strike'] - net_credit;
					net_delta = short_put['Delta'] - long_put['Delta'];
					net_theta = short_put['Theta'] - long_put['Theta'];
					profit_prob = 1 + short_put['Delta'];

					spread_data = {
						'short_strike': short_put['strike'],
						'long_strike': long_put['strike'],
						'spread': spread,
						'credit': net_credit,
						'max_risk': max_risk,
						'max_profit': max_profit,
						'risk_reward_ratio': abs(max_risk/max_profit) if max_profit > 0 else float('inf'),
						'breakeven': breakeven,
						'net_delta': net_delta,
						'net_theta': net_theta,
						'profit_prob': profit_prob,
						'short_iv': short_put['IV'],
						'long_iv': long_put['IV'],
						'short_ivp': short_put['IVP'],
						'long_ivp': long_put['IVP'],
						'days_to_expiry': short_put['days_to_expiry'],
						'underlying': short_put['underlying'],
						'underlying_price': short_put['underlying_price']
					};

					spreads.append(spread_data);

	df_spreads = pd.DataFrame(spreads);
	df_spreads = df_spreads.loc[(df_spreads['credit'] > 0) & (df_spreads['risk_reward_ratio'].between(0,10,'both'))];

	return df_spreads;


#
# Analyze Bear Put Spreads (DEBIT)
#

def analyze_bear_puts (df):
	puts = df[df['option_type'] == 'put'].copy();
	spreads = [];

	for _, long_put in puts.iterrows():
		for _, short_put in puts.iterrows():
			if long_put['strike'] > short_put['strike']:
				net_debit = long_put['ask'] - short_put['bid'];
				spread = long_put['strike'] - short_put['strike'];
				max_profit = spread - net_debit;
				max_risk = net_debit;
				breakeven = long_put['strike'] - net_debit;
				net_delta = long_put['Delta'] - short_put['Delta'];
				net_theta = long_put['Theta'] - short_put['Theta'];
				profit_prob = 1 - abs(long_put['Delta']);

				spread_data = {
					'long_strike': long_put['strike'],
					'short_strike': short_put['strike'],
					'spread': spread,
					'debit': net_debit,
					'max_profit': max_profit,
					'max_risk': max_risk,
					'risk_reward_ratio': abs(max_risk/max_profit) if max_profit > 0 else float('inf'),
					'breakeven': breakeven,
					'net_delta': net_delta,
					'net_theta': net_theta,
					'profit_prob': profit_prob,
					'long_iv': long_put['IV'],
					'short_iv': short_put['IV'],
					'long_ivp': long_put['IVP'],
					'short_ivp': short_put['IVP'],
					'days_to_expiry': long_put['days_to_expiry'],
					'underlying': long_put['underlying'],
					'underlying_price': long_put['underlying_price']
				};

				print(spread_data);
				print('\n');

				spreads.append(spread_data);

	df_spreads = pd.DataFrame(spreads);
	df_spreads = df_spreads.loc[(df_spreads['debit'] > 0) & (df_spreads['risk_reward_ratio'].between(0,10,'both'))];

	return df_spreads;



# >>> dd
#                  symbol  strike   bid   ask    mp  bidsize  asksize  open_interest  volume option_type  ...     NPV   Delta   Gamma    Theta      IV       IVP  underlying  underlying_price  XTM           sector
# 1990  DD241129P00085000    85.0  3.60  3.90  3.75       20       92              9     0.0         put  ...  3.4925 -0.5363  0.0546 -11.9855  0.2880  0.871087          DD             83.67  ITM  Basic Materials
# 1991  DD241129P00084000    84.0  3.00  3.40  3.20       20       31              0     0.0         put  ...  2.9482 -0.4823  0.0546 -12.1427  0.2877  0.868852          DD             83.67  ITM  Basic Materials
# 1992  DD241129P00082000    82.0  2.15  2.45  2.30       22       14              2     0.0         put  ...  2.0262 -0.3749  0.0516 -11.7165  0.2911  0.857671          DD             83.67  OTM  Basic Materials
# 1993  DD241129P00080000    80.0  1.55  1.85  1.70       31       60              2     0.0         put  ...  1.3189 -0.2748  0.0452 -10.4073  0.3052  0.841393          DD             83.67  OTM  Basic Materials
# 1994  DD241129P00083000    83.0  2.45  2.85  2.65       89        3              5     0.0         put  ...  2.4660 -0.4282  0.0534 -12.0191  0.2817  0.836957          DD             83.67  OTM  Basic Materials
# 1995  DD241129P00086000    86.0  4.10  4.40  4.25       21       11              5     0.0         put  ...  4.0933 -0.5890  0.0536 -11.5870  0.2794  0.833024          DD             83.67  ITM  Basic Materials
# 1996  DD241129P00087000    87.0  4.80  5.00  4.90       17        5             23     0.0         put  ...  4.7319 -0.6405  0.0520 -11.0415  0.2816  0.818882          DD             83.67  ITM  Basic Materials
# 1997  DD241129P00081000    81.0  1.80  2.05  1.92       20       10              0     0.0         put  ...  1.6410 -0.3232  0.0488 -11.1828  0.2919  0.815029          DD             83.67  OTM  Basic Materials
# 1998  DD241129P00088000    88.0  5.50  5.80  5.65       21       63             22     0.0         put  ...  5.4296 -0.6888  0.0495 -10.2716  0.2868  0.800000          DD             83.67  ITM  Basic Materials
# 1999  DD241129C00086000    86.0  2.10  2.45  2.28       20       53              1     0.0        call  ...  2.1404  0.4155  0.0524 -13.8431  0.2778  0.781528          DD             83.67  OTM  Basic Materials
# 2000  DD241129C00083000    83.0  3.60  3.90  3.75       11       77              6     0.0        call  ...  3.5130  0.5735  0.0527 -14.2235  0.2865  0.774194          DD             83.67  ITM  Basic Materials
# 2001  DD241129C00085000    85.0  2.55  2.75  2.65       22       31              2     0.0        call  ...  2.5406  0.4672  0.0535 -14.2237  0.2748  0.772487          DD             83.67  OTM  Basic Materials
# 2002  DD241129P00078000    78.0  1.00  1.35  1.18       22       54              0     0.0         put  ...  0.7950 -0.1873  0.0365  -8.5082  0.3121  0.769397          DD             83.67  OTM  Basic Materials
# 2003  DD241129C00087000    87.0  1.75  2.05  1.90       12       56              2     0.0        call  ...  1.7758  0.3653  0.0507 -13.3135  0.2772  0.769369          DD             83.67  OTM  Basic Materials
# 2004  DD241129P00090000    90.0  7.00  7.60  7.30       21        1              0     0.0         put  ...  6.9419 -0.7769  0.0431  -8.4081  0.3050  0.764302          DD             83.67  ITM  Basic Materials
# 2005  DD241129C00084000    84.0  2.95  3.30  3.12       33       48              0     0.0        call  ...  2.9953  0.5203  0.0537 -14.3666  0.2762  0.756708          DD             83.67  OTM  Basic Materials
# 2006  DD241129C00082000    82.0  4.20  4.40  4.30       24       20              0     0.0        call  ...  4.0718  0.6261  0.0510 -13.8983  0.2868  0.749543          DD             83.67  ITM  Basic Materials
# 2007  DD241129P00079000    79.0  1.25  1.45  1.35       23        2             25     0.0         put  ...  1.0367 -0.2293  0.0410  -9.5119  0.3015  0.744467          DD             83.67  OTM  Basic Materials

# [18 rows x 22 columns]
# >>> analyze_bull_puts(dd)
#     short_strike  long_strike  spread  credit  max_risk  max_profit  risk_reward_ratio  breakeven  net_delta  net_theta  profit_prob  short_iv  long_iv  short_ivp  long_ivp  days_to_expiry underlying  underlying_price
# 0           85.0         84.0     1.0    0.20      0.80        0.20           4.000000      84.80    -0.0540     0.1572       0.4637     0.288   0.2877   0.871087  0.868852              31         DD             83.67
# 1           85.0         82.0     3.0    1.15      1.85        1.15           1.608696      83.85    -0.1614    -0.2690       0.4637     0.288   0.2911   0.871087  0.857671              31         DD             83.67
# 2           85.0         80.0     5.0    1.75      3.25        1.75           1.857143      83.25    -0.2615    -1.5782       0.4637     0.288   0.3052   0.871087  0.841393              31         DD             83.67
# 3           85.0         83.0     2.0    0.75      1.25        0.75           1.666667      84.25    -0.1081     0.0336       0.4637     0.288   0.2817   0.871087  0.836957              31         DD             83.67
# 4           85.0         81.0     4.0    1.55      2.45        1.55           1.580645      83.45    -0.2131    -0.8027       0.4637     0.288   0.2919   0.871087  0.815029              31         DD             83.67
# ..           ...          ...     ...     ...       ...         ...                ...        ...        ...        ...          ...       ...      ...        ...       ...             ...        ...               ...
# 60          90.0         87.0     3.0    2.00      1.00        2.00           0.500000      88.00    -0.1364     2.6334       0.2231     0.305   0.2816   0.764302  0.818882              31         DD             83.67
# 61          90.0         81.0     9.0    4.95      4.05        4.95           0.818182      85.05    -0.4537     2.7747       0.2231     0.305   0.2919   0.764302  0.815029              31         DD             83.67
# 62          90.0         88.0     2.0    1.20      0.80        1.20           0.666667      88.80    -0.0881     1.8635       0.2231     0.305   0.2868   0.764302  0.800000              31         DD             83.67
# 63          90.0         78.0    12.0    5.65      6.35        5.65           1.123894      84.35    -0.5896     0.1001       0.2231     0.305   0.3121   0.764302  0.769397              31         DD             83.67
# 64          90.0         79.0    11.0    5.55      5.45        5.55           0.981982      84.45    -0.5476     1.1038       0.2231     0.305   0.3015   0.764302  0.744467              31         DD             83.67

# [63 rows x 18 columns]
# >>> analyze_bear_puts(dd)
# >>> 




#
# EXAMPLE OF INPUT ARGUMENT DATAFRAME
# • Transpose shown for complete view of all columns
#

#
# I think this computes something related to butterfly spreads?
#

def bfly_credits (df_options):
	strikes = list(df_options['strike']);
	asks = list(df_options['ask']);
	bids = list(df_options['bid']);

	results = list();

	for i in range(len(strikes)):
		for j in range(i+1, len(strikes)):
			for k in range(j+1, len(strikes)):
				lower_strike, middle_strike, upper_strike = strikes[i], strikes[j], strikes[k];

				middle_short = 2*bids[j];
				lower_long = asks[i];
				upper_long = asks[k];

				net_premium = middle_short - (lower_long+upper_long);

				results.append({
					'low_strike': lower_strike,
					'mid_strike': middle_strike,
					'high_strike': upper_strike,
					'net': net_premium
				});

	df_results = pd.DataFrame(results);

	return df_results;
