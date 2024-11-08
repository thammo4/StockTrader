# FILE: `StockTrader/analysisBO.py`

#
# Analyze profit/loss profiles of potential options trades using recommendations from BinomialOptions output
#

from config import *

#
# Analyze Bull Call Spreads
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
# Analyze Bear Call Spreads
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
# Analyze Bull Put Spreads
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
# Analyze Bear Put Spreads
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
				breakeven = long_put['strike'] - net_debit;
				net_delta = long_put['Delta'] - short_put['Delta'];
				net_theta = long_put['Theta'] - short_put['Theta'];
				# profit_prob = 


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
