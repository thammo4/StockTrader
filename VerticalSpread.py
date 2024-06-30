from config import *


class VerticalSpread:
	def __init__ (self, underlying_price, K1, K2, premium1, premium2, spread_type):
		self.underlying_price = underlying_price;

		#
		# Define Upper/Lower Strikes
		# 	• K1 = lower strike
		# 	• K2 = upper strike
		#

		self.K1 = min(K1, K2);
		self.K2 = max(K1, K2);

		self.premium1 = np.round(premium1, 4);
		self.premium2 = np.round(premium2, 4);
		self.premium = np.round(abs(self.premium1-self.premium2), 4);

		self.spread_type = spread_type;

		self.price_range = np.linspace(.8*self.underlying_price, 1.2*self.underlying_price, 200);

		vertical_spreads = ['bear_call', 'bear_put', 'bull_call', 'bull_put'];

		if spread_type not in vertical_spreads:
			raise ValueError('Invalid Spread type');

		self.is_credit_spread = spread_type in ['bear_call', 'bull_put'];
		self.is_debit_spread = not self.is_credit_spread;


		print(f'Lower Strike: {self.K1}');
		print(f'Upper Strike: {self.K2}');
		print(f'Lower Strike Premium: {self.premium1}');
		print(f'Upper Strike Premium: {self.premium2}');
		print(f'Net Premium: {self.premium}');
		print(f'Credit Spread: {self.is_credit_spread}');
		print(f'Debit Spread: {self.is_debit_spread}');

	def get_payoff (self, S_T):
		if self.spread_type == 'bull_call':
			return np.maximum(S_T-self.K1,0) - np.maximum(S_T-self.K2,0) - self.premium;
		elif self.spread_type == 'bull_put':
			return np.maximum(self.K1-S_T,0) - np.maximum(self.K2-S_T,0) + self.premium;
		elif self.spread_type == 'bear_call':
			return np.maximum(S_T-self.K2,0) - np.maximum(S_T-self.K1,0) + self.premium;
		elif self.spread_type == 'bear_put':
			return np.maximum(self.K2-S_T,0) - np.maximum(self.K1-S_T,0) - self.premium;
		else:
			return np.array();


	def breakeven_price (self):
		def objective_function (S_T):
			if self.spread_type == 'bull_call':
				return np.maximum(S_T-self.K1,0) - np.maximum(S_T-self.K2,0) - self.premium;
			elif self.spread_type == 'bull_put':
				return np.maximum(self.K1-S_T,0) - np.maximum(self.K2-S_T,0) + self.premium;
			elif self.spread_type == 'bear_call':
				return np.maximum(S_T-self.K2,0) - np.maximum(S_T-self.K1,0) + self.premium;
			elif self.spread_type == 'bear_put':
				return np.maximum(self.K2-S_T,0) - np.maximum(self.K1-S_T,0) - self.premium;

		lower_bound = self.K1;
		upper_bound = self.K2;

		print(f'Bounds: ({lower_bound, upper_bound})');

		if self.is_credit_spread:
			upper_bound += self.premium;
		else:
			lower_bound -= self.premium;

		print(f'Bounds: ({lower_bound, upper_bound})');

		try:
			return brentq(objective_function, lower_bound, upper_bound);
		except ValueError:
			print('wtf breakeven');
			return None;

	def max_loss (self):
		if self.is_credit_spread:
			return (self.K2 - self.K1) - self.premium;
		else:
			return self.premium;

	def max_profit (self):
		if self.is_credit_spread:
			return self.premium;
		else:
			return (self.K2 - self.K1) - self.premium;

	def risk_reward_ratio (self):
		return self.max_loss() / self.max_profit();

	def return_on_risk (self):
		return self.max_profit() / self.max_loss();

	def payoff_diagram (self):
		payoff_values = self.get_payoff(self.price_range);
		plt.figure(figsize=(10,6));
		plt.plot(self.price_range, payoff_values);
		plt.axhline(y=0, color='r', linestyle='--');
		plt.axvline(x=self.underlying_price, color='g', linestyle='--', label='Current Price');
		plt.title(f"{self.spread_type.replace('_', ' ').title()} Spread Payoff");
		plt.xlabel('Underlying Price at Expiry');
		plt.ylabel('Payoff');
		plt.grid(True);
		plt.legend();
		plt.show();