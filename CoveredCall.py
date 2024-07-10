from config import *

jpm_price = quotes.get_quote_day('JPM', last_price=True);

jpm_options = options_data.get_chain_day(
	symbol = 'JPM',
	expiry = options_data.get_closest_expiry('JPM', 14),
	strike_low = .95*jpm_price,
	strike_high = 1.05*jpm_price
);

class CoveredCall:
	def __init__ (self, underlying_price, K_short_call, premium_short_call):
		self.underlying_price = underlying_price;
		self.K_short_call = K_short_call;
		self.premium_short_call = self.premium_short_call;

		self.price_range = np.linspace(.8*self.underlying_price, 1.2*self.underlying_price, 200);