from config import *

class CoveredCall:
	def __init__ (self, underlying_price, K_short_call, premium_short_call):
		self.underlying_price = underlying_price;
		self.K_short_call = K_short_call;
		self.premium_short_call = self.premium_short_call;