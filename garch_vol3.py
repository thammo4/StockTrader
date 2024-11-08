# FILE: `StockTrader/garch_vol3.py`
from config import * # this imports all necessary packages and functions referenced but not explicitly defined below



class BinomialOptions:
	# def __init__ (self, S, r, q, settlement=None):
	def __init__ (self, symbol, past_bars=365, option_expiry_days=30):
		self.symbol = symbol;

		self.day_count = ql.Actual365Fixed();
		self.calendar = ql.UnitedStates(ql.UnitedStates.NYSE);
		self.settlement = self.calendar.advance(ql.Date.todaysDate(), ql.Period(2,ql.Days));
		self.past_bars = past_bars;
		self.option_expiry_days = option_expiry_days;
		self.set_market_data(past_bars, option_expiry_days);

	def set_market_data (self, past_bars, option_expiry_days):
		expiry_str = options_data.get_closest_expiry(symbol=self.symbol, num_days=option_expiry_days);
		expiry_dt = datetime.strptime(expiry_str, "%Y-%m-%d");
		self.days_to_expiry = (expiry_dt - datetime.today()).days;
		self.S = quotes.get_quote_day(symbol=self.symbol, last_price=True);
		print(f'Price [{self.symbol}]: ${self.S}');
		print(f'Expiry String: {expiry_str}');
		print(f'Days to Expiry: {self.days_to_expiry}');
		print(97*'-');

		self.r = fred_rate();
		self.q = dividend_yield(self.symbol, self.S);
		print(f"Risk Free Rate: {self.r}");
		print(f"Dividend Yield: {self.q}");
		print(97*'-');

		self.options_chain = options_data.get_chain_day(symbol=self.symbol, expiry=expiry_str);
		self.options_chain.drop(
			labels = ['last', 'change', 'close', 'open', 'high', 'low', 'change_percentage', 'last_volume', 'trade_date', 'prevclose', 'bidexch', 'bid_date', 'askexch', 'ask_date'],
			axis = 1,
			inplace=True
		);
		self.options_chain['mp'] = np.round(.5*(self.options_chain['bid'] + self.options_chain['ask']),2);
		self.options_chain = self.options_chain[['symbol', 'bid', 'ask', 'mp', 'bidsize', 'asksize', 'open_interest', 'volume', 'option_type']]
		print(f"Options Chain:\n{self.options_chain}");
		print(97*'-');

		self.bars = quotes.get_historical_quotes(
			symbol = self.symbol,
			start_date = (datetime.today() - timedelta(weeks=int(past_bars/7)) - timedelta(days=1)).strftime('%Y-%m-%d'),
			end_date = datetime.today().strftime('%Y-%m-%d')
		);
		self.bars['log_return'] = np.log(self.bars['close']).diff();
		self.bars.dropna(inplace=True);
		print(f"Bar Data:\n{self.bars}");
		print(f'Num bars: {len(self.bars)}');
		print(97*'-');

		self.sigma_historical = estimate_historical_vol(self.bars);
		print(f'Historical Vol: {self.sigma_historical:.4f}');
		print(97*'-');







b = BinomialOptions(symbol='DD');