# FILE: `StockTrader/BinomialOptions.py`
from config import * # this imports all necessary packages and functions referenced but not explicitly defined below



class BinomialOptions:
	def __init__ (self, symbol, past_bars=252, option_expiry_days=30, option_expiry_date=None):

		#
		# Configure dates for historical price and desired option chain data.
		# Then, prepare historical price data and option_chain dataframes.
		#

		self.symbol = symbol;
		self.day_count = ql.Actual365Fixed();
		self.calendar = ql.UnitedStates(ql.UnitedStates.NYSE);
		self.settlement_date = self.calendar.advance(ql.Date.todaysDate(), ql.Period(2,ql.Days));
		self.past_bars = past_bars;
		self.option_expiry_days = option_expiry_days;
		self.set_market_data(past_bars, option_expiry_days);

		#
		# Define search bounds for implied volatility numerical procedure
		#

		self.sigma_lower = .0125;
		self.sigma_upper = 55.00;

	def set_market_data (self, past_bars, option_expiry_days):
		self.expiry_str = options_data.get_closest_expiry(symbol=self.symbol, num_days=option_expiry_days);
		expiry_dt = datetime.strptime(self.expiry_str, "%Y-%m-%d");
		self.days_to_expiry = (expiry_dt - datetime.today()).days;
		self.S = quotes.get_quote_day(symbol=self.symbol, last_price=True);
		print(f'Price [{self.symbol}]: ${self.S}');
		print(f'Expiry String: {self.expiry_str}');
		print(f'Days to Expiry: {self.days_to_expiry}');
		print(97*'-');

		self.r = fred_rate();
		self.q = dividend_yield(self.symbol, self.S);
		print(f"Risk Free Rate: {self.r}");
		print(f"Dividend Yield: {self.q}");
		print(97*'-');

		self.options_chain = options_data.get_chain_day(symbol=self.symbol, expiry=self.expiry_str);
		self.options_chain.drop(
			labels = ['last', 'change', 'open', 'high', 'low', 'change_percentage', 'last_volume', 'trade_date', 'prevclose', 'bidexch', 'bid_date', 'askexch', 'ask_date'],
			axis = 1,
			inplace=True
		);
		self.options_chain['mp'] = np.round(.5*(self.options_chain['bid'] + self.options_chain['ask']),2);
		self.options_chain = self.options_chain[['symbol', 'strike', 'bid', 'ask', 'mp', 'bidsize', 'asksize', 'open_interest', 'volume', 'option_type']]
		# self.options_chain.sort_values(by=['option_type', 'strike'], ascending=[True, True], inplace=True);
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

	def binomQL (self, strike_price, option_type, sigma0=None):
		expiry_date = self.calendar.advance(ql.Date.todaysDate(), ql.Period(self.option_expiry_days, ql.Days));

		self.spotH = ql.QuoteHandle(ql.SimpleQuote(self.S));
		self.risk_freeH = ql.YieldTermStructureHandle(ql.FlatForward(self.settlement_date, self.r, self.day_count));
		self.div_yieldH = ql.YieldTermStructureHandle(ql.FlatForward(self.settlement_date, self.q, self.day_count));
		self.volH = ql.BlackVolTermStructureHandle(
			ql.BlackConstantVol(
				self.settlement_date,
				self.calendar,
				self.sigma_historical if sigma0 is None else sigma0,
				self.day_count
			)
		);
		self.bsm_process = ql.BlackScholesMertonProcess(self.spotH, self.div_yieldH, self.risk_freeH, self.volH);
		self.payoff = ql.PlainVanillaPayoff(
			ql.Option.Call if option_type == 'call' else ql.Option.Put,
			strike_price
		);
		self.exercise = ql.AmericanExercise(self.settlement_date, expiry_date);
		self.option_contract = ql.VanillaOption(self.payoff, self.exercise);
		self.binom_engine = ql.BinomialVanillaEngine(self.bsm_process, 'crr', 100);
		self.option_contract.setPricingEngine(self.binom_engine);
		# print(f'Strike Price: {strike_price}');
		# print(f'Option Type: {option_type}');
		# print(f'Exercise: {self.exercise}');
		# print(f'Engine: {self.binom_engine}');
		# print(f'Contract: {self.option_contract}');
		# print(97*'-');

		npv = self.option_contract.NPV();
		delta = self.option_contract.delta();
		gamma = self.option_contract.gamma();
		theta = self.option_contract.theta();

		return npv, delta, gamma, theta

	def append_greeks_NPV(self):
		self.options_chain['NPV'] = np.nan;
		self.options_chain['Delta'] = np.nan;
		self.options_chain['Gamma'] = np.nan;
		self.options_chain['Theta'] = np.nan;

		for idx, row in self.options_chain.iterrows():
			npv, delta, gamma, theta = self.binomQL(strike_price=row['strike'], option_type=row['option_type']);
			sigma_iv = self.binomIV(strike_price=row['strike'], option_type=row['option_type'], broker_price=row['mp']);

			self.options_chain.at[idx, 'NPV'] = npv;
			self.options_chain.at[idx, 'Delta'] = delta;
			self.options_chain.at[idx, 'Gamma'] = gamma;
			self.options_chain.at[idx, 'Theta'] = theta;
			self.options_chain.at[idx, 'IV'] = sigma_iv;

		self.options_chain['NPV'] = np.round(self.options_chain['NPV'], 4);
		self.options_chain['Delta'] = np.round(self.options_chain['Delta'], 4);
		self.options_chain['Gamma'] = np.round(self.options_chain['Gamma'], 4);
		self.options_chain['Theta'] = np.round(self.options_chain['Theta'], 4);
		self.options_chain['IV'] = np.round(self.options_chain['IV'], 4);

		print(97*'-');
		pd.set_option('display.max_rows', None);
		print(f'Options Chain with NPV, Greeks:\n{self.options_chain}');
		pd.set_option('display.max_rows', 60);
		print(97*'-');

	def binomIV (self, strike_price, option_type, broker_price):
		def objectiveIV (vol):
			try:
				binom_npv = self.binomQL(strike_price=strike_price, option_type=option_type, sigma0=vol)[0];
				return binom_npv - broker_price;
			except RuntimeError:
				return 1e5;

		def get_good_endpoints ():
			go_again = False;
			lower, upper = self.sigma_lower, self.sigma_upper;
			max_iter = 3750;
			expansion_factor = 1.3725;
			contraction_factor = .8135;

			print(f"Seeking endpoints: Strike={strike_price}, Type={option_type}, Broker={broker_price}");
			for k in range(max_iter):
				f_lower = objectiveIV(lower);
				f_upper = objectiveIV(upper);

				if k % 613 == 0:
					expansion_factor = 1.8645;
					contraction_factor = .6975;




				# if f_lower*f_upper < 0 and abs(f_lower) < 1e4 and abs(f_upper) < 1e4:
				if f_lower*f_upper < 0:
					print(f'\tDifferent Signed Endpoints: ({lower}, {upper})');
					if abs(f_lower) > 1e4:
						lower *= .6
					if abs(f_upper) > 1e4:
						upper *= .6
					return lower, upper;

				if f_lower * f_upper > 0:
					if abs(f_lower) < abs(f_upper):
						lower /= expansion_factor;
					else:
						upper *= expansion_factor;
				else:
					if abs(f_lower) > 1e4:
						lower = .125*(lower+upper);
					if abs(f_upper) > 1e4:
						upper = .125*(lower+upper);


				if k % 85 == 0:
					upper += upper*random.random();
					lower += lower*random.random();

				if k % 52 == 0:
					upper -= upper*random.random();
					lower += lower*random.random();

				if k % (max_iter/2) == 0:
					upper = (upper % 250) + lower;
					lower = lower % 250;

				if lower >= upper:
					mid = .5*(lower+upper);
					spread = max(abs(upper-lower), 1e-5);
					lower = max(mid-spread, 1e-6);
					upper = min(mid+spread, 100);

				upper = (upper % 4) + lower;
				lower = lower % 3

				if k % 10 == 0:
					lower = lower * (1-contraction_factor) + upper*contraction_factor;
					upper = upper * (1-contraction_factor) + lower*contraction_factor;
			# print('.....');
			print(f'\tNo good endpoints. Returning [Iteration={k}, Current:({lower},{upper})]');
			print("\n");
			return lower, upper;


		lower_bound, upper_bound = get_good_endpoints();

		try:
			# iv = root_scalar(objectiveIV, method='toms748', bracket=[self.sigma_lower, self.sigma_upper], xtol=1e-6);
			iv = root_scalar(objectiveIV, method='toms748', bracket=[lower_bound,upper_bound], xtol=1e-6);
			if iv.converged:
				print(f'CONVERGED \t\t[{strike_price}, {option_type}] \t-> σ_iv = {iv.root:.4f}'); print('.....');
				return iv.root;
			else:
				print(f'FAILED [K={strike_price}, Type={option_type}, Broker={broker_price}]'); print('.....');
				return None;
		except ValueError as e:
			print(f'BAD ROOT [K={strike_price}, Type={option_type}, Broker={broker_price}]: \t{str(e)}');
			return None;

	def plot_greek (self, greek):
		if greek not in ['Delta', 'Gamma', 'Theta']:
			raise ValueError('JUNK GREEK. Valid: Delta, Gamma, Theta');


		# atm_bounds = [(1-.0025)*self.S, (1+.0025)*self.S];
		atm_lower_bound, atm_upper_bound = [(1-.0025)*self.S, np.ceil((1+.0025)*self.S)];

		call_options = self.options_chain.loc[self.options_chain['option_type'] == 'call'];
		put_options = self.options_chain.loc[self.options_chain['option_type'] == 'put'];

		call_itm = call_options.loc[call_options['strike'] < atm_lower_bound];
		call_atm = call_options.loc[call_options['strike'].between(left=atm_lower_bound, right=atm_upper_bound, inclusive='both')];
		call_otm = call_options.loc[call_options['strike'] > atm_upper_bound];

		put_otm = put_options.loc[put_options['strike'] < atm_lower_bound];
		put_atm = put_options.loc[put_options['strike'].between(left=atm_lower_bound, right=atm_upper_bound, inclusive='both')];
		put_itm = put_options.loc[put_options['strike'] > atm_upper_bound];

		plt.figure(figsize=(12, 8));

		plt.scatter(call_itm['strike'], call_itm[greek], color='green', marker='o', label='ITM Call');
		plt.scatter(call_atm['strike'], call_atm[greek], color='blue', marker='o', label='ATM Call');
		plt.scatter(call_otm['strike'], call_otm[greek], color='red', marker='o', label='OTM Call');

		plt.scatter(put_itm['strike'], put_itm[greek], color='green', marker='^', label='ITM Put');
		plt.scatter(put_atm['strike'], put_atm[greek], color='blue', marker='^', label='ATM Put');
		plt.scatter(put_otm['strike'], put_otm[greek], color='red', marker='^', label='OTM Put');

		plt.axvline(x=self.S, color='black', linestyle=':', label='Underlying');

		plt.xlabel('Strike');
		plt.ylabel(greek);
		plt.title(f'Strike vs. {greek} [{self.symbol}, S=${self.S}, T={self.expiry_str}]');
		plt.legend();
		plt.grid(True, linestyle=':', alpha=.50);

		plt.show();

	def plot_smileIV (self):
		call_options = self.options_chain.loc[self.options_chain['option_type'] == 'call'];
		put_options = self.options_chain.loc[self.options_chain['option_type'] == 'put'];

		plt.figure(figsize=(12,8));

		plt.scatter(call_options['strike'], call_options['IV'], color='blue', label='Calls', alpha=.70);
		plt.scatter(call_options['strike'], call_options['IV'], color='blue', linestyle='--', alpha=0.50);

		plt.scatter(put_options['strike'], put_options['IV'], color='red', label='Puts', alpha=.70);
		plt.scatter(put_options['strike'], put_options['IV'], color='red', label='Puts', alpha=0.50);

		plt.axvline(x=self.S, color='green', linestyle=':', label='Underlying');

		plt.xlabel('Strike');
		plt.ylabel('IV');
		plt.title(f'VOLATILITY SMILE [{self.symbol}, S=${self.S}, T={self.expiry_str}]');

		plt.legend();
		plt.grid(True, linestyle=':', alpha=0.50);
		plt.show();


	def plot_surfaceIV (self):
		self.expiries = options_data.get_expiry_dates(self.symbol);
		vol_surface = [];
		for expiry_str in self.expiries:
			self.set_market_data(
				past_bars = self.past_bars,
				option_expiry_days = (datetime.strptime(expiry_str, '%Y-%m-%d')-datetime.today()).days
			);
			self.append_greeks_NPV();
			self.options_chain['Expiry'] = expiry_str;
			self.options_chain['TTM'] = self.days_to_expiry / 365.0;
			self.options_chain.sort_values(by=['option_type', 'strike'], ascending=[True, True], inplace=True);

			print(f'APPENDING OPTIONS/VOLLY DATA FOR {expiry_str}\n{self.options_chain}');
			print(97*'-');
			vol_surface.append(self.options_chain);