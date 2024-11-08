# FILE: `StockTrader/BinomialOptions.py`

from config import * # this imports all necessary packages and functions referenced but not explicitly defined below
from OptionsIV import OptionsIV

class BinomialOptions:
	def __init__ (self, symbol, past_bars=365, option_expiry_days=30, option_expiry_date=None, itm_percent=.025, optionsIV_csv=None, optionsIV_verbose=False):

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
		self.itm_percent = itm_percent;
		self.add_moneyness();

		#
		# Define search bounds for implied volatility numerical procedure
		#

		self.sigma_lower = .0125;
		self.sigma_upper = 55.00;

		self.append_greeks_NPV();

		#
		# Use OptionsIV Class to Determine Each Option's Implied Volatility Percentile
		#

		self.optionsIV = None;
		self.optionsIV_csv = optionsIV_csv;
		self.optionsIV_verbose = optionsIV_verbose;

		if self.optionsIV_csv is not None:
			print(f'OptionsIV CSV FILE: {self.optionsIV_csv}');

			self.optionsIV = OptionsIV(csv_file=self.optionsIV_csv, symbol=self.symbol, verbose=self.optionsIV_verbose);
			self.binomIVPercentile();

	def set_itm_options (self):
		self.near_itm = self.options_chain.loc[self.options_chain['strike'].between(left=1-self.itm_percent, right=1+self.itm_percent, inclusive='both')]

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
		self.options_chain['mp'] = np.round(.5*(self.options_chain['bid'] + self.options_chain['ask']),2);
		cols_to_keep = ['symbol', 'strike', 'bid', 'ask', 'mp', 'bidsize', 'asksize', 'open_interest', 'volume', 'option_type'];
		cols_to_keep = [col for col in cols_to_keep if col in self.options_chain.columns];
		self.options_chain = self.options_chain[cols_to_keep];
		# self.options_chain = self.options_chain[['symbol', 'strike', 'bid', 'ask', 'mp', 'bidsize', 'asksize', 'open_interest', 'volume', 'option_type']]
		self.options_chain['days_to_expiry'] = self.days_to_expiry;
		print(f"Options Chain:\n{self.options_chain}");
		print(97*'-');

		self.bars = quotes.get_historical_quotes(
			symbol = self.symbol,
			start_date = (datetime.today() - timedelta(weeks=int(past_bars/7)) - timedelta(days=1) - timedelta(days=option_expiry_days)).strftime('%Y-%m-%d'),
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

		self.bars['vol_historical'] = self.bars['log_return'].rolling(window=self.days_to_expiry).std()*np.sqrt(self.past_bars);
		self.bars.dropna(inplace=True);
		print(f"Bar Data:\n{self.bars}");
		print(f"Num bars: {len(self.bars)}");
		print(97*'-');

		self.bars['date'] = pd.to_datetime(self.bars['date']);

	def add_moneyness (self):
		self.options_chain['Moneyness'] = self.options_chain['strike'] / self.S;

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

		self.options_chain['IV'].fillna(-100, inplace=True)

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
			print(f'\tNo good endpoints. Returning [Iteration={k}, Current:({lower},{upper})]');
			print("\n");
			return lower, upper;

		lower_bound, upper_bound = get_good_endpoints();

		try:
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

	def binomIVPercentile (self):
		self.options_chain['IVP'] = np.nan;

		for idx, row in self.options_chain.iterrows():
			self.options_chain.at[idx, 'IVP'] = self.optionsIV.iv_percentile(row);

		self.options_chain.fillna(-69, inplace=True);


	def plot_greek (self, greek):
		if greek not in ['Delta', 'Gamma', 'Theta']:
			raise ValueError('JUNK GREEK. Valid: Delta, Gamma, Theta');

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



if __name__ == '__main__':
	the_date_today = datetime.today().strftime('%Y-%m-%d');

	xlfBO = BinomialOptions(symbol='XLF', past_bars=365, option_expiry_days=30, itm_percent=.075, optionsIV_csv='./options/xlf_options.csv', optionsIV_verbose=False);

	print(f"CURRENT DATA [{the_date_today}]: ACTIVELY TRADED CONTRACTS FOR $XLF\n{xlfBO.options_chain}");
	print(50*'-');
	print(f"HISTORICAL DATA [≈ 1 YEAR]: PREVIOUSLY TRADED CONTRACTS FOR $XLF\n{xlfBO.optionsIV.df}");
	print("\nDone.");



#
# OUTPUT SAMPLE - Current/Historical Options Contracts Data for $XLF
#


# CURRENT DATA [2024-09-12]: ACTIVELY TRADED CONTRACTS FOR $XLF
#                 symbol  strike    bid    ask     mp  bidsize  asksize  open_interest  volume option_type  days_to_expiry  Moneyness      NPV   Delta   Gamma   Theta        IV        IVP
# 0   XLF241011P00025000    25.0   0.00   0.27   0.14        0      326              0       0         put              28   0.563317   0.0000  0.0000  0.0000  0.0000    0.9865 -69.000000
# 1   XLF241011C00025000    25.0  17.25  21.45  19.35       16       16              0       0        call              28   0.563317  19.4427  0.9985  0.0000 -0.6058 -100.0000 -69.000000
# 2   XLF241011P00030000    30.0   0.00   0.28   0.14        0      328              0       0         put              28   0.675980   0.0000 -0.0000  0.0000 -0.0000    0.7144   0.655914
# 3   XLF241011C00030000    30.0  12.30  16.50  14.40       16       16              0       0        call              28   0.675980  14.4687  0.9985  0.0000 -0.8570   54.6445   1.000000
# 4   XLF241011C00033000    33.0   9.40  13.50  11.45       20       17              0       0        call              28   0.743578  11.4842  0.9985  0.0000 -1.0077 -100.0000   0.000000
# ..                 ...     ...    ...    ...    ...      ...      ...            ...     ...         ...             ...        ...      ...     ...     ...     ...       ...        ...
# 57  XLF241011C00049000    49.0   0.00   0.57   0.28        0      381              0       0        call              28   1.104101   0.0072  0.0113  0.0160 -0.2833    0.2715   0.787810
# 58  XLF241011C00049500    49.5   0.00   0.64   0.32        0      396              0       0        call              28   1.115367   0.0034  0.0058  0.0089 -0.1560    0.3033   0.784574
# 59  XLF241011P00049500    49.5   3.00   7.50   5.25       20       40              0       0         put              28   1.115367   5.1200 -1.0000  0.0000  1.8486    0.2799   0.311037
# 60  XLF241011P00050000    50.0   3.25   7.80   5.52       17       30              0       0         put              28   1.126634   5.6200 -1.0000  0.0000  1.8738 -100.0000   0.000000
# 61  XLF241011C00050000    50.0   0.00   0.43   0.22        0      352              1       0        call              28   1.126634   0.0015  0.0028  0.0046 -0.0805    0.2887   0.699717
#
# [62 rows x 18 columns]
# --------------------------------------------------
# HISTORICAL DATA [≈ 1 YEAR]: PREVIOUSLY TRADED CONTRACTS FOR $XLF
#              date expiration  ttm  midprice  strike call_put      close  log_return  div_yield  fred_rate  vol_historical      NPV        IV  moneyness              OCC
# 2545   2023-09-06 2023-10-06   30     0.005    24.0      Put  34.320000   -0.000583   0.018974     0.0534        0.032767   0.0000  0.477214     0.6993  XLF231006P24000
# 2641   2023-09-06 2023-10-06   30     0.155    25.0      Put  34.320000   -0.000583   0.018974     0.0534        0.032767   0.0000  0.717436     0.7284  XLF231006P25000
# 2929   2023-09-06 2023-10-06   30     0.015    28.0      Put  34.320000   -0.000583   0.018974     0.0534        0.032767   0.0000  0.330086     0.8159  XLF231006P28000
# 3025   2023-09-06 2023-10-06   30     0.020    29.0      Put  34.320000   -0.000583   0.018974     0.0534        0.032767   0.0000  0.292508     0.8450  XLF231006P29000
# 3121   2023-09-06 2023-10-06   30     0.035    30.0      Put  34.320000   -0.000583   0.018974     0.0534        0.032767   0.0000  0.266267     0.8741  XLF231006P30000
# ...           ...        ...  ...       ...     ...      ...        ...         ...        ...        ...             ...      ...       ...        ...              ...
# 408287 2024-06-21 2024-08-16   56    10.050    51.0      Put  41.167204   -0.007811   0.015818     0.0505        0.020860   9.8328  0.430475     1.2389  XLF240816P51000
# 408298 2024-06-21 2024-08-16   56     0.140    52.0     Call  41.167204   -0.007811   0.015818     0.0520        0.020860   0.0000  0.357623     1.2631  XLF240816C52000
# 408299 2024-06-21 2024-08-16   56     0.140    52.0     Call  41.167204   -0.007811   0.015818     0.0505        0.020860   0.0000  0.357931     1.2631  XLF240816C52000
# 408358 2024-06-21 2024-08-16   56    13.275    54.0      Put  41.167204   -0.007811   0.015818     0.0520        0.020860  12.8328  0.586893     1.3117  XLF240816P54000
# 408359 2024-06-21 2024-08-16   56    13.275    54.0      Put  41.167204   -0.007811   0.015818     0.0505        0.020860  12.8328  0.585093     1.3117  XLF240816P54000
#
# [11934 rows x 15 columns]
#
# Done.













