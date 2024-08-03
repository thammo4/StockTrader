# FILE: `StockTrader/HestonOptions.py`
from config import * # this imports all necessary packages and functions referenced but not explicitly defined below

class HestonOptions:
	# def __init__ (self, symbol, past_bars=252, option_expiry_days=30, option_expiry_date=None):
	def __init__ (
		self,
		symbol,
		past_bars=252, option_expiry_days=30, option_expiry_date=None,
		vol_initial=.175, kappa=1.0, theta=.175, vol_of_vol=.175, rho=-.50
	):

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
		# Heston Model Parameters
		#

		self.vol_initial = vol_initial;
		self.kappa = kappa;
		self.theta = theta;
		self.vol_of_vol = vol_of_vol;
		self.rho = rho;

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


	def hestonQL (self, strike_price, option_type):
		expiry_date = self.calendar.advance(ql.Date.todaysDate(), ql.Period(self.option_expiry_days, ql.Days));

		self.spotH = ql.QuoteHandle(ql.SimpleQuote(self.S));
		self.risk_freeTS = ql.YieldTermStructureHandle(ql.FlatForward(self.settlement_date, self.r, self.day_count));
		self.div_yieldTS = ql.YieldTermStructureHandle(ql.FlatForward(self.settlement_date, self.q, self.day_count));
		self.heston_process = ql.HestonProcess(self.risk_freeTS, self.div_yieldTS, self.spotH, self.vol_initial, self.kappa, self.theta, self.vol_of_vol, self.rho);
		self.exercise = ql.AmericanExercise(self.settlement_date, expiry_date);
		self.payoff = ql.PlainVanillaPayoff(
			ql.Option.Call if option_type == 'call' else ql.Option.Put,
			strike_price
		);
		self.option_contract = ql.VanillaOption(self.payoff, self.exercise);
		self.heston_engine = ql.FdHestonVanillaEngine(ql.HestonModel(self.heston_process), 100, 100, 50);
		self.option_contract.setPricingEngine(self.heston_engine);

		npv = self.option_contract.NPV();
		delta = self.option_contract.delta();
		gamma = self.option_contract.gamma();
		theta = self.option_contract.theta();

		return npv, delta, gamma, theta;

		print(f'Spot: {self.S}');
		print(f'Risk Free: {self.r}');
		print(f'Div Yield: {self.q}');
		print(f'NPV: {npv}');

	def append_greeks_NPV(self):
		self.options_chain['NPV'] = np.nan;
		self.options_chain['Delta'] = np.nan;
		self.options_chain['Gamma'] = np.nan;
		self.options_chain['Theta'] = np.nan;

		for idx, row in self.options_chain.iterrows():
			npv, delta, gamma, theta = self.hestonQL(strike_price=row['strike'], option_type=row['option_type']);
			sigma_iv = self.hestonIV(strike_price=row['strike'], option_type=row['option_type'], broker_price=row['mp']);

			self.options_chain.at[idx, 'NPV'] = npv;
			self.options_chain.at[idx, 'Delta'] = delta;
			self.options_chain.at[idx, 'Gamma'] = gamma;
			self.options_chain.at[idx, 'Theta'] = theta;
			self.options_chain.at[idx, 'IV'] = sigma_iv;

		self.options_chain['NPV'] = np.round(self.options_chain['NPV'], 4);
		self.options_chain['Delta'] = np.round(self.options_chain['Delta'], 4);
		self.options_chain['Gamma'] = np.round(self.options_chain['Gamma'], 4);
		self.options_chain['Theta'] = np.round(self.options_chain['Theta'], 4);

		print(97*'-');
		pd.set_option('display.max_rows', None);
		print(f'OPTIONS CHAIN WITH NPV, GREEKS:\n{self.options_chain}');
		pd.set_option('display.max_rows', 60);
		print(97*'-');

	# def binomIV (self, strike_price, option_type, broker_price):
	def hestonIV (self, strike_price, option_type, broker_price):
		def objectiveIV (vol):
			try:
				heston_npv = self.hestonQL(strike_price=strike_price, option_type=option_type)[0];
				return heston_npv - broker_price;
			except RuntimeError:
				return 1e5;

		def get_good_endpoints ():
			go_again = False;
			lower, upper = self.sigma_lower, self.sigma_upper;
			max_iter = 3750;
			expansion_factor = 1.1725;
			contraction_factor = .9135;

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
				if k % 5 == 0:
					print(f'k = {k}');
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



#
# EXAMPLE - Church & Dwight Company Inc. [CHD]
#


# Price [CHD]: $98.53
# Expiry String: 2024-09-20
# Days to Expiry: 47
# -------------------------------------------------------------------------------------------------
# Risk Free Rate: 0.052000000000000005
# Dividend Yield: 0.0115
# -------------------------------------------------------------------------------------------------
# Options Chain:
#                 symbol  strike    bid    ask     mp  bidsize  asksize  open_interest  volume option_type
# 0   CHD240920P00055000    55.0   0.00   2.15   1.08        0       27              0       0         put
# 1   CHD240920C00055000    55.0  41.90  45.50  43.70       10       10              0       0        call
# 2   CHD240920C00060000    60.0  36.50  40.10  38.30       10       10              0       0        call
# 3   CHD240920P00060000    60.0   0.00   2.15   1.08        0       27              0       0         put
# 4   CHD240920P00065000    65.0   0.00   1.60   0.80        0       64              0       0         put
# 5   CHD240920C00065000    65.0  31.80  35.80  33.80       10       10              0       0        call
# 6   CHD240920P00070000    70.0   0.00   0.95   0.48        0       16              0       0         put
# 7   CHD240920C00070000    70.0  26.90  30.70  28.80       10       10              0       0        call
# 8   CHD240920P00075000    75.0   0.00   0.95   0.48        0       14              0       0         put
# 9   CHD240920C00075000    75.0  22.00  25.90  23.95       11        3              1       0        call
# 10  CHD240920P00080000    80.0   0.00   0.95   0.48        0       15              0       0         put
# 11  CHD240920C00080000    80.0  17.00  21.00  19.00        6       11              0       0        call
# 12  CHD240920P00085000    85.0   0.00   0.35   0.18        0       11              0       0         put
# 13  CHD240920C00085000    85.0  12.00  16.10  14.05       10       10              0       0        call
# 14  CHD240920P00090000    90.0   0.45   0.80   0.62       51       20              2       6         put
# 15  CHD240920C00090000    90.0   9.10   9.90   9.50       26       24              0       0        call
# 16  CHD240920P00095000    95.0   1.35   1.60   1.48       10       20             97      13         put
# 17  CHD240920C00095000    95.0   4.20   5.60   4.90       94        5              0       1        call
# 18  CHD240920P00100000   100.0   3.40   3.80   3.60       13       20             27       0         put
# 19  CHD240920C00100000   100.0   2.30   2.60   2.45       16       20             34      39        call
# 20  CHD240920P00105000   105.0   6.60   9.20   7.90       23       22              0       0         put
# 21  CHD240920C00105000   105.0   0.65   1.40   1.02       30      243             51       0        call
# 22  CHD240920P00110000   110.0   9.80  13.40  11.60       11       22              0       0         put
# 23  CHD240920C00110000   110.0   0.15   0.50   0.32       27       16              5       4        call
# 24  CHD240920C00115000   115.0   0.15   0.75   0.45        0       22              0       0        call
# 25  CHD240920P00115000   115.0  14.90  18.40  16.65       11        9              0       0         put
# 26  CHD240920P00120000   120.0  19.60  23.40  21.50        6       10              0       0         put
# 27  CHD240920C00120000   120.0   0.00   0.75   0.38        0       42              0       0        call
# 28  CHD240920P00125000   125.0  24.40  28.30  26.35       10       10              0       0         put
# 29  CHD240920C00125000   125.0   0.00   0.75   0.38        0       31              0       0        call
# 30  CHD240920P00130000   130.0  29.80  33.30  31.55       10       10              0       0         put
# 31  CHD240920C00130000   130.0   0.00   0.75   0.38        0       31              0       0        call
# 32  CHD240920P00135000   135.0  34.40  38.60  36.50        1       10              0       0         put
# 33  CHD240920C00135000   135.0   0.00   2.15   1.08        0       25              0       0        call
# 34  CHD240920P00140000   140.0  39.40  43.60  41.50        1       10              0       0         put
# 35  CHD240920C00140000   140.0   0.00   1.70   0.85        0       16              0       0        call
# 36  CHD240920P00145000   145.0  44.70  47.80  46.25        1       10              0       0         put
# 37  CHD240920C00145000   145.0   0.00   1.50   0.75        0       16              0       0        call
# 38  CHD240920P00150000   150.0  49.60  53.20  51.40       10       10              0       0         put
# 39  CHD240920C00150000   150.0   0.00   1.50   0.75        0       16              0       0        call
# 40  CHD240920P00155000   155.0  54.40  58.30  56.35        1        1              0       0         put
# 41  CHD240920C00155000   155.0   0.00   1.50   0.75        0       16              0       0        call
# -------------------------------------------------------------------------------------------------
# Bar Data:
#            date    open     high     low   close   volume  log_return
# 1    2023-11-27   94.47   95.275   94.37   95.25  1231770    0.004419
# 2    2023-11-28   95.48   95.900   94.88   95.10  1837614   -0.001576
# 3    2023-11-29   95.20   95.450   94.50   94.66  1602275   -0.004637
# 4    2023-11-30   94.48   96.680   93.69   96.63  3966414    0.020598
# 5    2023-12-01   96.54   96.880   95.07   95.37  1490548   -0.013125
# ..          ...     ...      ...     ...     ...      ...         ...
# 168  2024-07-29  101.30  102.260  100.77  101.59  1349752    0.002069
# 169  2024-07-30   98.36   99.620   97.65   99.43  2005409   -0.021491
# 170  2024-07-31   99.48   99.550   97.66   98.01  3040123   -0.014384
# 171  2024-08-01   98.37  100.240   97.88  100.01  2346373    0.020201
# 172  2024-08-02   96.94   99.930   96.35   98.53  3855940   -0.014909

# [172 rows x 7 columns]
# Num bars: 172
# -------------------------------------------------------------------------------------------------
# Historical Vol: 0.1625
# -------------------------------------------------------------------------------------------------
# OPTIONS CHAIN WITH NPV, GREEKS:
#                 symbol  strike    bid    ask     mp  bidsize  asksize  open_interest  volume option_type      NPV   Delta   Gamma    Theta
# 0   CHD240920P00055000    55.0   0.00   2.15   1.08        0       27              0       0         put   0.0001 -0.0000  0.0000  -0.0091
# 1   CHD240920C00055000    55.0  41.90  45.50  43.70       10       10              0       0        call  43.7542  0.9996  0.0000  -1.7320
# 2   CHD240920C00060000    60.0  36.50  40.10  38.30       10       10              0       0        call  38.7810  0.9993  0.0001  -2.0476
# 3   CHD240920P00060000    60.0   0.00   2.15   1.08        0       27              0       0         put   0.0012 -0.0003  0.0001  -0.0663
# 4   CHD240920P00065000    65.0   0.00   1.60   0.80        0       64              0       0         put   0.0076 -0.0016  0.0003  -0.3334
# 5   CHD240920C00065000    65.0  31.80  35.80  33.80       10       10              0       0        call  33.8128  0.9979  0.0003  -2.5727
# 6   CHD240920P00070000    70.0   0.00   0.95   0.48        0       16              0       0         put   0.0359 -0.0069  0.0013  -1.2144
# 7   CHD240920C00070000    70.0  26.90  30.70  28.80       10       10              0       0        call  28.8664  0.9926  0.0013  -3.7100
# 8   CHD240920P00075000    75.0   0.00   0.95   0.48        0       14              0       0         put   0.1317 -0.0224  0.0036  -3.3395
# 9   CHD240920C00075000    75.0  22.00  25.90  23.95       11        3              1       0        call  23.9875  0.9771  0.0036  -6.0868
# 10  CHD240920P00080000    80.0   0.00   0.95   0.48        0       15              0       0         put   0.3882 -0.0581  0.0080  -7.1863
# 11  CHD240920C00080000    80.0  17.00  21.00  19.00        6       11              0       0        call  19.2689  0.9415  0.0080 -10.1746
# 12  CHD240920P00085000    85.0   0.00   0.35   0.18        0       11              0       0         put   0.9513 -0.1239  0.0144 -12.4721
# 13  CHD240920C00085000    85.0  12.00  16.10  14.05       10       10              0       0        call  14.8559  0.8759  0.0143 -15.6821
# 14  CHD240920P00090000    90.0   0.45   0.80   0.62       51       20              2       6         put   1.9951 -0.2245  0.0214 -17.9029
# 15  CHD240920C00090000    90.0   9.10   9.90   9.50       26       24              0       0        call  10.9213  0.7759  0.0212 -21.3070
# 16  CHD240920P00095000    95.0   1.35   1.60   1.48       10       20             97      13         put   3.6728 -0.3542  0.0270 -21.6962
# 17  CHD240920C00095000    95.0   4.20   5.60   4.90       94        5              0       1        call   7.6160  0.6474  0.0267 -25.2621
# 18  CHD240920P00100000   100.0   3.40   3.80   3.60       13       20             27       0         put   6.0678 -0.4983  0.0295 -22.5560
# 19  CHD240920C00100000   100.0   2.30   2.60   2.45       16       20             34      39        call   5.0205  0.5050  0.0290 -26.2535
# 20  CHD240920P00105000   105.0   6.60   9.20   7.90       23       22              0       0         put   9.1719 -0.6389  0.0284 -20.3343
# 21  CHD240920C00105000   105.0   0.65   1.40   1.02       30      243             51       0        call   3.1233  0.3671  0.0277 -24.1406
# 22  CHD240920P00110000   110.0   9.80  13.40  11.60       11       22              0       0         put  12.8980 -0.7611  0.0245 -15.9466
# 23  CHD240920C00110000   110.0   0.15   0.50   0.32       27       16              5       4        call   1.8333  0.2485  0.0236 -19.8482
# 24  CHD240920C00115000   115.0   0.15   0.75   0.45        0       22              0       0        call   1.0165  0.1569  0.0181 -14.7426
# 25  CHD240920P00115000   115.0  14.90  18.40  16.65       11        9              0       0         put  17.1156 -0.8573  0.0193 -10.7515
# 26  CHD240920P00120000   120.0  19.60  23.40  21.50        6       10              0       0         put  21.6888 -0.9272  0.0141  -5.9000
# 27  CHD240920C00120000   120.0   0.00   0.75   0.38        0       42              0       0        call   0.5336  0.0926  0.0126  -9.9880
# 28  CHD240920P00125000   125.0  24.40  28.30  26.35       10       10              0       0         put  26.5022 -0.9747  0.0090  -2.0809
# 29  CHD240920C00125000   125.0   0.00   0.75   0.38        0       31              0       0        call   0.2658  0.0514  0.0081  -6.2271
# 30  CHD240920P00130000   130.0  29.80  33.30  31.55       10       10              0       0         put  31.4698 -1.0003  0.0004   0.0700
# 31  CHD240920C00130000   130.0   0.00   0.75   0.38        0       31              0       0        call   0.1261  0.0269  0.0048  -3.6017
# 32  CHD240920P00135000   135.0  34.40  38.60  36.50        1       10              0       0         put  36.4700 -1.0000 -0.0001  -0.0018
# 33  CHD240920C00135000   135.0   0.00   2.15   1.08        0       25              0       0        call   0.0572  0.0133  0.0027  -1.9471
# 34  CHD240920P00140000   140.0  39.40  43.60  41.50        1       10              0       0         put  41.4700 -1.0000 -0.0000  -0.0003
# 35  CHD240920C00140000   140.0   0.00   1.70   0.85        0       16              0       0        call   0.0248  0.0063  0.0014  -0.9906
# 36  CHD240920P00145000   145.0  44.70  47.80  46.25        1       10              0       0         put  46.4700 -1.0000 -0.0000   0.0000
# 37  CHD240920C00145000   145.0   0.00   1.50   0.75        0       16              0       0        call   0.0104  0.0028  0.0007  -0.4772
# 38  CHD240920P00150000   150.0  49.60  53.20  51.40       10       10              0       0         put  51.4700 -1.0000 -0.0000   0.0000
# 39  CHD240920C00150000   150.0   0.00   1.50   0.75        0       16              0       0        call   0.0042  0.0012  0.0003  -0.2189
# 40  CHD240920P00155000   155.0  54.40  58.30  56.35        1        1              0       0         put  56.4700 -1.0000 -0.0000   0.0000
# 41  CHD240920C00155000   155.0   0.00   1.50   0.75        0       16              0       0        call   0.0016  0.0005  0.0001  -0.0961
# -------------------------------------------------------------------------------------------------

chd = HestonOptions('CHD');
chd.append_greeks_NPV();

















