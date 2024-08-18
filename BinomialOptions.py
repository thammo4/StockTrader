# FILE: `StockTrader/BinomialOptions.py`
from config import * # this imports all necessary packages and functions referenced but not explicitly defined below

class BinomialOptions:
	def __init__ (self, symbol, past_bars=252, option_expiry_days=30, option_expiry_date=None, itm_percent=.025):

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

		self.append_greeks_NPV();

		# self.near_itm = self.options_chain.loc[self.options_chain['strike'].between(left=.975*self.S,right=1.025*self.S,inclusive='both')];
		self.near_itm = self.options_chain.loc[self.options_chain['strike'].between(left=(1-itm_percent)*self.S, right=(1+itm_percent)*self.S, inclusive='both')];

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

		self.bars['vol_historical'] = self.bars['log_return'].rolling(window=self.days_to_expiry).std()*np.sqrt(252);
		self.bars.dropna(inplace=True);
		print(f"Bar Data:\n{self.bars}");
		print(f"Num bars: {len(self.bars)}");
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








#
# Volatility Modeling Functions - Volatility Risk Premium Strategy
#


#
# Apply Continuous Wavelet Transform to Historical Rolling Volatility Data to determine mean, standard dev of 'signal' Power
# • Default mother wavelet = Morlet
#

def cwt_vol (bar_data, wavelet_type='morl'):
	scales = np.arange(1,128);
	coefs, freqs = pywt.cwt(bar_data['vol_historical'], scales, wavelet_type);

	wavelet_features = pd.DataFrame({
		'cwt_power_mean': np.mean(np.abs(coefs), axis=0),
		'cwt_power_std': np.std(np.abs(coefs), axis=0)
	}, index=bar_data.index);
	bar_data = pd.concat([bar_data, wavelet_features], axis=1);
	return bar_data;

#
# Categorize Magnitude of Historical Volatility's CWT Time-Frequency Domain
# >>> eix.bars['vol_category'] = eix.bars.apply(lambda row: vol_category(eix.bars.loc[:row.name]),axis=1)
#

def vol_category (bar_data):
	recent_power = bar_data['cwt_power_mean'].iloc[-1];
	mean_power = bar_data['cwt_power_mean'].mean();
	std_power = bar_data['cwt_power_mean'].std();

	if recent_power > (mean_power + std_power):
		return "High";
	elif recent_power < (mean_power - std_power):
		return "Low";
	else:
		return "Medium";



#
# Fit GARCH(p,q) Model to Historical Rolling Volatility Data to Compute Conditional Expected Future Volatility
# • Conditioned on historical rolling volatility
#

def garch_vol_forecast (bar_data, P=1, Q=1, forecast_horizon=30):
	garch_model = arch_model(bar_data['vol_historical'], vol='Garch', p=P, q=Q);
	garch_fit = garch_model.fit(disp='off');
	forecast = garch_fit.forecast(horizon=forecast_horizon, start=bar_data.index[0]);
	return np.sqrt(forecast.variance.iloc[-1]);


#
# Compute Volatility Risk Preumium (VRP) of Options Contracts
# VRP = IV - GARCH_Volatility_Forecast
#

def VRP(options_chain, vol_garch_forecast):
	options_chain['VRP'] = options_chain['IV'] - vol_garch_forecast;
	return options_chain;








#
# EXAMPLE - EDISION INTERNATIONAL (EIX)
#

eix = BinomialOptions(symbol='EIX', past_bars=365);
eix.append_greeks_NPV();


# >>> eix.bars
#            date   open    high    low  close   volume  log_return  vol_historical
# 46   2023-09-11  70.00  70.590  69.75  69.87  1170202    0.001002        0.179245
# 47   2023-09-12  70.01  70.500  69.72  70.29  1725911    0.005993        0.178614
# 48   2023-09-13  70.42  71.080  70.18  70.74  2195487    0.006382        0.177971
# 49   2023-09-14  71.48  71.815  71.15  71.52  1542855    0.010966        0.176155
# 50   2023-09-15  71.21  72.120  71.16  71.56  2876658    0.000559        0.172567
# ..          ...    ...     ...    ...    ...      ...         ...             ...
# 267  2024-07-29  78.63  79.010  77.62  78.20  2030789   -0.003956        0.189140
# 268  2024-07-30  78.10  79.500  78.05  79.38  1922362    0.014977        0.187980
# 269  2024-07-31  79.75  80.520  79.06  80.01  4308250    0.007905        0.183351
# 270  2024-08-01  80.50  81.880  80.18  81.61  1834128    0.019800        0.188144
# 271  2024-08-02  82.58  83.050  80.74  82.27  2263934    0.008055        0.187742

# [226 rows x 8 columns]
# >>> eix.options_chain
#                 symbol  strike    bid    ask     mp  bidsize  asksize  open_interest  volume option_type      NPV   Delta   Gamma    Theta      IV
# 0   EIX240920C00047500    47.5  33.00  36.90  34.95       10       10              0       0        call  34.7700  1.0000  0.0000   0.6480  0.8722
# 1   EIX240920P00047500    47.5   0.00   0.70   0.35        0       37              0       0         put   0.0000 -0.0000  0.0000  -0.0000  0.9588
# 2   EIX240920P00050000    50.0   0.00   0.75   0.38        0       37              0       0         put   0.0000 -0.0000  0.0000  -0.0000  0.8969
# 3   EIX240920C00050000    50.0  31.30  34.40  32.85        1       10              0       0        call  32.2700  1.0000  0.0000   0.5180  0.9882
# 4   EIX240920P00055000    55.0   0.05   0.25   0.15       11      205              0       0         put   0.0000 -0.0000  0.0000  -0.0000  0.6304
# 5   EIX240920C00055000    55.0  25.50  29.50  27.50       10       10              0       0        call  27.2700  1.0000  0.0000   0.2580  0.6832
# 6   EIX240920P00060000    60.0   0.00   0.90   0.45        0       91              2       0         put   0.0000 -0.0000  0.0000  -0.0001  0.6367
# 7   EIX240920C00060000    60.0  20.50  24.70  22.60        1        2              1       1        call  22.2751  0.9975  0.0005  -0.0665  0.5924
# 8   EIX240920P00065000    65.0   0.00   1.40   0.70        0        1              0       0         put   0.0003 -0.0002  0.0001  -0.0185  0.5673
# 9   EIX240920C00065000    65.0  15.80  18.40  17.10       14       17              0       0        call  17.2997  0.9958  0.0002  -0.2847     NaN
# 10  EIX240920P00067500    67.5   0.05   0.75   0.40       14       46              2       0         put   0.0025 -0.0015  0.0009  -0.1190  0.4292
# 11  EIX240920C00067500    67.5  13.20  17.30  15.25        2        1              1       0        call  14.8162  0.9944  0.0009  -0.5067  0.4376
# 12  EIX240920P00070000    70.0   0.15   0.25   0.20       45       83              8       1         put   0.0143 -0.0075  0.0037  -0.5082  0.3115
# 13  EIX240920C00070000    70.0  11.10  14.90  13.00       10       10              1       0        call  12.3424  0.9883  0.0037  -1.0216  0.4241
# 14  EIX240920C00072500    72.5  10.00  10.90  10.45       10       23              0       2        call   9.9045  0.9680  0.0114  -2.2083  0.3457
# 15  EIX240920P00072500    72.5   0.25   0.35   0.30       39      117              8       0         put   0.0619 -0.0279  0.0114  -1.5693  0.2831
# 16  EIX240920P00075000    75.0   0.40   0.55   0.48       39       94             32       8         put   0.2068 -0.0788  0.0262  -3.5863  0.2585
# 17  EIX240920C00075000    75.0   8.20   9.60   8.90       30       33              9       0        call   7.5635  0.9173  0.0261  -4.3448  0.4056
# 18  EIX240920C00077500    77.5   4.50   6.30   5.40      134       31             37       2        call   5.4227  0.8206  0.0459  -7.1360  0.2007
# 19  EIX240920P00077500    77.5   0.75   0.90   0.82       31       93             23       1         put   0.5526 -0.1759  0.0461  -6.2660  0.2394
# 20  EIX240920P00080000    80.0   1.35   1.45   1.40       28        4             70       2         put   1.2122 -0.3202  0.0642  -8.6085  0.2226
# 21  EIX240920C00080000    80.0   4.20   4.40   4.30        7       33             24       2        call   3.5939  0.6772  0.0637  -9.5748  0.2732
# 22  EIX240920P00082500    82.5   2.25   2.45   2.35       21       12              2       1         put   2.2982 -0.4945  0.0717  -9.4090  0.2088
# 23  EIX240920C00082500    82.5   2.60   2.80   2.70       12       12            255       2        call   2.1881  0.5045  0.0709 -10.4606  0.2508
# 24  EIX240920P00085000    85.0   3.60   3.90   3.75       33       12              0       0         put   3.8178 -0.6656  0.0661  -8.3361  0.1972
# 25  EIX240920C00085000    85.0   1.50   1.65   1.58       16       27            152      42        call   1.2115  0.3359  0.0648  -9.4586  0.2411
# 26  EIX240920C00087500    87.5   0.65   1.00   0.82       24       20             10       7        call   0.6059  0.1985  0.0495  -7.1781  0.2316
# 27  EIX240920P00087500    87.5   4.20   6.40   5.30       31       11              0       0         put   5.7159 -0.8065  0.0513  -5.9942  0.1342
# 28  EIX240920P00090000    90.0   6.60   8.20   7.40       44       22              0       0         put   7.8966 -0.9058  0.0344  -3.3858     NaN
# 29  EIX240920C00090000    90.0   0.35   0.45   0.40       35       28              0       9        call   0.2730  0.1039  0.0321  -4.6250  0.2279
# 30  EIX240920P00095000    95.0  11.20  14.90  13.05       11       11              0       0         put  12.7300 -0.9997  0.0037   1.3017  0.3282
# 31  EIX240920C00095000    95.0   0.00   0.75   0.38        0       29              0       0        call   0.0406  0.0199  0.0086  -1.2283  0.3150
# 32  EIX240920P00100000   100.0  16.20  19.80  18.00       17       15              0       0         put  17.7300 -1.0000  0.0000   2.0820  0.4007
# 33  EIX240920C00100000   100.0   0.00   0.75   0.38        0       26              0       0        call   0.0040  0.0024  0.0013  -0.1892  0.3947
# 34  EIX240920P00105000   105.0  21.10  24.80  22.95       13       12              0       0         put  22.7300 -1.0000  0.0000   2.3420  0.4625
# 35  EIX240920C00105000   105.0   0.00   0.75   0.38        0       40              0       0        call   0.0003  0.0002  0.0001  -0.0180  0.4678
# 36  EIX240920P00110000   110.0  26.00  30.00  28.00       10        7              0       0         put  27.7300 -1.0000  0.0000   2.6020  0.5480
# 37  EIX240920C00110000   110.0   0.00   2.15   1.08        0       79              0       0        call   0.0000  0.0000  0.0000  -0.0011  0.6856





eix.bars = cwt_vol(eix.bars)
# >>> eix.bars
#            date   open    high    low  close   volume  log_return  vol_historical  cwt_power_mean  cwt_power_std
# 46   2023-09-11  70.00  70.590  69.75  69.87  1170202    0.001002        0.179245        0.084179       0.072118
# 47   2023-09-12  70.01  70.500  69.72  70.29  1725911    0.005993        0.178614        0.082751       0.047603
# 48   2023-09-13  70.42  71.080  70.18  70.74  2195487    0.006382        0.177971        0.087499       0.040509
# 49   2023-09-14  71.48  71.815  71.15  71.52  1542855    0.010966        0.176155        0.081955       0.037533
# 50   2023-09-15  71.21  72.120  71.16  71.56  2876658    0.000559        0.172567        0.079667       0.040276
# ..          ...    ...     ...    ...    ...      ...         ...             ...             ...            ...
# 267  2024-07-29  78.63  79.010  77.62  78.20  2030789   -0.003956        0.189140        0.108734       0.055089
# 268  2024-07-30  78.10  79.500  78.05  79.38  1922362    0.014977        0.187980        0.092461       0.050033
# 269  2024-07-31  79.75  80.520  79.06  80.01  4308250    0.007905        0.183351        0.078363       0.045813
# 270  2024-08-01  80.50  81.880  80.18  81.61  1834128    0.019800        0.188144        0.063542       0.031681
# 271  2024-08-02  82.58  83.050  80.74  82.27  2263934    0.008055        0.187742        0.045598       0.024286

# [226 rows x 10 columns]





eix.bars['vol_category'] = eix.bars.apply(lambda row: vol_category(eix.bars.loc[:row.name]),axis=1)
# >>> eix.bars
#            date   open    high    low  close   volume  log_return  vol_historical  cwt_power_mean  cwt_power_std vol_category
# 46   2023-09-11  70.00  70.590  69.75  69.87  1170202    0.001002        0.179245        0.084179       0.072118       Medium
# 47   2023-09-12  70.01  70.500  69.72  70.29  1725911    0.005993        0.178614        0.082751       0.047603       Medium
# 48   2023-09-13  70.42  71.080  70.18  70.74  2195487    0.006382        0.177971        0.087499       0.040509         High
# 49   2023-09-14  71.48  71.815  71.15  71.52  1542855    0.010966        0.176155        0.081955       0.037533       Medium
# 50   2023-09-15  71.21  72.120  71.16  71.56  2876658    0.000559        0.172567        0.079667       0.040276          Low
# ..          ...    ...     ...    ...    ...      ...         ...             ...             ...            ...          ...
# 267  2024-07-29  78.63  79.010  77.62  78.20  2030789   -0.003956        0.189140        0.108734       0.055089       Medium
# 268  2024-07-30  78.10  79.500  78.05  79.38  1922362    0.014977        0.187980        0.092461       0.050033       Medium
# 269  2024-07-31  79.75  80.520  79.06  80.01  4308250    0.007905        0.183351        0.078363       0.045813          Low
# 270  2024-08-01  80.50  81.880  80.18  81.61  1834128    0.019800        0.188144        0.063542       0.031681          Low
# 271  2024-08-02  82.58  83.050  80.74  82.27  2263934    0.008055        0.187742        0.045598       0.024286          Low

# [226 rows x 11 columns]






eix_garch_forecast = garch_vol_forecast(eix.bars, forecast_horizon=eix.days_to_expiry)
# >>> pd.DataFrame(eix_garch_forecast).T
#          h.01      h.02      h.03      h.04      h.05      h.06      h.07      h.08      h.09  ...     h.38      h.39      h.40      h.41     h.42      h.43     h.44      h.45      h.46
# 271  0.022393  0.022497  0.022598  0.022696  0.022793  0.022887  0.022979  0.023068  0.023156  ...  0.02498  0.025024  0.025067  0.025109  0.02515  0.025191  0.02523  0.025269  0.025307

# [1 rows x 46 columns]





eix.options_chain = VRP(eix.options_chain, eix_garch_forecast.mean())
# >>> eix.options_chain
#                 symbol  strike    bid    ask     mp  bidsize  asksize  open_interest  volume option_type      NPV   Delta   Gamma    Theta      IV       VRP
# 0   EIX240920C00047500    47.5  33.00  36.90  34.95       10       10              0       0        call  34.7700  1.0000  0.0000   0.6480  0.8722  0.848109
# 1   EIX240920P00047500    47.5   0.00   0.70   0.35        0       37              0       0         put   0.0000 -0.0000  0.0000  -0.0000  0.9588  0.934709
# 2   EIX240920P00050000    50.0   0.00   0.75   0.38        0       37              0       0         put   0.0000 -0.0000  0.0000  -0.0000  0.8969  0.872809
# 3   EIX240920C00050000    50.0  31.30  34.40  32.85        1       10              0       0        call  32.2700  1.0000  0.0000   0.5180  0.9882  0.964109
# 4   EIX240920P00055000    55.0   0.05   0.25   0.15       11      205              0       0         put   0.0000 -0.0000  0.0000  -0.0000  0.6304  0.606309
# 5   EIX240920C00055000    55.0  25.50  29.50  27.50       10       10              0       0        call  27.2700  1.0000  0.0000   0.2580  0.6832  0.659109
# 6   EIX240920P00060000    60.0   0.00   0.90   0.45        0       91              2       0         put   0.0000 -0.0000  0.0000  -0.0001  0.6367  0.612609
# 7   EIX240920C00060000    60.0  20.50  24.70  22.60        1        2              1       1        call  22.2751  0.9975  0.0005  -0.0665  0.5924  0.568309
# 8   EIX240920P00065000    65.0   0.00   1.40   0.70        0        1              0       0         put   0.0003 -0.0002  0.0001  -0.0185  0.5673  0.543209
# 9   EIX240920C00065000    65.0  15.80  18.40  17.10       14       17              0       0        call  17.2997  0.9958  0.0002  -0.2847     NaN       NaN
# 10  EIX240920P00067500    67.5   0.05   0.75   0.40       14       46              2       0         put   0.0025 -0.0015  0.0009  -0.1190  0.4292  0.405109
# 11  EIX240920C00067500    67.5  13.20  17.30  15.25        2        1              1       0        call  14.8162  0.9944  0.0009  -0.5067  0.4376  0.413509
# 12  EIX240920P00070000    70.0   0.15   0.25   0.20       45       83              8       1         put   0.0143 -0.0075  0.0037  -0.5082  0.3115  0.287409
# 13  EIX240920C00070000    70.0  11.10  14.90  13.00       10       10              1       0        call  12.3424  0.9883  0.0037  -1.0216  0.4241  0.400009
# 14  EIX240920C00072500    72.5  10.00  10.90  10.45       10       23              0       2        call   9.9045  0.9680  0.0114  -2.2083  0.3457  0.321609
# 15  EIX240920P00072500    72.5   0.25   0.35   0.30       39      117              8       0         put   0.0619 -0.0279  0.0114  -1.5693  0.2831  0.259009
# 16  EIX240920P00075000    75.0   0.40   0.55   0.48       39       94             32       8         put   0.2068 -0.0788  0.0262  -3.5863  0.2585  0.234409
# 17  EIX240920C00075000    75.0   8.20   9.60   8.90       30       33              9       0        call   7.5635  0.9173  0.0261  -4.3448  0.4056  0.381509
# 18  EIX240920C00077500    77.5   4.50   6.30   5.40      134       31             37       2        call   5.4227  0.8206  0.0459  -7.1360  0.2007  0.176609
# 19  EIX240920P00077500    77.5   0.75   0.90   0.82       31       93             23       1         put   0.5526 -0.1759  0.0461  -6.2660  0.2394  0.215309
# 20  EIX240920P00080000    80.0   1.35   1.45   1.40       28        4             70       2         put   1.2122 -0.3202  0.0642  -8.6085  0.2226  0.198509
# 21  EIX240920C00080000    80.0   4.20   4.40   4.30        7       33             24       2        call   3.5939  0.6772  0.0637  -9.5748  0.2732  0.249109
# 22  EIX240920P00082500    82.5   2.25   2.45   2.35       21       12              2       1         put   2.2982 -0.4945  0.0717  -9.4090  0.2088  0.184709
# 23  EIX240920C00082500    82.5   2.60   2.80   2.70       12       12            255       2        call   2.1881  0.5045  0.0709 -10.4606  0.2508  0.226709
# 24  EIX240920P00085000    85.0   3.60   3.90   3.75       33       12              0       0         put   3.8178 -0.6656  0.0661  -8.3361  0.1972  0.173109
# 25  EIX240920C00085000    85.0   1.50   1.65   1.58       16       27            152      42        call   1.2115  0.3359  0.0648  -9.4586  0.2411  0.217009
# 26  EIX240920C00087500    87.5   0.65   1.00   0.82       24       20             10       7        call   0.6059  0.1985  0.0495  -7.1781  0.2316  0.207509
# 27  EIX240920P00087500    87.5   4.20   6.40   5.30       31       11              0       0         put   5.7159 -0.8065  0.0513  -5.9942  0.1342  0.110109
# 28  EIX240920P00090000    90.0   6.60   8.20   7.40       44       22              0       0         put   7.8966 -0.9058  0.0344  -3.3858     NaN       NaN
# 29  EIX240920C00090000    90.0   0.35   0.45   0.40       35       28              0       9        call   0.2730  0.1039  0.0321  -4.6250  0.2279  0.203809
# 30  EIX240920P00095000    95.0  11.20  14.90  13.05       11       11              0       0         put  12.7300 -0.9997  0.0037   1.3017  0.3282  0.304109
# 31  EIX240920C00095000    95.0   0.00   0.75   0.38        0       29              0       0        call   0.0406  0.0199  0.0086  -1.2283  0.3150  0.290909
# 32  EIX240920P00100000   100.0  16.20  19.80  18.00       17       15              0       0         put  17.7300 -1.0000  0.0000   2.0820  0.4007  0.376609
# 33  EIX240920C00100000   100.0   0.00   0.75   0.38        0       26              0       0        call   0.0040  0.0024  0.0013  -0.1892  0.3947  0.370609
# 34  EIX240920P00105000   105.0  21.10  24.80  22.95       13       12              0       0         put  22.7300 -1.0000  0.0000   2.3420  0.4625  0.438409
# 35  EIX240920C00105000   105.0   0.00   0.75   0.38        0       40              0       0        call   0.0003  0.0002  0.0001  -0.0180  0.4678  0.443709
# 36  EIX240920P00110000   110.0  26.00  30.00  28.00       10        7              0       0         put  27.7300 -1.0000  0.0000   2.6020  0.5480  0.523909
# 37  EIX240920C00110000   110.0   0.00   2.15   1.08        0       79              0       0        call   0.0000  0.0000  0.0000  -0.0011  0.6856  0.661509








#
# Example - SPY
#

spy = BinomialOptions(symbol='SPY', past_bars=365, option_expiry_days=30);
spy.append_greeks_NPV();
