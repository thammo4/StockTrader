# FILE: `StockTrader/OptionsIV.py`

from config import * 							# loads necessary modules and variables

class OptionsIV:
	def __init__ (self, csv_file, symbol, past_bars=365, option_expiry_days=30, itm_percent=.025, verbose=False):
		self.csv_file = csv_file;
		self.symbol = symbol;
		self.past_bars = past_bars;
		self.option_expiry_days = option_expiry_days;
		self.itm_percent = .025;
		self.verbose = verbose;

		self.df = None;
		self.df_itm = None;
		self.bar_data = None;
		self.dividend_data = None;
		self.fred_data = None;

		self.prep_dolt_historical();
		self.df['NPV'] = self.df.apply(lambda row: self.binom_npv(row), axis=1);
		self.df['IV'] = self.df.apply(lambda row: self.binom_iv(row), axis=1);
		self.df.dropna(inplace=True);
		self.add_moneyness();
		self.df['OCC'] = self.df.apply(lambda row: self.add_occ(row), axis=1);
		self.create_itm_df();

		if self.verbose:

			print(f"SELF.DF:\n{self.df}");
			print(75*'-');

			print(f"ITM DF:\n{self.df_itm}");
			print(75*'-');


	def prep_dolt_historical (self):
		self.df = pd.read_csv(self.csv_file, sep='|');
		self.df.columns = [x.strip() for x in list(self.df.columns)];
		self.df = self.df[['date', 'expiration', 'ttm', 'midprice', 'strike', 'call_put', 'act_symbol']]; 

		for x in ['date', 'expiration', 'call_put', 'act_symbol']:
			self.df[x] = self.df[x].str.strip();
			if x in ['date', 'expiration']:
				self.df[x] = pd.to_datetime(self.df[x]);

		self.df = self.df.reset_index(drop=True);

		if self.verbose:
			print(f'DF:\n{self.df}');
			print(75*'-');

		#
		# Merge Historical Closing Price Data from Tradier
		#

		self.bar_data = quotes.get_historical_quotes(
			symbol = self.symbol,
			start_date = (self.df.iloc[0]['date'] - timedelta(weeks=5)).strftime('%Y-%m-%d'),
			end_date = datetime.today().strftime('%Y-%m-%d')
		);

		self.bar_data['log_return'] = np.log(self.bar_data['close']).diff();
		self.bar_data.dropna(inplace=True);
		self.bar_data = self.bar_data[['date', 'close', 'log_return']];
		self.bar_data['date'] = pd.to_datetime(self.bar_data['date']);

		if self.verbose:
			print(f'BAR DATA:\n{self.bar_data}');

		df_merged_price = pd.merge(self.df, self.bar_data, on='date', how='inner');

		if self.verbose:
			print(f'MERGED PRICE:\n{df_merged_price}');
			print(75*'-');

		#
		# Merge Historical Dividend Data from Tradier
		#

		self.dividend_data = dividend_table(self.symbol);
		df_merged_dividends = pd.merge(df_merged_price, self.dividend_data, how='cross');
		df_merged_dividends = df_merged_dividends[
			df_merged_dividends['ex_date'].between(df_merged_dividends['date'], df_merged_dividends['ex_date'], inclusive='right')
		];
		df_merged_dividends['div_yield'] = (df_merged_dividends['cash_amount'] * df_merged_dividends['frequency']) / df_merged_dividends['close'];
		df_merged_dividends.drop(['cash_amount', 'frequency', 'ex_date'], axis=1, inplace=True);
		df_merged_dividends.drop_duplicates(inplace=True);

		if self.verbose:
			print(f'MERGED DIVIDENDS:\n{df_merged_dividends}');
			print(75*'-');

		#
		# Fetch + Merge Historical Risk Free Rate Data from FRED (3-Month T-Bill)
		#

		self.fred_data = fred.get_series(series_id='TB3MS', observation_start=df_merged_dividends.iloc[0]['date']);
		self.fred_data = self.fred_data.reset_index();
		self.fred_data.columns = ['fred_date', 'fred_rate'];
		self.fred_data['fred_rate'] /= 100;

		df_merged_fred = pd.merge(df_merged_dividends, self.fred_data, how='cross');
		df_merged_fred = df_merged_fred[
			df_merged_fred['fred_date'].between(df_merged_fred['date'], df_merged_fred['expiration'], inclusive='both')
		];
		df_merged_fred.drop(['act_symbol', 'symbol', 'fred_date'], axis=1, inplace=True);
		df_merged_fred.drop_duplicates(inplace=True);

		if self.verbose:
			print(f'MERGED FRED:\n{df_merged_fred}');
			print(75*'-');

		#
		# Add Historical Rolling Volatility
		#

		def past_vol (row, min_w=5):
			w = max(row['ttm'], min_w);
			std_rolling = self.bar_data['log_return'].rolling(window=w).std();
			tau = w/252; # annualize
			return std_rolling.iloc[-1] / np.sqrt(tau);

		df_merged_final = df_merged_fred;
		df_merged_final['vol_historical'] = df_merged_final.apply(lambda row: past_vol(row), axis=1);

		if self.verbose:
			print(f'MERGED VOL:\n{df_merged_final}');
			print(75*'-');

		#
		# Round Columns with Many Decimal Places
		#

		for x in ['div_yield', 'fred_rate', 'vol_historical', 'log_return']:
			d_places = 8 if x != 'fred_rate' else 4;
			df_merged_final[x] = np.round(df_merged_final[x], d_places);

		#
		# Drop Any Straggling Duplicates and Set df as Final DataFrame
		#

		self.df = df_merged_final.drop_duplicates(subset=['date', 'expiration', 'midprice', 'call_put', 'close', 'fred_rate']);

		if (self.verbose):
			print(f'SELF.DF:\n{self.df}');
			print(75*'-');

	def add_moneyness (self):
		self.df['moneyness'] = np.round(self.df['strike'] / self.df['close'], 4);

	def add_occ (self, row):
		yr = row['expiration'].year % 100;
		month = row['expiration'].month;
		day = row['expiration'].day;

		expiry = f"{yr:02d}{month:02d}{day:02d}";

		option_type = 'C' if row['call_put'] == 'Call' else 'P';

		if row['strike'] < 1000:
			strike = f"{int(row['strike']*1000):05d}";
		else:
			strike = f"{int(row['strike']*100):05d}";

		return f"{self.symbol}{expiry}{option_type}{strike}";

	def create_itm_df (self):
		if 'moneyness' not in self.df.columns:
			self.add_moneyness();
		self.df_itm = self.df.loc[self.df['moneyness'].between(1-self.itm_percent, 1+self.itm_percent, inclusive='both')];

	def binom_npv (self, row):
		try:
			S = row['close'];
			K = row['strike'];
			q = row['div_yield'];
			r = row['fred_rate'];
			sigma = row['vol_historical'];
			eval_date = row['date'];
			expiry_date = row['expiration'];
			option_type = ql.Option.Call if row['call_put'] == 'Call' else ql.Option.Put;

			if self.verbose:
				print(f'NPV(S,K,[T0,T1],r,q,σ) = NPV({S}, {K}, [{eval_date.strftime("%Y-%m-%d")}, {expiry_date.strftime("%Y-%m-%d")}], {r:.6}, {q:.6}, {sigma:.6})')

			if isinstance(eval_date, str):
				eval_date = datetime.strptime(eval_date, '%Y-%m-%d');
			if isinstance(expiry_date, str):
				expiry_date = datetime.strptime(expiry_date, '%Y-%m-%d');

			eval_date = ql.Date(eval_date.day, eval_date.month, eval_date.year);
			expiry_date = ql.Date(expiry_date.day, expiry_date.month, expiry_date.year);

			ql.Settings.instance().evaluationDate = eval_date;

			payoff = ql.PlainVanillaPayoff(option_type, K);
			exercise = ql.AmericanExercise(eval_date, expiry_date);
			amr_option = ql.VanillaOption(payoff, exercise);

			spotH = ql.QuoteHandle(ql.SimpleQuote(S));
			risk_freeTS = ql.YieldTermStructureHandle(
				ql.FlatForward(eval_date, r, ql.Actual365Fixed())
			);
			div_yieldTS = ql.YieldTermStructureHandle(
				ql.FlatForward(eval_date, q, ql.Actual365Fixed())
			);
			volH = ql.BlackVolTermStructureHandle(
				ql.BlackConstantVol(
					eval_date,
					ql.UnitedStates(ql.UnitedStates.NYSE),
					sigma,
					ql.Actual365Fixed()
				)
			);

			bsm_process = ql.BlackScholesMertonProcess(spotH, div_yieldTS, risk_freeTS, volH);

			binom_engine = ql.BinomialVanillaEngine(bsm_process, 'crr', 325);
			amr_option.setPricingEngine(binom_engine);
			return np.round(amr_option.NPV(), 4);

		except Exception as e:
			print(f'ERROR binom_npv: {e}');
			return np.nan;

	def binom_iv (self, row):
		def obj (sigma):
			row_copy = row.copy();
			#
			# Record the initial values of sigma and NPV
			#

			if self.verbose:
				print(f"BinomIV [{row['call_put']}]: S={row['close']}, K={row['strike']}, q={row['div_yield']}, r={row['fred_rate']}, σ={row['vol_historical']}, date={row['date']}, expiry={row['expiration']}, NPV={row['NPV']}");
				# print(75*'-');

			#
			# Update the volatility value with the new guess
			#

			row_copy['vol_historical'] = sigma;

			#
			# Compute the Binomial Model NPV with the updated volatililty value
			#

			binom_price_new = self.binom_npv(row_copy);
			if self.verbose:
				print(f"σ' = {sigma} -> NPV(σ') = NPV({sigma}) = {binom_price_new}");

			if np.isnan(binom_price_new):
				return 570;

			#
			# Determine the difference between the broker midprice and the updated Binomial NPV price
			#

			price_diff = binom_price_new - row['midprice'];

			if self.verbose:
				print(f"NPV(σ') - MP = [{binom_price_new} - {row['midprice']}] = {price_diff}");
				print('.........');

			return price_diff;

		try:
			iv = brentq(f=obj, a=1e-3, b=1.25, xtol=1e-8);
			if self.verbose:
				print(f">>> σ_iv = {iv}");
				print(75*'-'); print('\n');
			return np.round(iv, 8);
		except Exception as e:
			if self.verbose:
				print(f"FAILED [brentq]: {row}"); print(f"Error: {e}"); print("\n");
			return np.nan;

	def iv_percentile (self, row):
		def filter_iv_factors (row):
			df_option_type = self.df.loc[self.df['call_put'].str.lower() == row['option_type']];
			df_option_ttm = df_option_type.loc[
				df_option_type['ttm'].between(left=row['days_to_expiry']-10, right=row['days_to_expiry']+10, inclusive='both')
			];
			df_option_moneyness = df_option_ttm.loc[
				df_option_ttm['moneyness'].between(left=row['Moneyness']-.05, right=row['Moneyness']+.05, inclusive='both')
			];
			return df_option_moneyness;

		df_options_similar = filter_iv_factors(row);
		if len(df_options_similar) == 0:
			if self.verbose:
				print(f"No Historical Comparison: {row}");
			return np.nan;

		count_below = np.sum(df_options_similar['IV'] < row['IV']);
		percentile = count_below / len(df_options_similar);

		return percentile;


if __name__ == '__main__':
	xli = OptionsIV(csv_file='./options/xli_options.csv', symbol='XLI', itm_percent=.075, verbose=True);



#
# OUTPUT SAMPLE - Class Variable DataFrames
# • dia = OptionsIV(csv='options/DIA_options_data.csv', symbol='DIA')
# • dia.df = Historical Options Data
# • dia.df_itm = Historical Options Contracts with Moneyness ≈ 1
# • dia.bar_data = Historical Data on Underlying
#

# >>> dia.df
#               date expiration  ttm  midprice  strike call_put   close  log_return  div_yield  fred_rate  vol_historical     NPV        IV  moneyness
# 7345    2023-09-06 2023-10-06   30     0.015   240.0      Put  345.00   -0.005723   0.011365     0.0534        0.027943    0.00  0.431338     0.6957
# 7633    2023-09-06 2023-10-06   30     0.020   250.0      Put  345.00   -0.005723   0.011365     0.0534        0.027943    0.00  0.396804     0.7246
# 7921    2023-09-06 2023-10-06   30     0.025   260.0      Put  345.00   -0.005723   0.011365     0.0534        0.027943    0.00  0.359743     0.7536
# 8209    2023-09-06 2023-10-06   30     0.030   265.0      Put  345.00   -0.005723   0.011365     0.0534        0.027943    0.00  0.343727     0.7681
# 8497    2023-09-06 2023-10-06   30     0.040   275.0      Put  345.00   -0.005723   0.011365     0.0534        0.027943    0.00  0.309241     0.7971
# ...            ...        ...  ...       ...     ...      ...     ...         ...        ...        ...             ...     ...       ...        ...
# 1559531 2024-07-29 2024-09-20   53     0.020   505.0     Call  405.51   -0.001134   0.009669     0.0505        0.018575    0.00  0.195727     1.2453
# 1559543 2024-07-29 2024-09-20   53    99.575   505.0      Put  405.51   -0.001134   0.009669     0.0505        0.018575   99.49  0.340985     1.2453
# 1559555 2024-07-29 2024-09-20   53     0.015   515.0     Call  405.51   -0.001134   0.009669     0.0505        0.018575    0.00  0.206337     1.2700
# 1559567 2024-07-29 2024-09-20   53   109.525   515.0      Put  405.51   -0.001134   0.009669     0.0505        0.018575  109.49  0.356146     1.2700
# 1559591 2024-07-29 2024-09-20   53   114.575   520.0      Put  405.51   -0.001134   0.009669     0.0505        0.018575  114.49  0.377662     1.2823
#
# [14968 rows x 14 columns]
#
# >>> dia.df_itm
#               date expiration  ttm  midprice  strike call_put   close  log_return  div_yield  fred_rate  vol_historical     NPV        IV  moneyness
# 10657   2023-09-06 2023-10-06   30     9.500   338.0     Call  345.00   -0.005723   0.011365     0.0534        0.027943  8.1592  0.108425     0.9797
# 10801   2023-09-06 2023-10-06   30     2.105   338.0      Put  345.00   -0.005723   0.011365     0.0534        0.027943  0.0011  0.132328     0.9797
# 10945   2023-09-06 2023-10-06   30     4.575   345.0     Call  345.00   -0.005723   0.011365     0.0534        0.027943  1.7954  0.100355     1.0000
# 11089   2023-09-06 2023-10-06   30     4.250   345.0      Put  345.00   -0.005723   0.011365     0.0534        0.027943  0.7230  0.120010     1.0000
# 11233   2023-09-06 2023-10-06   30     1.510   352.0     Call  345.00   -0.005723   0.011365     0.0534        0.027943  0.0189  0.093181     1.0203
# ...            ...        ...  ...       ...     ...      ...     ...         ...        ...        ...             ...     ...       ...        ...
# 1559255 2024-07-29 2024-09-20   53     4.325   397.0      Put  405.51   -0.001134   0.009669     0.0505        0.018575  0.0000  0.141454     0.9790
# 1559267 2024-07-29 2024-09-20   53     9.075   406.0     Call  405.51   -0.001134   0.009669     0.0505        0.018575  2.3381  0.131609     1.0012
# 1559279 2024-07-29 2024-09-20   53     7.225   406.0      Put  405.51   -0.001134   0.009669     0.0505        0.018575  0.7595  0.129116     1.0012
# 1559291 2024-07-29 2024-09-20   53     4.875   414.0     Call  405.51   -0.001134   0.009669     0.0505        0.018575  0.0187  0.121019     1.0209
# 1559303 2024-07-29 2024-09-20   53    11.400   414.0      Put  405.51   -0.001134   0.009669     0.0505        0.018575  8.4900  0.121965     1.0209
#
# [2309 rows x 14 columns]
#
# >>> dia.bar_data
#           date   close  log_return
# 1   2023-08-03  351.99   -0.002128
# 2   2023-08-04  350.65   -0.003814
# 3   2023-08-07  354.62    0.011258
# 4   2023-08-08  353.02   -0.004522
# 5   2023-08-09  351.28   -0.004941
# ..         ...     ...         ...
# 272 2024-08-30  416.21    0.005541
# 273 2024-09-03  410.25   -0.014423
# 274 2024-09-04  410.42    0.000414
# 275 2024-09-05  408.46   -0.004787
# 276 2024-09-06  404.47   -0.009816

# [276 rows x 3 columns]
















