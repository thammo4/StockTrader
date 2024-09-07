# SAMPLE DOLT SQL QUERIES TO RETRIEVE ORIGINAL RAW DATASET
#
# dolt sql -q 'SELECT `date`, `expiration`, DATEDIFF(`expiration`, `date`) AS ttm, .5*(`bid`+`ask`) AS midprice, `strike`, `call_put`, `act_symbol` FROM `option_chain` WHERE `act_symbol` = "SPY" AND `date` >= DATE_SUB(CURDATE(), INTERVAL 1 YEAR)' > spy_options.txt 
# dolt sql -q 'SELECT `date`, `expiration`, DATEDIFF(`expiration`, `date`) AS ttm, .5*(`bid`+`ask`) AS midprice, `strike`, `call_put`, `act_symbol` FROM `option_chain` WHERE `act_symbol` = "KRE" AND `date` >= DATE_SUB(CURDATE(), INTERVAL 1 YEAR)' > kre_options.txt
# dolt sql -q 'SELECT `date`, `expiration`, DATEDIFF(`expiration`, `date`) AS ttm, .5*(`bid`+`ask`) AS midprice, `strike`, `call_put`, `act_symbol` FROM `option_chain` WHERE `act_symbol` = "XLF" AND `date` >= DATE_SUB(CURDATE(), INTERVAL 1 YEAR)' > xlf_options.txt
# dolt sql -q 'SELECT `date`, `expiration`, DATEDIFF(`expiration`, `date`) AS ttm, .5*(`bid`+`ask`) AS midprice, `strike`, `call_put`, `act_symbol` FROM `option_chain` WHERE `act_symbol` = "XLI" AND `date` >= DATE_SUB(CURDATE(), INTERVAL 1 YEAR)' > xli_options.txt
# dolt sql -q 'SELECT `date`, `expiration`, DATEDIFF(`expiration`, `date`) AS ttm, .5*(`bid`+`ask`) AS midprice, `strike`, `call_put`, `act_symbol` FROM `option_chain` WHERE `act_symbol` = "DIA" AND `date` >= DATE_SUB(CURDATE(), INTERVAL 1 YEAR)' > dia_options.txt


from config import * # loads necessary modules and variables
from BinomialOptions import BinomialOptions

def iv_histogram (iv_series):
	plt.figure(figsize=(10,8));
	plt.hist(iv_series, color='green', edgecolor='black');
	plt.title('Histogram of Historical IV');
	plt.xlabel('IV Values'); plt.ylabel('Freq');
	plt.show();

def iv_boxplot (iv_series):
	plt.figure(figsize=(10,8));
	sns.boxplot(x=iv_series, color='blue');
	plt.title('Boxplot of Historical IV');
	plt.xlabel('IV Values');
	plt.show();

def prep_dolt_historical (csv, symbol, num_past_bars=365, verbose=False):
	#
	# Load Dolt Historical Options Data from Local CSV
	#

	df = pd.read_csv(csv, sep='|');
	df.columns = [x.strip() for x in list(df.columns)];

	df = df[['date', 'expiration', 'ttm', 'midprice', 'strike', 'call_put', 'act_symbol']];

	for x in ['date', 'expiration', 'call_put', 'act_symbol']:
		df[x] = [y.strip() for y in df[x]];

	for x in ['date', 'expiration']:
		df[x] = pd.to_datetime(df[x]);

	df = df.reset_index(drop=True);
	if verbose:
		print(f'DF:\n{df}');
		print(75*'-');


	#
	# Merge Historical Closing Price Data from Tradier 

	bar_data = quotes.get_historical_quotes(
		symbol = symbol,
		start_date = (df.iloc[0]['date'] - timedelta(weeks=5)).strftime('%Y-%m-%d'),
		end_date = datetime.today().strftime('%Y-%m-%d')
	);
	bar_data['log_return'] = np.log(bar_data['close']).diff();
	bar_data.dropna(inplace=True);
	bar_data = bar_data[['date', 'close', 'log_return']];
	bar_data['date'] = pd.to_datetime(bar_data['date']);
	if verbose:
		print(f'BAR DATA:\n{bar_data}');

	df_merged_price = pd.merge(df, bar_data, on='date', how='inner');
	if verbose:
		print(f'MERGED PRICE:\n{df_merged_price}');
		print(75*'-');

	#
	# Merge Historical Dividend Data from Tradier
	#

	div_data = dividend_table('QCOM');
	df_merged_dividends = pd.merge(df_merged_price, div_data, how='cross');
	df_merged_dividends = df_merged_dividends[
		df_merged_dividends['ex_date'].between(df_merged_dividends['date'], df_merged_dividends['ex_date'], inclusive='right')
	];
	df_merged_dividends['div_yield'] = (df_merged_dividends['cash_amount'] * df_merged_dividends['frequency']) / df_merged_dividends['close'];
	df_merged_dividends.drop(['cash_amount', 'frequency', 'ex_date'], axis=1, inplace=True);
	# df_merged_dividends = df_merged_dividends.drop_duplicates();
	df_merged_dividends.drop_duplicates(inplace=True);
	if verbose:
		print(f'MERGED DIVIDENDS:\n{df_merged_dividends}');
		print(75*'-');


	#
	# Add 3-Month T-Bill Data from FRED for Risk Free Rate
	#

	fred_data = fred.get_series(series_id='TB3MS', observation_start = df_merged_dividends.iloc[0]['date']);
	fred_data = fred_data.reset_index();
	fred_data.columns = ['fred_date', 'fred_rate'];
	fred_data['fred_rate'] /= 100;

	df_merged_fred = pd.merge(df_merged_dividends, fred_data, how='cross');
	df_merged_fred = df_merged_fred[
		df_merged_fred['fred_date'].between(df_merged_fred['date'], df_merged_fred['expiration'], inclusive='both')
	];
	df_merged_fred.drop('fred_date', axis=1, inplace=True)
	df_merged_fred.drop_duplicates(inplace=True)
	if verbose:
		print(f'MERGED FRED\n{df_merged_fred}');
		print(75*'-');


	#
	# Add historical rolling volatility
	#

	def past_vol (row, min_w=5):
		w = max(row['ttm'], min_w);
		std_rolling = bar_data['log_return'].rolling(window=w).std();
		tau = w/252;
		return std_rolling.iloc[-1] / np.sqrt(tau);

	df_merged = df_merged_fred.drop(['act_symbol', 'symbol'], axis=1);
	df_merged['vol_historical'] = df_merged.apply(
		lambda row: past_vol(row), axis=1
	);
	if verbose:
		print(f'MERGED VOL:\n{df_merged}');
		print(75*'-');


	#
	# Round Columns with Many Decimal Places
	#

	df_merged['div_yield'] = np.round(df_merged['div_yield'], 7);
	df_merged['fred_rate'] = np.round(df_merged['fred_rate'], 4);
	df_merged['vol_historical'] = np.round(df_merged['vol_historical'], 7);
	df_merged['log_return'] = np.round(df_merged['log_return'], 8);


	#
	# Drop Duplicates and Return
	#

	df_merged.drop_duplicates(subset=['date', 'expiration', 'midprice', 'call_put', 'close',  'fred_rate'], inplace=True)

	return df_merged;

def binom_npv (row):
	try:
		S = row['close'];
		K = row['strike'];
		q = row['div_yield'];
		r = row['fred_rate'];
		sigma = row['vol_historical'];
		eval_date = row['date'];
		expiry_date = row['expiration'];
		option_type = ql.Option.Call if row['call_put'] == 'Call' else ql.Option.Put;

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

def binom_iv (row):
	def obj (sigma):
		row_copy = row.copy();
		#
		# Record the initial values of sigma and NPV
		#

		print(f"BinomIV [{row['call_put']}]: S={row['close']}, K={row['strike']}, q={row['div_yield']}, r={row['fred_rate']}, σ={row['vol_historical']}, date={row['date']}, expiry={row['expiration']}, NPV={row['NPV']}");
		# print('\n');

		#
		# Update the volatility value with the new guess
		#

		row_copy['vol_historical'] = sigma;

		#
		# Compute the Binomial Model NPV with the updated volatililty value
		#

		binom_price_new = binom_npv(row_copy);
		print(f"σ' = {sigma} -> NPV(σ') = NPV({sigma}) = {binom_price_new}");
		# print('\n');

		if np.isnan(binom_price_new):
			return 570;

		#
		# Determine the difference between the broker midprice and the updated Binomial NPV price
		#

		price_diff = binom_price_new - row['midprice'];
		print(f"NPV(σ') - MP = [{binom_price_new} - {row['midprice']}] = {price_diff}");
		print('.............');
		# print('\n');

		return price_diff;

	try:
		iv = brentq(obj, 1e-3, 1.0, xtol=1e-8);
		print(f">>> σ_iv = {iv}");
		print(75*'-'); print('\n');
		return np.round(iv, 5);
	except ValueError as e:
		return np.nan;

def add_moneyness (df):
	df['moneyness'] = np.round(df['strike'] / df['close'], 4);
	return df;

def filter_moneyness (df, itm_percent=.05):
	return df.loc[df['moneyness'].between(1-itm_percent, 1+itm_percent, inclusive='both')];

def filter_iv_factors (row, df_past):
	df_filter_type = df_past.loc[df_past['call_put'].str.lower() == row['option_type']];
	df_filter_ttm = df_filter_type.loc[df_filter_type['ttm'].between(row['days_to_expiry']-10, row['days_to_expiry']+10, inclusive='both')];
	df_filter_moneyness = df_filter_ttm.loc[df_filter_ttm['moneyness'].between(row['Moneyness']-.05, row['Moneyness']+.05, inclusive='both')];
	return df_filter_moneyness;

def iv_percentile (row, df_past):
	df_filtered = filter_iv_factors(row, df_past);
	if len(df_filtered) == 0:
		return np.nan;
	count_below = np.sum(df_filtered['IV'] < row['IV']);
	percentile = count_below / len(df_filtered);
	return percentile;

def main(csv_file, ticker_symbol, ttm=30, itm=.05):
	df = prep_dolt_historical(csv_file, symbol=ticker_symbol);
	df = add_moneyness(df);
	df_itm = filter_moneyness(df, itm_percent=itm);
	df_itm['NPV'] = df_itm.apply(binom_npv, axis=1);
	df_itm['IV'] = df_itm.apply(binom_iv, axis=1);

	b = BinomialOptions(symbol=ticker_symbol, past_bars=365, option_expiry_days=ttm);
	b.set_itm_options();

	current_options = b.near_itm.sort_values(by=['option_type']);
	current_options['IVP'] = current_options.apply(lambda row: iv_percentile(row, df_itm), axis=1);

	return current_options;

if __name__ == '__main__':
	df = prep_dolt_historical('qcom_options.csv', symbol='QCOM');
	df.rename({'mp':'midprice'}, axis=1, inplace=True);
	df = add_moneyness(df);
	df_itm = filter_moneyness(df, itm_percent = .035);
	df_itm['NPV'] = df_itm.apply(binom_npv, axis=1);
	df_itm['IV'] = df_itm.apply(binom_iv, axis=1);

	qcom = BinomialOptions(symbol='QCOM', past_bars=365, option_expiry_days=30);
	qcom.set_itm_options();

	qcom_options = qcom.near_itm;
	qcom_options['IVP'] = qcom_options.apply(lambda row: iv_percentile(row, df_itm), axis=1);

	print(f'QCOM OPTIONS W/IVP:\n{qcom_options}');