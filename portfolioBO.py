# FILE: `StockTrader/portfolioBO.py`

#
# Compute metrics to analyze current positions in portfolio
#

from config import *

#
# Track intraday PnL during trading hours
#

def live_pnl(num_seconds=120, start_at="09:45", stop_at="16:15", compute_beta_weight=False):
	pnl_values = [];
	timestamps = [];
	while True:
		the_time = time.strftime("%H:%M:%S");
		if the_time <= start_at:
			print(f"Market not yet open. [{the_time}]");
			print(f"Waiting {num_seconds} seconds...");
			time.sleep(num_seconds);
			continue;

		df = real_time_pnl(compute_beta_weight);
		pnl = np.sum(df['gain_loss']);

		pnl_values.append(pnl);
		timestamps.append(the_time);

		print(the_time); print(f"Net Gain/Loss: {pnl:.4f}"); print(df);
		print("-"*65); print('\n');

		if the_time >= stop_at:
			break;

		time.sleep(num_seconds);

	return timestamps, pnl_values;


#
# Construct DataFrame reflecting portfolio market value relative to cost basis
#

def real_time_pnl (compute_beta_weight=False, verbose=True):
	cols_to_drop = ['description', 'exch', 'type', 'ask_date', 'bid_date', 'expiration_type', 'contract_size', 'bidexch', 'askexch', 'week_52_high', 'week_52_low', 'trade_date', 'average_volume', 'close', 'change_percentage', 'last_volume', 'high', 'low', 'change', 'open', 'root_symbol'];
	more_cols_to_drop = ['volume', 'prevclose', 'bidsize', 'asksize', 'open_interest'];

	acct_positions = acct.get_positions(options=True); 						# only return positions for which the security is an option contract (opposed to stock)
	acct_quotes = quotes.get_quote_data(list(acct_positions['symbol']));

	acct_quotes.drop(cols_to_drop+more_cols_to_drop, axis=1, inplace=True);

	df_positions = pd.merge(
		acct_positions[['symbol', 'quantity', 'cost_basis']],
		acct_quotes,
		on='symbol',
		how='inner'
	);

	#
	# Retrieve quote data for underlyings
	#

	underlying_quotes = quotes.get_quote_data(list(df_positions['underlying'].unique()));
	underlying_prices = dict(
		zip(underlying_quotes['symbol'], underlying_quotes['last'])
	);

	#
	# Add economic sector of underlying
	#

	df_positions['sector'] = df_positions['underlying'].map(symbols_to_NYSESector(df_positions['underlying']));

	#
	# Add underlying price and moneyness of option
	#

	df_positions['underlying_price'] = df_positions['underlying'].apply(lambda x: underlying_prices.get(x));
	df_positions['XTM'] = df_positions.apply(lambda row: what_the_money(row['strike'], row['underlying_price'], row['option_type']), axis=1);

	# print(f'df positions after XTM:\n{df_positions}');

	#
	# Add cost basis, market value, and gain/loss by comparing cost basis and market value
	# Note - Market Value uses the midprice as proxy for current price
	#

	df_positions['mp'] = .5*(df_positions['bid'] + df_positions['ask']);
	df_positions['per_contract'] = df_positions['cost_basis'] / (100*df_positions['quantity'].abs());
	df_positions['market_value'] = 100 * df_positions['quantity'].abs() * df_positions['mp'];
	df_positions['gain_loss'] = np.where(
		df_positions['quantity'] > 0,
		df_positions['market_value'] - df_positions['cost_basis'],
		abs(df_positions['cost_basis']) - df_positions['market_value']
	);

	#
	# Compute size of each position with respect to total equity in account
	#

	df_positions['position_size'] = df_positions['market_value'] / acct.get_account_balance(True)['total_equity'];

	df_positions = df_positions[['underlying', 'symbol', 'sector', 'position_size', 'expiration_date', 'option_type','quantity', 'underlying_price', 'strike', 'XTM', 'market_value', 'cost_basis', 'last', 'per_contract', 'gain_loss', 'bid', 'ask', 'mp']];
	df_positions['expiry'] = pd.to_datetime(df_positions['expiration_date']); # + pd.to_timedelta('16:00:00'); # options expire at end of trading day of specified expiry
	df_positions['expiry_days'] = (df_positions['expiry'] - datetime.today()).dt.days + 2;
	df_positions.drop(['expiration_date'], axis=1, inplace=True);

	df_positions = df_positions[['underlying', 'symbol', 'sector', 'position_size', 'expiry', 'expiry_days', 'option_type','quantity', 'underlying_price', 'strike', 'XTM', 'bid', 'ask', 'mp', 'per_contract', 'market_value', 'cost_basis', 'gain_loss']];

	df_positions.sort_values(by=['gain_loss'], ascending=False, inplace=True);

	#
	# Compute delta-weighted beta of each position
	#

	if compute_beta_weight:
		underlyings = df_positions['underlying'].unique().tolist();
		beta_underlyings = {symbol: beta for symbol,beta in zip(underlyings, [beta_weight(x) for x in underlyings])}
		df_positions['beta'] = df_positions['underlying'].map(beta_underlyings);

		risk_free_rate = fred_rate();
		df_positions['delta'] = df_positions.apply(
			lambda row: portfolio_delta(row['underlying'], row['underlying_price'], row['strike'], risk_free_rate, row['expiry'], row['option_type']),
			axis=1
		);

		df_positions['delta_weighted_beta'] = np.round(
			df_positions['quantity'] * df_positions['beta'] * df_positions['delta'],
			4
		);

	if verbose:
		print(f"Net Gain/Loss: {np.sum(df_positions['gain_loss']):.4f}");

	return df_positions;


#
# Aggregate Vertical Spread Positions Market Value to Cost Basis
#

def vertical_spread_pnl (df=None):
	if df is None:
		df = real_time_pnl(compute_beta_weight=True);

	df_spreads = df.groupby('underlying').apply(
	lambda g: pd.Series({
		'sector': g['sector'].iloc[0],
		'option_type': g['option_type'].iloc[0],
		'expiry': g['expiry'].iloc[0],
		'expiry_days': g['expiry_days'].iloc[0],
		'size': g['position_size'].sum(),
		'underlying_price': g['underlying_price'].iloc[0],
		'short_strike': g.loc[g['quantity'] < 0, 'strike'].iloc[0] if not g.loc[g['quantity'] < 0, 'strike'].empty else None,
		'long_strike': g.loc[g['quantity'] > 0, 'strike'].iloc[0] if not g.loc[g['quantity'] > 0, 'strike'].empty else None,
		'delta_weighted_beta': g['delta_weighted_beta'].sum(),
		'cost_basis': g['cost_basis'].sum(),
		'market_value': g['market_value'].sum(),
		'gain_loss': g['gain_loss'].sum(),
		'gain_loss2': (g[g['quantity'] < 0]['market_value'].sum() - g[g['quantity'] > 0]['market_value'].sum()) + g['cost_basis'].sum(),
	})
	).reset_index();

	return df_spreads;


#
# Aggregate Position Size Exposure Across Economic Sector of Underlying
#

def sector_positions (df=None):
	acct_bal = acct.get_account_balance(True);
	if df is None:
		df = real_time_pnl();

	df_sector_positions = df.groupby('sector')['position_size'].sum().reset_index();
	df_sector_positions.columns = ['sector', 'position_size'];
	df_sector_positions.set_index('sector', inplace=True);

	df_sectors = pd.DataFrame({'sector':list(nyse_sectors.keys()), 'position_size':0});
	df_sectors['sector'] = df_sectors['sector'].replace('Unknown', 'Cash');
	df_sectors.set_index('sector', inplace=True);
	df_sectors.update(df_sector_positions);
	df_sectors.sort_values(by='sector', ascending=True, inplace=True)
	df_sectors.reset_index(inplace=True);
	df_sectors.at[1, 'position_size'] = 1 - df_sectors['position_size'].sum();

	return df_sectors;

#
# Convenience Function to Identify Sectors that Remain Unrepresented in Portfolio
#

def unfilled_sectors (trades=None):
	if trades is None:
		trades = pd.read_csv('./trades/trades_oct29.csv');
	trade_sectors = trades.sector.value_counts();
	positions = sector_positions();
	portfolio_sectors = positions[positions['position_size'] > 0]['sector'].to_list();
	return trade_sectors[~trade_sectors.index.isin(portfolio_sectors)];


#
# Compute β of a stock relative to a specified benchmark
# 	• Y = α + βΧ + ε
#

def beta_weight (stock, benchmark='SPY', num_weeks=52, verbose=False):
	df_stock = quotes.get_historical_quotes(
		symbol=stock,
		start_date=(datetime.today()-timedelta(weeks=num_weeks)).strftime('%Y-%m-%d'),
		end_date=today
	);
	df_benchmark = quotes.get_historical_quotes(
		symbol=benchmark,
		start_date=(datetime.today()-timedelta(weeks=num_weeks)).strftime('%Y-%m-%d'),
		end_date=today
	);

	stock_R = np.log(df_stock['close']/df_stock['close'].shift(1)).dropna();
	benchmark_R = np.log(df_benchmark['close']/df_benchmark['close'].shift(1)).dropna();
	X = sm.add_constant(benchmark_R);
	model = sm.OLS(stock_R, X).fit();

	if verbose:
		print(f'MODEL:\n{model.summary()}'); print('-'*65);

	return model.params[1];



#
# Use QuantLib Binomial Options Model to Compute Delta of Given Position
#

def portfolio_delta (underlying, S, K, r, expiry_dt, option_type):
	df_bars = quotes.get_historical_quotes(
		underlying,
		start_date=(datetime.today()-timedelta(weeks=52)).strftime('%Y-%m-%d')
	);

	q = dividend_yield(underlying);
	sigma = estimate_historical_vol(df_bars);
	

	today_dt = datetime.today();
	eval_date = ql.Date(today_dt.day, today_dt.month, today_dt.year);
	expiry_date = ql.Date(expiry_dt.day, expiry_dt.month, expiry_dt.year);
	ql.Settings.instance().evaluationDate = eval_date;

	option_type = ql.Option.Call if option_type == 'call' else ql.Option.Put;
	payoff = ql.PlainVanillaPayoff(option_type, K);
	exercise = ql.AmericanExercise(eval_date, expiry_date);
	amr_option = ql.VanillaOption(payoff, exercise);

	underlyingH = ql.QuoteHandle(ql.SimpleQuote(S));
	risk_freeTS = ql.YieldTermStructureHandle(
		ql.FlatForward(0, ql.TARGET(), ql.QuoteHandle(ql.SimpleQuote(r)), ql.Actual365Fixed())
	);
	div_yieldTS = ql.YieldTermStructureHandle(
		ql.FlatForward(0, ql.TARGET(), ql.QuoteHandle(ql.SimpleQuote(q)), ql.Actual365Fixed())
	);
	volTS = ql.BlackVolTermStructureHandle(
		ql.BlackConstantVol(0, ql.TARGET(), ql.QuoteHandle(ql.SimpleQuote(sigma)), ql.Actual365Fixed())
	);

	bsm_process = ql.BlackScholesMertonProcess(
		underlyingH, div_yieldTS, risk_freeTS, volTS
	);

	steps = 500;
	binom_engine = ql.BinomialVanillaEngine(bsm_process, 'crr', steps);
	amr_option.setPricingEngine(binom_engine);

	option_delta = amr_option.delta();

	print(f'PARAMS: (S={S}, K={K}, r={r}, q={q}, σ={sigma}, expiry={expiry_dt.strftime("%Y-%m-%d")}, type={option_type})  -->  Δ={option_delta}')

	return option_delta;















