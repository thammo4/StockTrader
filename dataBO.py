from BinomialOptions import *



#
# Convert the `ivpBO` dictionary of dataframes output from dBO into a consolidated dataframe
# • call this function on the `ivpBO` ouptut from dBO
#

def trade_ideas (dict_df):
	#
	# Iterate over dictonary elements (which are dataframes) and construct a single dataframe with all of the rows
	#

	trades = pd.DataFrame();
	for x in dict_df:
		try:
			dfBO = dict_df[x];
			dfBO_filtered = dfBO[dfBO['IVP'] >= .7250];
			if not dfBO_filtered.empty:
				trades = pd.concat([trades, dfBO_filtered], ignore_index=True);
		except Exception as e:
			print(f"ERROR [{x}]: {e}");

	#
	# Append underlying, underlying price, and moneyness (XTM) to each option contract row
	#

	if not trades.empty:
		trades['underlying'] = trades['symbol'].apply(options_order.extract_occ_underlying);
		underlying_symbols = trades['underlying'].unique().tolist();
		trades['underlying_price'] = trades['underlying'].map(
			quotes.get_quote_data(underlying_symbols).set_index('symbol')['last']
		);
		trades['XTM'] = trades.apply(
			lambda row: what_the_money(row['strike'], row['underlying_price'], row['option_type']),
			axis = 1
		);

		#
		# Remove contracts for which there are no buyers
		#
		
		trades = trades.loc[trades['bidsize'] > 0];
	return trades;


#
# Iterate over DOW30 Stocks and use Binomial Model to compute NPV
# • Apply Brent's method to iterative compute σ_iv
# • Filter out contracts which did not have meaningful historical comparisons and store in ivpBO
#

def dBO (symbol_list, csv_directory):
	'''
		Arguments:
			• symbol_list: ['ONE', 'TWO', 'ETC']
			• csv_directory: subdirectory of `~/StockTrader/options/` containing the CSV files with historical options data from DoltHub
	'''
	dataBO = dict();
	chainBO = dict();
	ivpBO = dict();

	for x in symbol_list:
		try:
			xBO = BinomialOptions(symbol=x, past_bars=365, itm_percent=.0825, optionsIV_csv=f"./options/{csv_directory}/{x}_options_data.csv", optionsIV_verbose=True);
			dataBO[x] = xBO;
			chainBO[x] = xBO.options_chain;

			tmp = xBO.options_chain[xBO.options_chain['IVP'] >= 0];
			tmp.sort_values(by=['IVP'], ascending=False, inplace=True);

			ivpBO[x] = tmp;
		except Exception as e:
			print(f"NO GOOD: {x} ------> {e}");

	return dataBO, chainBO, ivpBO;