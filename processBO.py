from config import *


#
# Iterate through all csv files in `csv_directory` and orgranize records into a single dataframe
# • the csv files are the output from `dataBO`
#

def consolidate_trades (csv_directory):
	df_trades = pd.DataFrame();
	for x in range(1,10):
		csv_file = f"{csv_directory}/largecap{x}.csv";
		try:
			largecap = pd.read_csv(csv_file);
			df_trades = pd.concat([df_trades, largecap], axis=0, ignore_index=True);
		except FileNotFoundError:
			print(f"Not Found: largecap{x}.csv");
			continue;

	if df_trades.empty:
		return df_trades;

	#
	# Remove contracts with bid price of 0 because we can't sell contracts for nothing
	#

	df_trades = df_trades.loc[df_trades['bid'] > 0]

	#
	# Remove rows with IVP = 1 because that seems too unrealistic
	#

	df_trades = df_trades.loc[df_trades['IVP'] < 1];

	#
	# Append the economic sector of the underlying and remove rows with 'Unknown' sector
	#

	u_sector = {};
	for u in list(df_trades['underlying'].unique()):
		found = False;
		for sector, symbols in nyse_sectors.items():
			if u in symbols:
				u_sector[u] = sector;
				found = True;
				break;
		if not found:
			u_sector[u] = 'Unknown'

	df_trades['sector'] = df_trades['underlying'].map(u_sector);
	df_trades = df_trades.loc[df_trades['sector'] != 'Unknown'];

	return df_trades;


#
# Remove rows for which the underlying fails to appear at least `min_contracts` times
#

def filter_contract_count (df_trades, min_contracts=12):
	freq_underlying = df_trades['underlying'].value_counts();
	freq_underlying_symbols = freq_underlying[freq_underlying >= min_contracts].index.tolist();
	df_trades = df_trades.loc[df_trades['underlying'].isin(freq_underlying_symbols)];

	return df_trades;
