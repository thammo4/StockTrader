#
# FILE: `StockTrader/scripts/dolt_csv_to_parquet.py`
#

import logging
import pandas as pd
import numpy as np
from StockTrader.settings import (
	STOCK_TRADER_MARKET_DATA,
	logger
)

def dolt_csv_to_parquet(symbol, return_df=False):
	#
	# Convert the date columns to unix timestamps because they will be read more easily by PySpark later
	#

	def format_date_cols (df):
		for x in ['date', 'expiration']:
			df[x] = pd.to_datetime(df[x]).astype(np.int64)
		df = df.reset_index(drop=True)
		return df

	try:
		#
		# Read in the raw CSV downloaded from Dolthub, format date columns, create parquet
		#

		csv_fpath = f"{STOCK_TRADER_MARKET_DATA}/{symbol}_options_data.csv"
		df_csv = pd.read_csv(csv_fpath)
		df_formatted = format_date_cols(df_csv)
		df_formatted.to_parquet(f"{STOCK_TRADER_MARKET_DATA}/{symbol}_options_data.parquet")
		logger.info(f"Parquet: {STOCK_TRADER_MARKET_DATA}/{symbol}_options_data.parquet")

		#
		# Create local text file with start_date and end_date for use by other processing scripts
		#

		start_date = str(df_formatted.iloc[0]['date'])
		end_date = str(df_formatted.iloc[-1]['date'])

		fpath = f"{STOCK_TRADER_MARKET_DATA}/{symbol}_date_range.txt"
		with open (fpath, "w") as f:
			f.write(start_date + "\n")
			f.write(end_date + "\n")
		logger.info(f"TS date txt file: {fpath}")

		#
		# Return dataframe for testing purposes
		#

		if return_df:
			return df_formatted

	except Exception as e:
		logger.error(f"ERROR [dolt_csv_to_parquet]: {e}")