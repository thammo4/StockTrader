#
# FILE: `StockTrader/scripts/prep_bopm_data.py`
#

import os
import traceback
import pandas as pd
import numpy as np
from StockTrader.settings import STOCK_TRADER_MARKET_DATA, logger

def prep_bopm_data (symbol, return_df=False):
	"""
	Extract, Transform, Load Data to Prep Input for Binomial Options Pricing Model (BOPM)
	Collect data from locally stored parquet files regarding historical closing stock prices, option prices, dividends, and 3-Month TBills

	Returns:
		Processed and Merged DataFrame if return_df=True else None
	"""
	try:
		logger.info(f"Preparing bopm dataset, symbol={symbol} [prep_bopm_data]")
		def extract_from_parquet (dataset, symbol=None):
			"""
			Read in various datasets from STOCK_TRADER_MARKET_DATA directory parquet files.
			Args:
				dataset (str): Data sought. One of: 'ohlcv_bar', 'options', 'dividend', 'fred'.
				symbol (str): Stock ticker symbol

			Returns:
				Dataframe with parquet data and formatted date columns.
			"""
			try:
				if dataset not in ["ohlcv_bar", "options", "dividend", "fred"]:
					logger.error(f"Unknown dataset requested: requested={dataset}, symbol={symbol} [prep_bopm_data]")
					return None

				parquet_file = f"{symbol}_{dataset}_data.parquet" if dataset != "fred" else "fred_data.parquet"
				fpath_parquet = os.path.join(STOCK_TRADER_MARKET_DATA, parquet_file)

				df = pd.read_parquet(fpath_parquet)
				if df.empty:
					logger.warning(f"Empty dataframe, dataset={dataset}, symbol={symbol} [prep_bopm_data]")
					return None

				#
				# Caste Date Columns from Nanosec Unix TS -> Date Type
				#

				date_convert_map = {
					"ohlcv_bar": [("date", "date")],
					"options": [("date", "date"), ("expiration", "expiration")],
					"dividend": [("ex_date", "ex_date")],
					"fred": [("fred_date", "fred_date")]
				}
				date_convert = date_convert_map.get(dataset, [])

				for col_name, new_name in date_convert:
					if col_name in df.columns:
						df[new_name] = pd.to_datetime(df[col_name], unit="ns").dt.normalize()

				return df

			except Exception as e:
				logger.error(
					f"Failed parquet extraction for symbol={symbol}, dataset={dataset}: {str(e)} [prep_bopm_data]"
				)
				logger.debug(f"Stack Trace:\n{traceback.format_exc()}")
				raise

		def construct_div_periods (div_df):
			"""
			Construct time intervals representing dividend distribution periods per ex-dividend dates.
			Args:
				div_df (DataFrame): Historical dividend data which includes ex_date and cash_amount
			Returns:
				Dataframe originally input with start_date and end_date columns denoting periods.
			"""

			try:
				logger.info(f"Constructing dividend periods for {symbol} [prep_bopm_data]")
				if div_df.empty:
					logger.warning("No dividend data [prep_bopm_data]")
					return None
				cols_required = ["symbol", "ex_date", "cash_amount"]
				cols_missing = [c for c in cols_required if c not in div_df.columns]
				if cols_missing:
					logger.error(f"Missing dividend columns: {cols_missing} [prep_bopm_data]")
					raise ValueError(f"Missing columns {cols_missing} from dividend dataframe [prep_bopm_data]")
					return None

				#
				# Define Window Specs
				# Note - we define current period's start_date using the previous period's ex-dividend date
				#

				div_df["ex_date"] = pd.to_datetime(div_df["ex_date"])
				div_df = div_df.sort_values(by="ex_date", ascending=True).reset_index(drop=True)
				div_df["prev_ex_date"] = div_df["ex_date"].shift(1)
				div_df["start_date"] = np.where(
					div_df["prev_ex_date"].isna(),
					div_df["ex_date"] - pd.DateOffset(months=3),
					div_df["prev_ex_date"]
				)
				div_df["end_date"] = div_df["ex_date"] - pd.DateOffset(days=1)
				div_df.drop(["frequency", "symbol", "prev_ex_date"], axis=1, inplace=True)

				return div_df

			except Exception as e:
				logger.error(f"Failed dividend period construction for symbol={symbol}: {str(e)} [prep_bopm_data]")
				logger.debug(f"Stack trace:\n{traceback.format_exc()}")
				raise

		def merge_options_dividends (options_df, div_periods):
			"""
			Merge historical option chain data and historical dividend distrib data.
			Each option contract (row) can span one or more dividend periods.

			Args:
				options_df (DataFrame): Options data with columns ['date', 'expiration', 'ttm']
				div_periods (DataFrame): Dividend periods with columns ['start_date', 'end_date', 'cash_amount']

			Returns:
				DataFrame containing historical option chain data and prorated dividend amount
			"""

			try:
				logger.info(f"Merging options and dividends symbol={symbol} [prep_bopm_data]")

				if options_df is None or options_df.empty:
					logger.warning(f"Missing options data symbol={symbol} [merge_options_dividends]")
					return None
				if div_periods is None or div_periods.empty:
					logger.warning(f"Missing dividend data symbol={symbol} [merge_options_dividends]")
					return None

				#
				# Verify options dataframe has requisite schema
				#

				cols_req = ["date", "expiration", "ttm", "midprice", "strike", "call_put"]
				cols_missing = [c for c in cols_req if c not in options_df.columns]
				if cols_missing:
					logger.warning(f"Bad options schema symbol={symbol} [merge_options_dividends]")
					return None

				#
				# Verify dividend dataframe has requisite schema
				#

				cols_req = ["cash_amount", "start_date", "end_date", "ex_date"]
				cols_missing = [c for c in cols_req if c not in div_periods.columns]
				if cols_missing:
					logger.warning(f"Bad dividend schema symbol={symbol} [merge_options_dividends]")
					return None

				#
				# Cross-Join Dividend Data and Historical Option Data
				# 	• This creates all possible combinations of (options,dividends) order pairs in a single data set
				# 	• Each option row is repeated for every row in the dividend data set -> #rows in xjoin = (#options_rows) * (#dividend_rows)
				#

				logger.debug(f"Xjoining options and dividends symbol={symbol} [merge_options_dividends]")

				df_xjoin = options_df.merge(div_periods, how='cross')

				#
				# Filter the Cross-Joined (options,dividends) Dataset for Valid Time Intervals
				# Criteria:
				# 	1. Option Date Falls in Dividend Period:
				# 		dividend.start_date <= option.date <= dividend.end_date
				# 	2. Option Expiration Falls in Dividend Period:
				# 		dividend.start_date <= option.expiration < dividend.end_date
				# 	3. Dividend Period Interval is Subset of Option Life
				# 		option.date <= dividend.start_date < dividend.end_date <= option.expiration
				#

				logger.debug(f"Filtering xjoined (options,dividends) symbol={symbol} [merge_options_dividends]")

				criteria_1 = df_xjoin["date"].between(df_xjoin["start_date"], df_xjoin["end_date"], inclusive="left")
				criteria_2 = df_xjoin["expiration"].between(df_xjoin["start_date"], df_xjoin["end_date"], inclusive="left")
				criteria_3 = (df_xjoin["date"] <= df_xjoin["start_date"]) & (df_xjoin["end_date"] <= df_xjoin["expiration"])

				df_filtered = df_xjoin[criteria_1 | criteria_2 | criteria_3].copy()

				if df_filtered.empty:
					logger.warning("Filtered (options,dividends) dataframe zero rows")
					return None

				#
				# Define Intersection Time Interval of Dividend Period and Option Life, Prorate Dividends
				#

				logger.debug(f"Prorating dividends symbol={symbol} [merge_options_dividends]")

				df_filtered['period_start'] = df_filtered[['date', 'start_date']].max(axis=1)
				df_filtered['period_end'] = df_filtered[['expiration', 'end_date']].min(axis=1)
				df_filtered['period_days'] = (df_filtered['period_end'] - df_filtered['period_start']).dt.days + 1
				df_filtered['period_weight'] = df_filtered['period_days'] / (df_filtered['ttm'] + 1)
				df_filtered['div_weighted'] = df_filtered['cash_amount'] * df_filtered['period_weight']


				#
				# Aggregate Weighted Dividend Amount to Assign Single Dividend Value to Each Option Contract
				#

				logger.debug(f"Aggregating prorated dividends symbol={symbol} [merge_options_dividends]")

				df_grouped = df_filtered.groupby([
					"date", "expiration", "ttm", "midprice", "strike", "call_put", "act_symbol"
				]).agg(
					div_amt=("div_weighted", "sum"),
					div_count=("ex_date", "count")
				).reset_index()

				logger.info(f"Merged (options,dividends) for symbol={symbol}")

				return df_grouped

			except Exception as e:
				logger.error(f"Failed merge (options,dividends) symbol={symbol}: {str(e)} [prep_bopm_data]")
				logger.debug(f"Stack Trace:\n{traceback.format_exc()}")
				raise

		#
		# Verify that the stock symbol pays dividends.
		# If it does not -> Stop.
		#

		lacks_dividends_marker = os.path.join(STOCK_TRADER_MARKET_DATA, f"{symbol}_lacks_dividends.txt")
		if os.path.exists(lacks_dividends_marker):
			logger.warning(f"{symbol} does not pay dividends [prep_bopm_data]")
			logger.info("Done. [prep_bopm_data]")
			return None

		#
		# Define Datasets
		# 	• ohlcv: Closing Prices and Historical Volatility Estimate
		# 	• options: Historical Options Closing Price Data
		# 	• dividend: Historical dividend distribution data including ex dividend dates
		# 	• fred: Historical Federal Reserve interest rate data for risk-free proxy
		#

		try:
			logger.info(f"Reading parquet data for symbol={symbol}")

			# Historical closing price bar data
			df_ohlcv = extract_from_parquet("ohlcv_bar", symbol)

			# Historical option chain data
			df_options = extract_from_parquet("options", symbol)

			# Dividend data + construct dividend distribution intervals
			df_dividends = extract_from_parquet("dividend", symbol)
			df_dividends = construct_div_periods(df_dividends)

			# FRED data + create end-of-month column for joining to merged dataframe
			df_fred = extract_from_parquet("fred")
			if df_fred is not None and not df_fred.empty:
				df_fred["month_end"] = pd.to_datetime(df_fred["fred_date"]) + pd.offsets.MonthEnd(0)

		except Exception as e:
			logger.error(f"Failed parquet read for symbol={symbol}: {str(e)} [prep_bopm_data]")
			logger.debug(f"Stack Trace:\n{traceback.format_exc()}")
			raise

		try:
			#
			# Construct Merged Dataset
			#

			df_merged = merge_options_dividends(df_options, df_dividends)
			df_merged = pd.merge(df_merged, df_ohlcv[["date", "close", "vol_estimate"]], on="date", how="left")

			# Temporary end-of-month column for joining with FRED
			df_merged["month_end"] = pd.to_datetime(df_merged["date"]) + pd.offsets.MonthEnd(0)
			df_merged = pd.merge(df_merged, df_fred, on="month_end", how="left")

			# Clean up final dataframe
			df_merged.drop(columns=["month_end", "fred_date"], inplace=True)
			df_merged.dropna(inplace=True)
			df_merged.sort_values(by=["date", "expiration", "call_put", "strike"], inplace=True)
		except AnalysisException as e:
			logger.error(f"Merge failed symbol={symbol}: {str(e)} [prep_bopm_data]")
			logger.debug(f"Stack Trace:\n{traceback.format_exc()}")
			raise
		except Exception as e:
			logger.error(f"Merged failed symbol={symbol}: {str(e)} [prep_bopm_data]")
			logger.debug(f"Stack Trace:\n{traceback.format_exc()}")
			raise

		try:
			#
			# Store Merged Dataframe as local Parquet for Downstream Use as QuantLib BOPM Input
			#

			fpath_parquet = os.path.join(STOCK_TRADER_MARKET_DATA, f"{symbol}_bopm_data.parquet")
			logger.info(f"Creating bopm parquet symbol={symbol} [prep_bopm_data]")

			df_merged.to_parquet(fpath_parquet, index=False, engine="pyarrow")
			logger.info(f"Created bopm parquet: {fpath_parquet}")
		except Exception as e:
			logger.error(f"Failed merged data parquet create symbol={symbol}: {str(e)} [prep_bopm_data]")
			logger.debug(f"Stack Trace:\n{traceback.format_exc()}")
			raise

		logger.info("Done [prep_bopm_data]")
		if return_df:
			return df_merged

	except Exception as e:
		logger.error(f"BAD {symbol}: {str(e)} [prep_bopm_data]")
		logger.debug(f"Stack Trace\n:{traceback.format_exc()}")
		if return_df:
			return None
		raise