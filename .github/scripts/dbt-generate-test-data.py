#
# FILE: `StockTrader/.github/scripts/dbt-generate-sample-data.py`
#

import os
import calendar
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def get_rt_env():
	if os.environ.get("GITHUB_ACTIONS"):
		return os.environ["STOCK_TRADER_DWH"]
	else:
		return os.path.join(os.environ["STOCK_TRADER_HOME"], "test_data", "warehouse")

def create_fred_test_data():
	"""Generate FED interest rate data for TB3MS series"""
	print("!!! FRED interest rate proxy data !!!")

	dates = []
	rates = []
	start_year = 2025
	base_rate = 4.5

	for m in range(1,13):
		last_day_of_month = calendar.monthrange(start_year, m)[1]
		fred_date = datetime(start_year, m, last_day_of_month)
		seasonality = 0.2*np.sin(m*np.pi/6)
		trend = -0.05*(m/12)
		epsilon = np.random.normal(0,0.15)

		rate = max(0.1, base_rate+seasonality+trend+epsilon)

		dates.append(fred_date.strftime("%Y-%m-%d"))
		rates.append(round(rate,2))

	df_fred = pd.DataFrame({
		"fred_date": dates,
		"fred_rate": rates,
		"created_date": datetime.now().strftime("%Y-%m-%d")
	})

	data_dir = get_rt_env() # data/warehouse/ or test_data/warehouse
	output_dir = os.path.join(data_dir, "fred_af") # data/warehouse/fred_af or test_data/warehouse/fred_af
	os.makedirs(output_dir, exist_ok=True)
	fpath_parquet = os.path.join(output_dir, "TB3MS.parquet") # ~/warehouse/fred_af/TB3MS.parquet

	df_fred.to_parquet(fpath_parquet, index=False, engine="pyarrow")

	print(f"Data Dir: {data_dir}")
	print(f"Generated {len(df_fred)} FRED records -> {fpath_parquet}")
	print(f"Date Range: {dates[0]}...{dates[-1]}")
	print(f"Sample Dates: {dates[:3]}...{dates[-3:]}")
	print("Schema:"); df_fred.info()

	return len(df_fred)

def create_options_test_data():
	print("!!! Daily options chain detailed quote data !!!")
	return 0

def create_quotes_test_data():
	print("!!! Daily detailed quote data !!!")


	def create_symbol_data(s):
		data = []
		base_price = base_prices.get(s)

		#
		# Generate Test Data for 5 Simulated Trading Days
		#

		n_trading_days = 5
		for d in range(n_trading_days):
			daily_drift = base_price * 0.002 * (d-2)
			adjusted_base = base_price + daily_drift

			price_range = 0.02 * adjusted_base
			price_low = adjusted_base - 0.5*price_range
			price_high = adjusted_base + 0.5*price_range
			price_open = np.random.uniform(price_low+0.10*price_range, price_high-0.10*price_range)
			price_close = np.random.uniform(price_low+0.10*price_range, price_high-0.10*price_range)
			price_last = price_close

			bid_ask_spread = adjusted_base * np.random.uniform(0.0001,0.0005)
			price_bid = price_last - 0.5*bid_ask_spread
			price_ask = price_last + 0.5*bid_ask_spread

			volume = np.random.randint(1000000, 10000000)
			volume_avg = int(volume*np.random.uniform(0.80,1.20))
			volume_last = 100 * np.random.randint(100, 1000)

			price_close_prev = adjusted_base * np.random.uniform(0.99, 1.01)
			price_change = price_last - price_close_prev
			price_change_pct = 100 * (price_change/price_close_prev) if price_close_prev > 0 else 0.1

			week_52_high = base_price * np.random.uniform(1.125,1.375)
			week_52_low = base_price * np.random.uniform(0.625, 0.825)

			base_time = int(datetime.now().timestamp() * 1000) - (4-d) * 86400 * 1000
			trade_date = base_time
			bid_date = base_time - np.random.randint(0,240) * 1000
			ask_date = base_time - np.random.randint(0,240) * 1000

			bid_size = 100 * np.random.randint(1,20)
			ask_size = 100 * np.random.randint(1,20)

			data.append({
				"symbol": s,
				"description": base_descriptions.get(s),
				"exch": base_exchanges.get(s),
				"type": "stock",
				"last": round(price_last,2),
				"change": round(price_change, 2),
				"volume": volume,
				"open": round(price_open,2),
				"high": round(price_high,2),
				"low": round(price_low,2),
				"close": round(price_close,2),
				"bid": round(price_bid,2),
				"ask": round(price_ask,2),
				"change_percentage": round(price_change_pct,2),
				"average_volume": volume_avg,
				"last_volume": volume_last,
				"trade_date": trade_date,
				"prevclose": round(price_close_prev,2),
				"week_52_high": round(week_52_high,2),
				"week_52_low": round(week_52_low,2),
				"bidsize": bid_size,
				"bidexch": base_exchanges.get(s),
				"bid_date": bid_date,
				"asksize": ask_size,
				"askexch": base_exchanges.get(s),
				"ask_date": ask_date,
				"root_symbols": base_root_symbols.get(s),
				"created_date": datetime.now().strftime("%Y-%m-%d")
			})
		return pd.DataFrame(data)


	n_records = 0
	symbols = ["AAPL", "KO", "PG", "C", "XOM"]

	prices = [12.5*x for x in range(50,75,5)]
	exchanges = ["D", "U", "C", "N", "Z"]
	descriptions = ["Apple Inc", "Coca-Cola Co", "Procter & Gamble Co", "Citigroup Inc", "Exxon Mobil Corp"]
	root_symbols = ["AAPL", "KO", "PG", "C", "XOM,XOM1,XOM2"]

	base_prices = dict(zip(symbols, prices))
	base_exchanges = dict(zip(symbols, exchanges))
	base_descriptions = dict(zip(symbols, descriptions))
	base_root_symbols = dict(zip(symbols, root_symbols))

	data_dir = get_rt_env()
	output_dir = os.path.join(data_dir, "quotes_af")
	os.makedirs(output_dir, exist_ok=True)

	print(f"Data Dir: {data_dir}")
	print(f"Output Dir: {output_dir}")


	#
	# Quote Data Stored in Parquet Files on Per-Symbol Basis
	#

	for s in symbols:
		df_symbol_quotes = create_symbol_data(s)
		fpath_parquet = os.path.join(output_dir, f"{s}.parquet")
		df_symbol_quotes.to_parquet(fpath_parquet, index=False, engine="pyarrow")

		n_records += len(df_symbol_quotes)


	print(f"Generated {n_records} records")

	print_schema_sample = True
	if print_schema_sample:
		print("Schema:"); df_symbol_quotes.info()
		print_schema_sample = False

	return n_records


def create_dividends_test_data():
	"""Generate dividend data matching airflow-ingested schema"""
	print("!!! Dividend payment data !!!")

	#
	# Helper Function to map: symbol -> dividend test data dataframe
	#

	def create_symbol_data (s):
		data = []
		base_amt = base_dividend.get(s)

		for yr in [2024,2025]:
			quarter_dates = [datetime(yr,2,5),datetime(yr,5,6),datetime(yr,8,5),datetime(yr,11,4)]
			for ex_date in quarter_dates:
				epsilon = np.random.uniform(.950, 1.05)
				div_amt = round(base_amt*epsilon, 2)
				data.append({
					"cash_amount": div_amt,
					"ex_date": ex_date,
					"frequency": 4,
					"symbol": s,
					"created_date": datetime.now().strftime("%Y-%m-%d")
				})

		return pd.DataFrame(data)

	n_records = 0

	symbols = ["AAPL", "KO", "PG", "C", "XOM"]
	dividends = [0.25*x for x in range(1,6)]
	base_dividend = dict(zip(symbols, dividends))

	data_dir = get_rt_env()
	output_dir = os.path.join(data_dir, "dividends_af")
	os.makedirs(output_dir, exist_ok=True)

	print(f"Data Dir: {data_dir}")
	print(f"Output Dir: {output_dir}")

	for s in symbols:
		df_symbol_dividends = create_symbol_data(s)

		fpath_parquet = os.path.join(output_dir, f"{s}.parquet")
		df_symbol_dividends.to_parquet(fpath_parquet, index=False, engine="pyarrow")

		n_records += len(df_symbol_dividends)

	print(f"Generated {n_records} records")

	print_schema_sample = True
	if print_schema_sample:
		print("Schema:"); df_symbol_dividends.info()
		print_schema_sample = False

	return n_records

if __name__ == "__main__":
	print("-"*60)
	print("Generating test data for dbt CI Pipeline")
	print("-"*60)

	np.random.seed(41)

	try:
		total_records = 0
		print("")
		total_records += create_fred_test_data()
		print("")
		total_records += create_options_test_data()
		print("")
		total_records += create_quotes_test_data()
		print("")
		total_records += create_dividends_test_data()

	except Exception as e:
		print(f"\nERROR - test data creation: {str(e)}")
		import traceback
		traceback.print_exc()
		exit(1)



#
# SAMPLE OUTPUT
#

# (venv12) thammons@tom StockTrader % python3.12 .github/scripts/dbt-generate-test-data.py
# ------------------------------------------------------------
# Generating test data for dbt CI Pipeline
# ------------------------------------------------------------

# !!! FRED interest rate proxy data !!!
# Data Dir: /Users/thammons/Desktop/StockTrader/test_data/warehouse
# Generated 12 FRED records -> /Users/thammons/Desktop/StockTrader/test_data/warehouse/fred_af/TB3MS.parquet
# Date Range: 2025-01-31...2025-12-31
# Sample Dates: ['2025-01-31', '2025-02-28', '2025-03-31']...['2025-10-31', '2025-11-30', '2025-12-31']
# Schema:
# <class 'pandas.core.frame.DataFrame'>
# RangeIndex: 12 entries, 0 to 11
# Data columns (total 3 columns):
#  #   Column        Non-Null Count  Dtype  
# ---  ------        --------------  -----  
#  0   fred_date     12 non-null     object 
#  1   fred_rate     12 non-null     float64
#  2   created_date  12 non-null     object 
# dtypes: float64(1), object(2)
# memory usage: 420.0+ bytes

# !!! Daily options chain detailed quote data !!!

# !!! Daily detailed quote data !!!
# Data Dir: /Users/thammons/Desktop/StockTrader/test_data/warehouse
# Output Dir: /Users/thammons/Desktop/StockTrader/test_data/warehouse/quotes_af
# Generated 25 records
# Schema:
# <class 'pandas.core.frame.DataFrame'>
# RangeIndex: 5 entries, 0 to 4
# Data columns (total 28 columns):
#  #   Column             Non-Null Count  Dtype  
# ---  ------             --------------  -----  
#  0   symbol             5 non-null      object 
#  1   description        5 non-null      object 
#  2   exch               5 non-null      object 
#  3   type               5 non-null      object 
#  4   last               5 non-null      float64
#  5   change             5 non-null      float64
#  6   volume             5 non-null      int64  
#  7   open               5 non-null      float64
#  8   high               5 non-null      float64
#  9   low                5 non-null      float64
#  10  close              5 non-null      float64
#  11  bid                5 non-null      float64
#  12  ask                5 non-null      float64
#  13  change_percentage  5 non-null      float64
#  14  average_volume     5 non-null      int64  
#  15  last_volume        5 non-null      int64  
#  16  trade_date         5 non-null      int64  
#  17  prevclose          5 non-null      float64
#  18  week_52_high       5 non-null      float64
#  19  week_52_low        5 non-null      float64
#  20  bidsize            5 non-null      int64  
#  21  bidexch            5 non-null      object 
#  22  bid_date           5 non-null      int64  
#  23  asksize            5 non-null      int64  
#  24  askexch            5 non-null      object 
#  25  ask_date           5 non-null      int64  
#  26  root_symbols       5 non-null      object 
#  27  created_date       5 non-null      object 
# dtypes: float64(12), int64(8), object(8)
# memory usage: 1.2+ KB

# !!! Dividend payment data !!!
# Data Dir: /Users/thammons/Desktop/StockTrader/test_data/warehouse
# Output Dir: /Users/thammons/Desktop/StockTrader/test_data/warehouse/dividends_af
# Generated 40 records
# Schema:
# <class 'pandas.core.frame.DataFrame'>
# RangeIndex: 8 entries, 0 to 7
# Data columns (total 5 columns):
#  #   Column        Non-Null Count  Dtype         
# ---  ------        --------------  -----         
#  0   cash_amount   8 non-null      float64       
#  1   ex_date       8 non-null      datetime64[ns]
#  2   frequency     8 non-null      int64         
#  3   symbol        8 non-null      object        
#  4   created_date  8 non-null      object        
# dtypes: datetime64[ns](1), float64(1), int64(1), object(2)
# memory usage: 452.0+ bytes