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
	print("Generating FRED interest rate data...")

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

		# dates.append(fred_date, strftime("%Y-%m-%d"))
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

	return len(df_fred)

def create_options_test_data():
	print("Generating options test data...")
	return 0

def create_quotes_test_data():
	print("Generating quotes test data...")

	def create_symbol_data (s):
		data = []
		base_price = base_prices.get(s)

		price_range = 0.02 * base_price
		price_low = base_price - .5*price_range
		price_high = base_price + .5*price_range
		price_open = np.random.uniform(price_low+0.10*price_range, price_high-0.10*price_range)
		price_close = np.random.uniform(price_low+0.10*price_range, price_high-0.10*price_range)
		price_last = price_close

		bid_ask_spread = base_price * np.random.uniform(0.0001,0.0005)
		price_bid = price_last - 0.50*bid_ask_spread
		price_ask = price_last + 0.50*bid_ask_spread

		volume = np.random.randint(1000000,10000000)
		# volume_avg = int(volume + np.random.uniform(0.80, 1.20))
		volume_avg = int(volume * np.random.uniform(0.80, 1.20))
		volume_last = 100*np.random.randint(100,1000)

		# price_close_prev = base_price * np.random.uniform(8.99,1.01)
		price_close_prev = base_price * np.random.uniform(0.99, 1.01)
		price_change = price_last - price_close_prev
		price_change_pct = 100*(price_change/price_close_prev) if price_close_prev > 0 else 1.0

		week_52_high = base_price * np.random.uniform(1.125,1.375)
		week_52_low = base_price * np.random.uniform(0.625,0.825)

		current_time = int(datetime.now().timestamp())
		trade_date = current_time
		bid_date = current_time - np.random.randint(0,240)
		ask_date = current_time - np.random.randint(0,240)

		bid_size = 100 * np.random.randint(1,20)
		ask_size = 100 * np.random.randint(1,20)

		data = [{
			"symbol": s,
			"description": base_descriptions.get(s),
			"exch": base_exchanges.get(s),
			"type": "stock",
			"last": round(price_last,2),
			"change": round(price_change,2),
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
		}]

		return pd.DataFrame(data)









	n_records = 0

	symbols = ["AAPL", "KO", "PG", "C", "XOM"]

	prices = [12.5*x for x in range(50,75,5)]
	exchanges = ["D", "U", "C", "N", "Z"]
	descriptions = ["Apple Inc", "Coca-Cola Co", "Proctor & Gamble Co", "Citigroup Inc", "Exxon Mobil Corp"]
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

	for s in symbols:
		df_symbol_quotes = create_symbol_data(s)

		fpath_parquet = os.path.join(output_dir, f"{s}.parquet")
		df_symbol_quotes.to_parquet(fpath_parquet, index=False, engine="pyarrow")

		print(f"symbol={s}, n={len(df_symbol_quotes)}")

		n_records += len(df_symbol_quotes)

	print(f"Generated {n_records} records")

	return n_records

	return 0

def create_dividends_test_data():
	"""Generate dividend data matching airflow-ingested schema"""
	print("Generating dividends test data...")

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

		print(f"symbol={s}, n={len(df_symbol_dividends)}")

		n_records += len(df_symbol_dividends)

	print(f"Generated {n_records} records")

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

# Generating FRED interest rate data...
# Data Dir: /Users/thammons/Desktop/StockTrader/test_data/warehouse
# Generated 12 FRED records -> /Users/thammons/Desktop/StockTrader/test_data/warehouse/fred_af/TB3MS.parquet
# Date Range: 2025-01-31...2025-12-31
# Sample Dates: ['2025-01-31', '2025-02-28', '2025-03-31']...['2025-10-31', '2025-11-30', '2025-12-31']

# Generating options test data...

# Generating quotes test data...
# Data Dir: /Users/thammons/Desktop/StockTrader/test_data/warehouse
# Output Dir: /Users/thammons/Desktop/StockTrader/test_data/warehouse/quotes_af
# symbol=AAPL, n=1
# symbol=KO, n=1
# symbol=PG, n=1
# symbol=C, n=1
# symbol=XOM, n=1
# Generated 5 records

# Generating dividends test data...
# Data Dir: /Users/thammons/Desktop/StockTrader/test_data/warehouse
# Output Dir: /Users/thammons/Desktop/StockTrader/test_data/warehouse/dividends_af
# symbol=AAPL, n=8
# symbol=KO, n=8
# symbol=PG, n=8
# symbol=C, n=8
# symbol=XOM, n=8
# Generated 40 records

