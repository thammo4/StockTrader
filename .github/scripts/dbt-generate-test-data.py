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
	print("!!! FRED DATA !!!")

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
	print(f"Output Dir: {output_dir}")
	print(f"Schema:"); df_fred.info()

	print(f"\nGenerated {len(df_fred)} records")

	return len(df_fred)

def create_options_test_data():
	print("!!! OPTIONS DATA !!!")

	def create_symbol_data (s):
		data = []
		base_price = base_prices.get(s)

		base_date = datetime.now()
		expiry_dates = [base_date+timedelta(days=x) for x in [7,30,60]]

		for expiry in expiry_dates:
			strike_range = int(0.4 * base_price)
			strike_step = max(2.5, int(0.025*base_price))

			strikes = []
			for i in range(-2,2):
				strike = base_price + strike_step * i
				strikes.append(2.5*max(1,round(strike/2.5)))

			for strike in strikes:
				for option_type in ["call", "put"]:
					expiry_string = expiry.strftime("%y%m%d")
					option_type_char = "C" if option_type == "call" else "P"
					strike_str = f"{int(1000*strike):08d}"
					occ_symbol = f"{s}{expiry_string}{option_type_char}{strike_str}"

					days_to_expiry = (expiry - base_date).days
					time_factor = np.sqrt(days_to_expiry/365.0)

					intrinsic_value = max(0,base_price-strike) if option_type == "call" else max(0,strike-base_price)

					moneyness = abs(base_price-strike)/base_price
					time_value = np.random.uniform(0.25,4.0)*time_factor*(1-moneyness)

					theoretical_price = intrinsic_value + time_value

					bid_ask_spread = max(0.01,0.02*theoretical_price)
					bid_price = max(0.01,theoretical_price - bid_ask_spread/2)
					ask_price = bid_price + bid_ask_spread

					price_last = np.random.uniform(bid_price,ask_price)

					volume_factor = 1 - min(0.8, moneyness*2)
					volume = int(np.random.randint(0,500)*volume_factor)
					open_interest = int(np.random.randint(0,2000)*volume_factor)

					daily_range = price_last * 0.1
					price_low = max(0.01,price_last-daily_range/2)
					price_high = price_last + daily_range/2
					price_open = np.random.uniform(price_low,price_high)
					price_close = price_last

					price_close_prev = price_last * np.random.uniform(0.9,1.1)
					price_change = price_last - price_close_prev
					price_change_pct = 100*(price_change/price_close_prev) if price_close_prev > 0 else 0

					base_time = int(datetime.now().timestamp() * 1000)
					trade_date = base_time - np.random.randint(0,3600) * 1000
					bid_date = base_time - np.random.randint(0,300) * 1000
					ask_date = base_time - np.random.randint(0,300) * 1000

					bid_size = 5 * np.random.randint(1,20)
					ask_size = 5 * np.random.randint(1,20)
					volume_last = 100 * np.random.randint(1,10)

					data.append({
						"symbol": occ_symbol,
						"last": round(price_last,2),
						"change": round(price_change,2),
						"volume": volume,
						"open": round(price_open,2),
						"high": round(price_high,2),
						"low": round(price_low,2),
						"close": round(price_close,2),
						"bid": round(bid_price,2),
						"ask": round(ask_price,2),
						"strike": strike,
						"change_percentage": round(price_change_pct,2),
						"last_volume": volume_last,
						"trade_date": trade_date,
						"prevclose": round(price_close_prev,2),
						"bidsize": bid_size,
						"bidexch": base_exchanges.get(s),
						"bid_date": bid_date,
						"asksize": ask_size,
						"askexch": base_exchanges.get(s),
						"ask_date": ask_date,
						"open_interest": open_interest,
						"option_type": option_type,
						"created_date": datetime.now().strftime("%Y-%m-%d")
					})

		return pd.DataFrame(data)

	n_records = 0
	symbols = ["AAPL", "KO", "PG", "C","XOM"]
	prices = [12.5*x for x in range(50,75,5)]
	exchanges = ["D", "U", "C", "N", "Z"]

	base_prices = dict(zip(symbols, prices))
	base_exchanges = dict(zip(symbols, exchanges))


	data_dir = get_rt_env()
	output_dir = os.path.join(data_dir, "options_af")
	os.makedirs(output_dir, exist_ok=True)

	for s in symbols:
		df_symbol_options = create_symbol_data(s)
		fpath_parquet = os.path.join(output_dir, f"{s}.parquet")
		df_symbol_options.to_parquet(fpath_parquet, index=False, engine="pyarrow")

		n_records += len(df_symbol_options)

	if len(df_symbol_options):
		print("Schema:"); df_symbol_options.info()

	print(f"\nGenerated {n_records} records")

	return n_records

def create_quotes_test_data():
	print("!!! QUOTE DATA !!!")


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

	if len(df_symbol_quotes):
		print("Schema:"); df_symbol_quotes.info()

	print(f"\nGenerated {n_records} records")

	return n_records

def create_dividends_test_data():
	print("!!! DIVIDEND DATA !!!")

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

	if len(df_symbol_dividends):
		print("Schema:"); df_symbol_dividends.info()

	print(f"\nGenerated {n_records} records")

	return n_records

def create_ohlcv_bars_test_data():
	print("!!! OHLCV BARS !!!")

	def create_symbol_data(s):
		data = []
		base_price = base_prices.get(s)

		n_trading_days = 45
		price_current = base_price

		for d in range(n_trading_days):
			daily_return = np.random.normal(0.0005, 0.015)
			price_current = price_current * (1 + daily_return)
			price_current = max(1.0, price_current)

			daily_vol = price_current * np.random.uniform(0.01, 0.03)

			price_open = price_current + np.random.uniform(-daily_vol, daily_vol)
			price_close = price_current + np.random.uniform(-daily_vol, daily_vol)

			price_high = max(price_open,price_close) + np.random.uniform(0,daily_vol/2)
			price_low = min(price_open, price_close) - np.random.uniform(0,daily_vol/2)

			price_low = min(price_low, price_open, price_close)
			price_high = max(price_high, price_open, price_close)

			jitter = abs(price_close-price_open)/price_open
			base_volume = np.random.randint(500000,3000000)
			volume = int(base_volume*(1+jitter*10))

			market_date = datetime.now() - timedelta(days=n_trading_days-d-1)

			data.append({
				"date": market_date,
				"open": round(price_open,2),
				"high": round(price_high,2),
				"low": round(price_low,2),
				"close": round(price_close,2),
				"volume": volume,
				"created_date": datetime.now().strftime("%Y-%m-%d"),
				"symbol": s
			})

			price_current = price_close

		return pd.DataFrame(data)


	n_records = 0
	symbols = ["AAPL", "KO", "PG", "C", "XOM"]
	prices = [12.5*x for x in range(50,75,5)]

	base_prices = dict(zip(symbols,prices))

	data_dir = get_rt_env()
	output_dir = os.path.join(data_dir, "ohlcv_bars")
	os.makedirs(output_dir, exist_ok=True)

	print(f"Data Dir: {data_dir}")
	print(f"Output Dir: {output_dir}")

	for s in symbols:
		df_symbol_bars = create_symbol_data(s)

		fpath_parquet = os.path.join(output_dir, f"{s}.parquet")
		df_symbol_bars.to_parquet(fpath_parquet, index=False, engine="pyarrow")

		n_records += len(df_symbol_bars)

	if len(df_symbol_bars):
		print("Schema:"); df_symbol_bars.info()

	print(f"\nGenerated {n_records} records")

	return n_records

if __name__ == "__main__":
	print("-"*60)
	print("Generating test data for dbt CI Pipeline")
	print("-"*60)

	np.random.seed(41)

	try:
		total_records = 0
		print("")
		total_records += create_dividends_test_data()
		print("-"*40, "\n\n")
		total_records += create_fred_test_data()
		print("-"*40, "\n\n")
		total_records += create_ohlcv_bars_test_data()
		print("-"*40, "\n\n")
		total_records += create_options_test_data()
		print("-"*40, "\n\n")
		total_records += create_quotes_test_data()
		print("-"*40, "\n\n")
		print(f"Created {total_records} total test records")
		print(f"Done")

	except Exception as e:
		print(f"\nERROR - test data creation: {str(e)}")
		import traceback
		traceback.print_exc()
		exit(1)
