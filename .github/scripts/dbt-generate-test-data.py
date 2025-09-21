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

	data_dir = get_rt_env()
	output_dir = os.path.join(os.environ["STOCK_TRADER_DWH"], "fred_af")

	os.makedirs(output_dir, exist_ok=True)
	output_path = os.path.join(output_dir, "TB3MS.parquet")
	df_fred.to_parquet(output_path, index=False, engine="pyarrow")

	print(f"Data Dir: {data_dir}")
	print(f"Generated {len(df_fred)} FRED records -> {output_path}")
	print(f"Date Range: {dates[0]}...{dates[-1]}")
	print(f"Sample Dates: {dates[:3]}...{dates[-3:]}")

	return len(df_fred)

def create_options_test_data():
	print("Generating options test data...")
	return 0

def create_quotes_test_data():
	print("Generating quotes test data...")
	return 0

def create_dividends_test_data():
	print("Generating dividends test data...")
	return 0

if __name__ == "__main__":
	print("-"*60)
	print("Generating test data for dbt CI Pipeline")
	print("-"*60)

	np.random.seed(41)

	try:
		total_records = 0
		total_records += create_fred_test_data()
		total_records += create_options_test_data()
		total_records += create_quotes_test_data()
		total_records += create_dividends_test_data()

	except Exception as e:
		print(f"\nERROR - test data creation: {str(e)}")
		import traceback
		traceback.print_exc()
		exit(1)

