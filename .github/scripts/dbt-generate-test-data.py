#
# FILE: `StockTrader/.github/scripts/dbt-generate-sample-data.py`
#

import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def create_fred_test_data():
	pass

def create_options_test_data():
	pass

def create_quotes_test_data():
	pass

def create_dividends_test_data():
	pass

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

