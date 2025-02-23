#
# FILE: `StockTrader/tests/scripts/test_dolt_csv_to_parquet.py` (test: `StockTrader/scripts/dolt_csv_to_parquet.py`)
# 	• Verify File I/O
# 	• Test correct date formatting
# 	• Validate parquet and date-text file outputs
# 	• Validate error handling
#

import os
import pytest
import logging
import pandas as pd
import numpy as np
from unittest.mock import patch
from scripts.dolt_csv_to_parquet import dolt_csv_to_parquet


STOCK_TRADER_MARKET_DATA = os.environ.get("STOCK_TRADER_MARKET_DATA")

@pytest.fixture
def create_test_data ():
	'''
		Create test dataframe with sample options_chain data to mock real CSV
		As with the actual script, date and expiration columns map from strings -> unix ts
	'''
	df = pd.DataFrame({
		'date': ['2024-02-03', '2024-02-03'],
		'expiration': ['2024-03-08', '2024-03-08'],
		'ttm': [14, 14],
		'midprice': [46.550, 0.200],
		'strike': [115.0, 115.0],
		'call_put': ['Call', 'Put'],
		'act_symbol': ['PG', 'PG']
	})
	df['date'] = pd.to_datetime(df['date']).astype(np.int64)
	df['expiration'] = pd.to_datetime(df['expiration']).astype(np.int64)
	return df


def test_csv_to_parquet (create_test_data):
	'''
		Test functionality of dolt_csv_to_parquet:
			• Read CSV data
			• Map date/expiration from strings -> unix ts
			• Validate parquet file in data directory
			• Validate the date range text file used for downstream processing
	'''
	fpath_parquet = f"{STOCK_TRADER_MARKET_DATA}/PG_options_data.parquet"
	fpath_date_range = f"{STOCK_TRADER_MARKET_DATA}/PG_date_range.txt"

	#
	# Mock pandas.read_csv() to avoid needing to create actual CSV
	#

	with patch('pandas.read_csv', return_value=create_test_data):
		result = dolt_csv_to_parquet('PG', return_df=True)

	#
	# Validate that date and expiration are unix timestamps...
	#

	assert result['date'].dtype == np.int64
	assert result['expiration'].dtype == np.int64

	#
	# Validate Parquet file creation...
	#

	assert os.path.exists(fpath_parquet), "Parquet file not created"


	#
	# Validate date range text file creation...
	#


	assert os.path.exists(fpath_date_range), "Date range file not created"

	#
	# Validate date range text file contents...
	#

	with open(fpath_date_range, "r") as f:
		lines = f.readlines()
		assert lines[0].strip() == str(create_test_data.iloc[0]['date'])
		assert lines[1].strip() == str(create_test_data.iloc[-1]['date'])

	#
	# Clean it up
	#

	os.remove(fpath_parquet)
	os.remove(fpath_date_range)


def test_csv_to_parquet_file_missing (caplog):
	'''
	Test that error is logged when input CSV not available.
	Use test symbol JUNK because no self-respecting company would use that ticker symbol and it will always be invalid
	'''

	with caplog.at_level(logging.ERROR):
		dolt_csv_to_parquet('JUNK')

	assert "ERROR [dolt_csv_to_parquet]" in caplog.text