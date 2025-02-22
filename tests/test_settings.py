#
# FILE: `StockTrader/tests/test_settings.py` (test: `StockTrader/src/settings.py`)
# 	• Confirm that directory paths are correctly specified.
# 	• Confirm that the dates are correctly defined.
#

import os
import logging
import importlib
import pytest
from datetime import datetime, timedelta
from unittest.mock import patch

@pytest.fixture
def settings ():
	import StockTrader.settings
	importlib.reload(StockTrader.settings)
	return StockTrader.settings

@pytest.fixture(autouse=True)
def mock_env ():
	with patch.dict(os.environ, {"STOCK_TRADER_HOME":"/mock/home", "STOCK_TRADER_MARKET_DATA":"/mock/data", "STOCK_TRADER_LOG":"/mock/logs"}):
		yield

def test_paths ():
	'''Test that directory paths are correctly specified'''
	with patch('logging.basicConfig') as mock_log:
		from StockTrader.settings import (
			STOCK_TRADER_HOME,
			STOCK_TRADER_MARKET_DATA,
			STOCK_TRADER_LOG
		)
		assert STOCK_TRADER_HOME == "/mock/home"
		assert STOCK_TRADER_MARKET_DATA == "/mock/data"
		assert STOCK_TRADER_LOG == "/mock/logs"
		mock_log.assert_called()

def test_dates (settings):
	'''Test that dates are correctly defined'''
	dt = datetime.today()
	today_expected = dt.strftime('%Y-%m-%d')
	last_year_expected = (dt-timedelta(weeks=52)).strftime("%Y-%m-%d")
	six_months_ago_expected = (dt-timedelta(weeks=26)).strftime("%Y-%m-%d")
	three_months_ago_expected = (dt-timedelta(weeks=13)).strftime("%Y-%m-%d")
	last_month_expected = (dt-timedelta(weeks=4)).strftime("%Y-%m-%d")

	assert settings.today == today_expected
	assert settings.today_last_year == last_year_expected
	assert settings.today_six_months_ago == six_months_ago_expected
	assert settings.today_three_months_ago == three_months_ago_expected
	assert settings.today_last_month == last_month_expected