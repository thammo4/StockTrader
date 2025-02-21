import os
import logging
import unittest
from datetime import datetime, timedelta
from unittest.mock import patch
import importlib
# from StockTrader.settings import(
# 	STOCK_TRADER_HOME,
# 	STOCK_TRADER_MARKET_DATA,
# 	STOCK_TRADER_LOG,
# 	LOG_FILE,
# 	logger,
# 	today,
# 	today_last_year,
# 	today_six_months_ago,
# 	today_three_months_ago,
# 	today_last_month
# )

class TestSettings (unittest.TestCase):

	@patch('logging.basicConfig')
	@patch.dict(os.environ, {"STOCK_TRADER_HOME":"/mock/home", "STOCK_TRADER_MARKET_DATA":"/mock/data", "STOCK_TRADER_LOG":"/mock/logs"})
	def test_home_path(self, mock_logging):
		import StockTrader.settings
		importlib.reload(StockTrader.settings)
		from StockTrader.settings import (STOCK_TRADER_HOME, STOCK_TRADER_MARKET_DATA, STOCK_TRADER_LOG)
		self.assertEqual(STOCK_TRADER_HOME, "/mock/home")
		self.assertEqual(STOCK_TRADER_MARKET_DATA, "/mock/data")
		self.assertEqual(STOCK_TRADER_LOG, "/mock/logs")

		mock_logging.assert_called()


	def test_dates (self):
		import StockTrader.settings
		importlib.reload(StockTrader.settings)
		from StockTrader.settings import (
			today,
			today_last_year,
			today_six_months_ago,
			today_three_months_ago,
			today_last_month
		)

		dt = datetime.today()
		today_expected = dt.strftime("%Y-%m-%d")
		last_year_expected = (dt-timedelta(weeks=52)).strftime("%Y-%m-%d")
		six_months_ago_expected = (dt-timedelta(weeks=26)).strftime("%Y-%m-%d")
		three_months_ago_expected = (dt-timedelta(weeks=13)).strftime("%Y-%m-%d")
		last_month_expected = (dt-timedelta(weeks=4)).strftime("%Y-%m-%d")

		self.assertEqual(today, today_expected)
		self.assertEqual(today_last_year, last_year_expected)
		self.assertEqual(today_six_months_ago, six_months_ago_expected)
		self.assertEqual(today_three_months_ago, three_months_ago_expected)
		self.assertEqual(today_last_month, last_month_expected)



if __name__ == '__main__':
	unittest.main()