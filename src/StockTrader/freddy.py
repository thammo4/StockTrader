#
# FILE: `StockTrader/src/StockTrader/freddy.py`
# Configure API connection to Federal Reserve Economic Data
#
#

import os
import dotenv
from fredapi import Fred
from StockTrader.settings import STOCK_TRADER_HOME

#
# Load API Key from Root Directory .env File
#

env_path = os.path.join(STOCK_TRADER_HOME, ".env")
dotenv.load_dotenv(env_path)

fred_api_key = os.getenv("fred_api_key")
fred = Fred(api_key=fred_api_key)