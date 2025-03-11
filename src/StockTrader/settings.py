#
# FILE: `StockTrader/src/StockTrader/settings.py`
# Defines Frequently Referenced Variables
#

import os
import logging
from datetime import datetime, timedelta


#
# Define StockTrader Directory/File Paths
#

STOCK_TRADER_HOME = os.environ.get("STOCK_TRADER_HOME")
STOCK_TRADER_MARKET_DATA = os.environ.get("STOCK_TRADER_MARKET_DATA")
STOCK_TRADER_LOG = os.environ.get("STOCK_TRADER_LOG")


#
# Configure Logging Directory/Format
#

# os.makedirs(STOCK_TRADER_LOG, exist_ok=True)

LOG_FILE = os.path.join(STOCK_TRADER_LOG, "log.log")
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

logger = logging.getLogger(__name__)


#
# Define Commonly Used Dates
#

today = datetime.today().strftime("%Y-%m-%d")
today_last_year = (datetime.today() - timedelta(weeks=52)).strftime("%Y-%m-%d")
today_six_months_ago = (datetime.today() - timedelta(weeks=26)).strftime("%Y-%m-%d")
today_three_months_ago = (datetime.today() - timedelta(weeks=13)).strftime("%Y-%m-%d")
today_last_month = (datetime.today() - timedelta(weeks=4)).strftime("%Y-%m-%d")
today_two_weeks_ago = (datetime.today() - timedelta(weeks=2)).strftime("%Y-%m-%d")
