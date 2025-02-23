#
# FILE: `StockTrader/tests/config.py`
# Pre-define variables commonly used by test modules
#

import os
import logging

STOCK_TRADER_LOG = os.environ.get("STOCK_TRADER_LOG")

#
# Configure Logging Directory/Format
#

LOG_FILE = os.path.join(STOCK_TRADER_LOG, "test_log.log")
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.DEBUG,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H-%M-%S",
)

logger = logging.getLogger(__name__)