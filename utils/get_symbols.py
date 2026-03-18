#
# `StockTrader/utils/get_symbols.py`
#

import os
import pandas as pd
from StockTrader.settings import logger

def get_symbols(path="/opt/stocktrader/largecap_all.txt"):
	if os.path.exists(path):
		return pd.read_table(path, header=None)[0].to_list()
	else:
		logger.warning(f"No symbol list, path={path}")
		return []

