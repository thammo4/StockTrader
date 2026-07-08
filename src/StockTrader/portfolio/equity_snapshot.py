#
# FILE: `StockTrader/src/StockTrader/portfolio/equity_snapshot.py`
#

import pandas as pd
from datetime import datetime

from StockTrader.settings import logger, today

class EquitySnapshot:
	def __init__ (self, account_client) -> None:
		self._account = account_client

	def snapshot (self) -> dict:
		try:
			bal = self._account.get_account_balance(return_as_series=True)
		except Exception as e:
			logger.error(f"Failed to fetch account balance: {str(e)} [equity_snapshot]")
			raise

		row = {
			"market_date": pd.to_datetime(today).date().isoformat(),
			"snapshot_ts": datetime.now().isoformat(timespec="seconds"),
			"total_equity": float(bal["equity"]),
			"total_cash": float(bal["cash"]),
			"market_value": float(bal["value"]),
			"upnl": float(bal["open_pl"]),
			"pnl": float(bal["close_pl"]),
			"option_requirement": float(bal["option_requirement"])
		}

		logger.info(
			f"Equity snapshot: equity={row['equity']:.2f}, cash={row['cash']:.2f}, value={row['value']:.2f}, upnl={row['upnl']:.2f}, pnl={row['pnl']:.2f}"
		)

		return row
