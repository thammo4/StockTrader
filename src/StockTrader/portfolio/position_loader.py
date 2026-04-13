#
# FILE: `StockTrader/src/StockTrader/portfolio/position_loader.py`
#

# from __future__import annotations
import pandas as pd
from StockTrader.settings import logger

class PositionLoader:
	def __init__ (self, account_client) -> None:
		self._account = account_client

	def load_options_positions(self) -> pd.DataFrame:
		try:
			df = self._account.get_positions(options=True)
			if df is None or df.empty:
				return pd.DataFrame(columns=["occ", "quantity", "cost_basis", "date_acquired", "id"])

			df["quantity"] = pd.to_numeric(df["quantity"])
			df["cost_basis"] = pd.to_numeric(df["cost_basis"])

			logger.info(f"Loaded n={len(df)} open positions")

			return df.rename({"symbol":"occ", "id":"tradier_id"}, axis=1)
		except Exception as e:
			logger.error(f"Failed to load positions: {str(e)}")
			return pd.DataFrame()
