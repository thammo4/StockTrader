#
# FILE: `StockTrader/src/StockTrader/portfolio/equity_snapshot.py`
#

import pandas as pd
from datetime import datetime
from StockTrader.settings import logger, today


class EquitySnapshot:
    def __init__(self, account_client) -> None:
        self._account = account_client
        self.columns_keep = ["total_equity", "total_cash", "market_value", "open_pl", "close_pl", "option_requirement"]
        self.columns_rename = {
            "total_equity": "equity",
            "total_cash": "cash",
            "market_value": "value",
            "open_pl": "upnl",
            "close_pl": "pnl",
            "option_requirement": "option_req",
        }
        self.columns_return = ["market_date", "snapshot_ts", "equity", "cash", "value", "upnl", "pnl", "option_req"]

    def load_equity_snapshot(self) -> pd.DataFrame:
        try:
            bal = self._account.get_account_balance()
            df = bal[self.columns_keep].rename(self.columns_rename, axis=1)
            df = df.astype(float)

            df["market_date"] = pd.to_datetime(today).date().isoformat()
            df["snapshot_ts"] = datetime.now().isoformat(timespec="seconds")

            equity = df["equity"].iloc[0]
            cash = df["cash"].iloc[0]
            value = df["value"].iloc[0]

            logger.info(
                f"Account Snapshot: equity={equity:.2f}, cash={equity:.2f}, value={value:.2f} [equity_snapshot]"
            )

            return df[self.columns_return]

        except Exception as e:
            logger.error(f"Failed to fetch acct bal: {str(e)} [equity_snapshot]")
            return pd.DataFrame(columns=self.columns_return)
