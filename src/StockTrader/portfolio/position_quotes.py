#
# FILE: `StockTrader/src/StockTrader/portfolio/position_quotes.py`
#

import pandas as pd
from StockTrader.settings import logger


class PositionQuotes:
    def __init__(self, quotes_client):
        self._quotes_client = quotes_client
        self.columns_keep = [
            'symbol',
            'volume',
            'bid',
            'ask',
            'underlying',
            'bidsize',
            'asksize',
            'open_interest',
            'contract_size',
            'expiration_date',
            'expiration_type',
            'option_type',
            'strike'
        ]
        self.columns_rename = {
            "underlying":"symbol",
            "symbol":"occ",
            "expiration_date":"expiry_date",
            "expiration_type":"expiry_type",
            "strike":"strike_price",
            "bid":"bid_price",
            "ask":"ask_price",
            "bidsize":"bid_size",
            "asksize":"ask_size",
            "contract_size":"n_contracts"
        }
        self.columns_return = [
            "symbol",
            "occ",
            "option_type",
            "expiry_date",
            "expiry_type",
            "n_contracts",
            "strike_price",
            "mid_price",
            "bid_price",
            "ask_price",
            "volume",
            "open_interest",
            "bid_size",
            "ask_size",
            "quantity",
            "cost_basis",
            "acq_date",
            "acq_time",
            "tradier_id"
        ]

    def add_market_data(self, df_positions: pd.DataFrame) -> pd.DataFrame:
        print(f'df_positions\n{df_positions}')
        if df_positions.empty:
            return df_positions

        positions_symbols = df_positions["occ"].tolist()
        df_quotes = self._quotes_client.get_quote_data(positions_symbols)

        if df_quotes.empty:
            return pd.DataFrame(columns=self.columns_return)

        df_quotes = df_quotes[self.columns_keep].rename(self.columns_rename, axis=1)
        print(f'df_quotes\n{df_quotes}\n{df_quotes.info()}')
        df_quotes["mid_price"] = df_quotes.apply(
            lambda x: .5*(x["bid_price"] + x["ask_price"]),
            axis=1
        )

        df_enriched = df_positions.merge(df_quotes, on="occ", how="left")
        print(f'df_enriched\n{df_enriched}\n{df_enriched.info()}')

        logger.info(f"Enriched n={len(df_enriched)} positions with current quote data [position_quotes]")

        return df_enriched[self.columns_return]
