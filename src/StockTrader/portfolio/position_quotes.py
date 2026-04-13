#
# FILE: `StockTrader/src/StockTrader/portfolio/position_quotes.py`
#

import pandas as pd
from StockTrader.settings import logger


class PositionQuotes:
    def __init__(self, quotes_client):
        self._quotes_client = quotes_client

    def add_market_data(self, df_positions: pd.DataFrame) -> pd.DataFrame:
        if df_positions.empty:
            return df_positions

        symbols = df_positions["occ"].tolist()
        df_quotes = self._quotes_client.get_quote_data(symbols)

        if df_quotes.empty:
            logger.warning("No quote data [positions_quotes]")
            df_positions["bid_price"] = None
            df_positions["ask_price"] = None
            df_positions["last_price"] = None
            df_positions["mid_price"] = None
            df_positions["contrac_size"] = 100

            return df_positions

        df_quotes = df_quotes.rename(
            {"symbol": "occ", "bid": "bid_price", "ask": "ask_price", "last": "last_price"}, axis=1
        )
        df_quotes = df_quotes[["occ", "last_price", "bid_price", "ask_price", "contract_size"]]
        df_enriched = df_positions.merge(df_quotes, on="occ", how="left")

        df_enriched["mid_price"] = df_enriched.apply(
            lambda x: (
                x["last_price"]
                if pd.isna(x["bid_price"]) or pd.isna(x["ask_price"])
                else (x["bid_price"] + x["ask_price"]) / 2
            ),
            axis=1,
        )

        df_enriched["contract_size"] = df_enriched["contract_size"].fillna(100)

        logger.info(f"Enriched n={len(df_enriched)} positions with current quote data")

        return df_enriched
