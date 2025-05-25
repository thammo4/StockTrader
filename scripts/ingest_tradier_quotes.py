#
# FILE: `StockTrader/scripts/ingest_tradier_quotes.py`
#

import os
import pandas as pd
from StockTrader.tradier import quotes
from StockTrader.settings import STOCK_TRADER_DWH, logger, today
from airflow.exceptions import AirflowSkipException


def get_symbols():
    path = "/opt/stocktrader/largecap_all.txt"
    if os.path.exists(path):
        return pd.read_table(path, header=None)[0].to_list()
    else:
        logger.warning(f"No symbol list: {path}")
        return []


#
# Retrieve + Store today's quote data from Tradier for all largecap_all symbols
#


def ingest_tradier_quotes(subdir="quotes_af"):
    #
    # Retrieve tickers in symbol space
    #

    symbol_list = get_symbols()
    if not symbol_list:
        logger.warning("Missing symbol list [ingest_tradier_quotes]")
        return
    try:
        #
        # Retrieve the day's quote data from Tradier
        #

        df = quotes.get_quote_data(symbol_list)
        if df.empty:
            logger.warning(f"No quote data, |symbols|={len(symbol_list)}")
            return
        df["created_date"] = today

        #
        # Define the landing directory
        #

        dir_landing = os.path.join(STOCK_TRADER_DWH, subdir)
        os.makedirs(dir_landing, exist_ok=True)

        #
        # Iterate over each symbol and create or append parquet data
        # Note:
        # 	• if a symbol in largecap_all does not contain data, that row will be missing from the dataset returned from Tradier
        # 	• ergo, we define symbol_list_received in order to only append for symbols for which we have data
        #

        symbol_list_received = df["symbol"].to_list()

        for symbol in symbol_list_received:
            df_symbol = df[df["symbol"] == symbol].copy()
            fpath_parquet = os.path.join(dir_landing, f"{symbol}.parquet")

            #
            # Check for existing data
            #

            if os.path.exists(fpath_parquet):
                df_existing = pd.read_parquet(fpath_parquet)
                df_symbol = pd.concat([df_existing, df_symbol])

            #
            # Load the quote data
            #

            df_symbol.to_parquet(fpath_parquet, index=False, engine="pyarrow")
            logger.info(f"Loaded quote, symbol={symbol}")

    except AirflowSkipException:
        raise
    except Exception as e:
        logger.error(f"Ingest failed, symbol={symbol}: {str(e)} [ingest_tradier_quotes]")
        raise
