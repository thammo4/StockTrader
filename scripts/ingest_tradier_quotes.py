#
# FILE: `StockTrader/scripts/ingest_tradier_quotes.py`
#

import os
import pandas as pd
from StockTrader.tradier import quotesL
from StockTrader.settings import STOCK_TRADER_DWH, logger, today
from utils.get_symbols import get_symbols
from utils.write_atomic import write_parquet_atomic
from airflow.exceptions import AirflowSkipException

#
# Retrieve + Store today's quote data from Tradier for all largecap_all symbols
#

def ingest_tradier_quotes(subdir="quotes_af", fpath=None):

    #
    # Retrieve tickers in symbol space
    #

    symbol_list = get_symbols() if fpath is None else get_symbols(fpath)
    if not symbol_list:
        logger.warning("Missing symbol list [ingest_tradier_quotes]")
        return

    logger.info(f"Starting Tradier quotes ingest, n={len(symbol_list)} symbols [ingest_tradier_quotes]")
    try:

        #
        # Retrieve the day's quote data from Tradier
        #

        df = quotesL.get_quote_data(symbol_list)
        if df.empty:
            logger.warning(f"No quote data, n={len(symbol_list)} [ingest_tradier_quotes]")
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

            logger.info(f"Inserting n={len(df_symbol)} new record, symbol={symbol} [ingest_tradier_quotes]")


            #
            # Check for existing data
            #

            if os.path.exists(fpath_parquet):
                df_existing = pd.read_parquet(fpath_parquet)
                df_symbol = pd.concat([df_existing, df_symbol])
                logger.info(f"Found existing data: symbol={symbol}, n0={len(df_existing)} [ingest_tradier_quotes]")
            else:
                logger.info(f"Creating new parquet file: symbol={symbol}, n={len(df_symbol)}, fpath={fpath_parquet} [ingest_tradier_quotes]")


            #
            # Load the quote data
            #

            write_parquet_atomic(df_symbol, fpath_parquet)

        logger.info(f"Done. [ingest_tradier_quotes]")

    except AirflowSkipException:
        raise
    except Exception as e:
        logger.error(f"Yikes! {str(e)} [ingest_tradier_quotes]")
        logger.error(f"Symbol list={symbol_list} [ingest_tradier_quotes]")
        raise
