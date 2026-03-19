#
# FILE: `StockTrader/scripts/ingest_tradier_options.py`
#

import os
import pandas as pd
from StockTrader.tradier import options_dataL
from StockTrader.settings import STOCK_TRADER_DWH, logger, today
from utils.write_atomic import write_parquet_atomic
from airflow.exceptions import AirflowSkipException


#
# Retrieve + Store today's options chain from Tradier for a given ticker symbol
#

def ingest_tradier_options(symbol, subdir="options_af"):
    try:
        #
        # Retrieve the day's options chain data from Tradier
        #

        df = options_dataL.get_chain_all(symbol=symbol)
        if df is None or df.empty:
            logger.info(f"No options data, symbol={symbol} [ingest_tradier_options]")
            raise AirflowSkipException(f"No options data, symbol={symbol} [ingest_tradier_options]")
        df["created_date"] = today

        #
        # Define the landing directory and parquet file
        #

        dir_landing = os.path.join(STOCK_TRADER_DWH, subdir)
        os.makedirs(dir_landing, exist_ok=True)
        fpath_parquet = os.path.join(dir_landing, f"{symbol}.parquet")

        #
        # Load the options chain data by appending/creating the parquet file
        #

        if os.path.exists(fpath_parquet):
            df_existing = pd.read_parquet(fpath_parquet)
            df = pd.concat([df_existing, df]).drop_duplicates(subset=["symbol", "created_date"])

        write_parquet_atomic(df, fpath_parquet)
        logger.info(f"Loaded options chain, symbol={symbol} [ingest_tradier_options]")

    except AirflowSkipException:
        raise

    #
    # Exception defined to catch bad/delisted symbols per uvatradier `get_chain_day` error handling
    #

    except Exception as e:
        # logger.error(f"Ingest failed, symbol={symbol}: {str(e)} [ingest_tradier_options]")
        # raise
        logger.warning(f"Skipping symbol={symbol}: {str(e)} [ingest_tradier_options]")
        raise AirflowSkipException(f"No data, symbol={symbol}: {str(e)}")
