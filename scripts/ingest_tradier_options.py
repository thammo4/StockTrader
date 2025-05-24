#
# FILE: `StockTrader/scripts/ingest_tradier_options.py`
#

import os
import pandas as pd
from StockTrader.tradier import options_data
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
# Retrieve + Store today's options chain from Tradier for a given ticker symbol
#


def ingest_tradier_options(symbol, subdir="options_af"):
    try:
        #
        # Retrieve the day's options chain data from Tradier
        #

        df = options_data.get_chain_all(symbol=symbol)
        if df.empty:
            logger.warning(f"No options data: {symbol}")
            raise AirflowSkipException(f"No options data, symbol={symbol}")
            # return
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

        df.to_parquet(fpath_parquet, index=False, engine="pyarrow")
        logger.info(f"Loaded options chain, symbol={symbol}")

    except AirflowSkipException:
        raise
    except Exception as e:
        logger.error(f"Ingest failed, symbol={symbol}: {str(e)}")
        raise
