#
# FILE: `StockTrader/scripts/ingest_tradier_dividends.py`
#

import os
import pandas as pd
from StockTrader.settings import STOCK_TRADER_DWH, logger, today, today_eighteen_months_ago
from utils.dividend_table import dividend_table
from airflow.exceptions import AirflowSkipException


def get_symbols():
    path = "/opt/stocktrader/largecap_all.txt"
    if os.path.exists(path):
        return pd.read_table(path, header=None)[0].to_list()
    else:
        logger.warning(f"No symbol list: {path}")
        return []


def ingest_tradier_dividends(symbol, subdir="dividends_af"):
    try:
        df = dividend_table(symbol=symbol, start_date=today_eighteen_months_ago)

        if df is None or df.empty:
            logger.warning(f"No dividend data, symbol={symbol}")
            raise AirflowSkipException(f"No dividend data, symbol={symbol}")

        df["symbol"] = symbol
        df["created_date"] = today

        #
        # Write output to local parquet, overwrite existing
        #

        dir_landing = os.path.join(STOCK_TRADER_DWH, subdir)
        os.makedirs(dir_landing, exist_ok=True)
        fpath_parquet = os.path.join(dir_landing, f"{symbol}.parquet")

        df.to_parquet(fpath_parquet, index=False, engine="pyarrow")
        logger.info(f"Wrote dividend data, symbol={symbol} [{len(df)} records]")
    except AirflowSkipException:
        raise
    except Exception as e:
        logger.error(f"Failed dividend ingest, sybmol={symbol}: {str(e)}")
        raise
