#
# FILE: `StockTrader/scripts/ingest_tradier_dividends.py`
#

import os
import pandas as pd
from StockTrader.settings import STOCK_TRADER_DWH, logger, today, today_eighteen_months_ago
from utils.dividend_table import dividend_table
from utils.write_atomic import write_parquet_atomic
from airflow.exceptions import AirflowSkipException


def ingest_tradier_dividends(symbol, subdir="dividends_af"):
    logger.info(f"Running dividend ingest, symbol={symbol} [ingest_tradier_dividends]")
    try:
        df = dividend_table(symbol=symbol, start_date=today_eighteen_months_ago)

        if df is None or df.empty:
            logger.warning(f"No dividend data, symbol={symbol} [ingest_tradier_dividends]")
            raise AirflowSkipException(f"No dividend data, symbol={symbol} [ingest_tradier_dividends]")

        df["symbol"] = symbol
        df["created_date"] = today

        #
        # Write output to local parquet, overwrite existing
        #

        dir_landing = os.path.join(STOCK_TRADER_DWH, subdir)
        os.makedirs(dir_landing, exist_ok=True)
        fpath_parquet = os.path.join(dir_landing, f"{symbol}.parquet")

        write_parquet_atomic(df, fpath_parquet)
        logger.info(f"Wrote dividend data, symbol={symbol}, n={len(df)} records [ingest_tradier_dividends]")
    except AirflowSkipException:
        raise
    except Exception as e:
        logger.error(f"Failed dividend ingest, symbol={symbol}: {str(e)} [ingest_tradier_dividends]")
        raise
