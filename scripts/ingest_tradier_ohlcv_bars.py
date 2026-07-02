#
# FILE: `StockTrader/scripts/ingest_tradier_ohlcv_bars.py`
#

import os
import pandas as pd
from StockTrader.tradier import quotesL
from StockTrader.settings import STOCK_TRADER_DWH, logger, today, today_last_year
from utils.write_atomic import write_parquet_atomic
from airflow.exceptions import AirflowSkipException


#
# Retrieve + Store historical OHLCV bar data from Tradier for a given ticker symbol
#


def ingest_tradier_ohlcv_bars(symbol, subdir="ohlcv_bars", start_date=None, end_date=None):
    """
    Ingest daily OHLCV bar data for a symbol from Tradier API.

    By default, performs incremental load: checks latest date in existing parquet file
    and fetches all missing data up to today. This ensures no gaps if DAG didn't run.
    For backfills, specify start_date and end_date.

    Args:
        symbol: Stock ticker symbol
        subdir: Subdirectory within warehouse for storage (default: ohlcv_bars)
        start_date: Start date in YYYY-MM-DD format (default: incremental from last available date)
        end_date: End date in YYYY-MM-DD format (default: today)
    """
    try:
        #
        # Define the landing directory and parquet file path
        #

        dir_landing = os.path.join(STOCK_TRADER_DWH, subdir)
        os.makedirs(dir_landing, exist_ok=True)
        fpath_parquet = os.path.join(dir_landing, f"{symbol}.parquet")

        #
        # Set date range for incremental loads
        # If file exists, fetch from day after latest date to today
        # If file doesn't exist, fetch last year of data
        #

        if start_date is None:
            if os.path.exists(fpath_parquet):
                df_existing = pd.read_parquet(fpath_parquet)
                if "date" in df_existing.columns and not df_existing.empty:
                    latest_date = pd.to_datetime(df_existing["date"]).max()
                    start_date = (latest_date + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
                    logger.info(
                        f"Found existing data up to {latest_date.strftime('%Y-%m-%d')}, fetching from {start_date} [ingest_tradier_ohlcv_bars]"
                    )
                else:
                    start_date = today_last_year
                    logger.info(
                        f"Existing file has no date column, fetching from {start_date} [ingest_tradier_ohlcv_bars]"
                    )
            else:
                start_date = today_last_year
                logger.info(f"No existing file, fetching from {start_date} [ingest_tradier_ohlcv_bars]")

        if end_date is None:
            end_date = today

        #
        # Check if we're already up to date (start_date > end_date)
        #

        if start_date > end_date:
            logger.info(f"Already up to date: symbol={symbol}, latest_date={start_date} [ingest_tradier_ohlcv_bars]")
            raise AirflowSkipException(f"Already up to date, symbol={symbol}")

        #
        # Retrieve daily bar data from Tradier
        #

        logger.info(
            f"Fetching OHLCV data: symbol={symbol}, start={start_date}, end={end_date} [ingest_tradier_ohlcv_bars]"
        )
        df = quotesL.get_historical_quotes(symbol=symbol, interval="daily", start_date=start_date, end_date=end_date)

        if df is None or df.empty:
            logger.info(f"No OHLCV data, symbol={symbol} [ingest_tradier_ohlcv_bars]")
            raise AirflowSkipException(f"No OHLCV data, symbol={symbol} [ingest_tradier_ohlcv_bars]")

        #
        # Ensure numeric columns are properly typed
        #

        for col in ["open", "high", "low", "close"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        df["created_date"] = today
        df["symbol"] = symbol

        #
        # Load the OHLCV data by appending/creating the parquet file
        #

        if os.path.exists(fpath_parquet):
            df_existing = pd.read_parquet(fpath_parquet)
            df = pd.concat([df_existing, df])
            logger.info(
                f"Appending to existing data: symbol={symbol}, n_existing={len(df_existing)}, n_new={len(df)-len(df_existing)} [ingest_tradier_ohlcv_bars]"
            )

        #
        # Deduplicate based on symbol, date, and created_date
        #

        if "date" in df.columns:
            df.drop_duplicates(subset=["symbol", "date", "created_date"], keep="last", inplace=True)
        else:
            df.drop_duplicates(subset=["symbol", "created_date"], keep="last", inplace=True)

        write_parquet_atomic(df, fpath_parquet)
        logger.info(f"Loaded OHLCV bars, symbol={symbol}, n={len(df)} [ingest_tradier_ohlcv_bars]")

    except AirflowSkipException:
        raise

    except Exception as e:
        logger.warning(f"Skipping symbol={symbol}: {str(e)} [ingest_tradier_ohlcv_bars]")
        raise AirflowSkipException(f"No data, symbol={symbol}: {str(e)}")
