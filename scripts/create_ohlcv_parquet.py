#
# FILE: `StockTrader/scripts/create_ohlcv_parquet.py`
#

import os
import pandas as pd
import numpy as np
from datetime import timedelta
from StockTrader.tradier import quotes, quotesL
from StockTrader.settings import STOCK_TRADER_MARKET_DATA, logger

#
# Load/Transform Historical OHLCV Bar Data from Tradier -> Store as Parquet
#


def create_ohlcv_parquet(symbol, return_df=False, is_live=False):

    #
    # Format Tradier OHLCV Historical Bar Data for Subsequent Processing
    # 	• Subset OHLCV columns for just date and close
    # 	• Compute + append column for daily closing log-returns
    #   • Compute + append column for annualized rolling volatility estimate (see: Chpt 14, Hull's 'Options, Futures and Other Derivatives')
    # 	• Convert date column to int64 for PySpark
    #

    def format_ohlcv(df):
        df.drop(["open", "high", "low", "volume"], axis=1, inplace=True)
        df["log_return"] = np.log(df["close"]).diff()
        df["vol_estimate"] = df["log_return"].rolling(window=9).std() * np.sqrt(252)  # \sqrt(252) annualizes
        df.dropna(inplace=True)
        df["date"] = pd.to_datetime(df["date"]).astype(np.int64)
        return df

    q = quotesL if is_live else quotes

    fpath = os.path.join(STOCK_TRADER_MARKET_DATA, f"{symbol}_date_range.txt")
    try:
        if not os.path.exists(fpath):
            raise ValueError(f"Missing file: {symbol}_date_range.txt [create_ohlcv_parquet]")

        with open(fpath, "r") as f:
            lines = f.readlines()

        if len(lines) < 2:
            e_msg = f"Bad file: {fpath} [create_ohlcv_parquet]"
            logger.error(e_msg)
            raise ValueError(e_msg)

        #
        # Reads unix timestamp from text file -> creates timestamp
        #

        start_date = pd.to_datetime(int(lines[0].strip()))
        end_date = pd.to_datetime(int(lines[1].strip()))

        #
        # Push the start date back two weeks to accommodate rolling volatility calculation
        # Rolling volatility with window $w$ will produce NaN for the first w-records
        #

        start_date -= timedelta(weeks=2)

        #
        # Fetch Data -> Add Necessary Feature Variables -> Create Parquet
        #

        logger.info(
            f"Creating ohlcv parquet: symbol={symbol}, start={start_date}, end={end_date} [create_ohlcv_parquet]"
        )

        #
        # Retrieve market data from Tradier Quotes endpoint
        #

        df_bars = q.get_historical_quotes(symbol=symbol, start_date=start_date, end_date=end_date)
        if df_bars.empty:
            e_msg = "No data from Tradier [create_ohlcv_parquet]"
            logger.error(e_msg)
            raise ValueError(e_msg)

        #
        # Prepare the dataframe for local storage
        #

        df_bars = format_ohlcv(df_bars)

        #
        # Create local parquet file
        #

        fpath_parquet = os.path.join(STOCK_TRADER_MARKET_DATA, f"{symbol}_ohlcv_bar_data.parquet")
        df_bars.to_parquet(fpath_parquet, index=False)
        logger.info(f"Created parquet file: {fpath_parquet} [create_ohlcv_parquet]")

        if return_df:
            return df_bars
    except ValueError as e_val:
        logger.error(f"ValueError: {e_val} [create_ohlcv_parquet]")
        raise
    except Exception as e:
        logger.error(f"Exception: {e} [create_ohlcv_parquet]")
        raise
