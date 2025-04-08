#
# FILE: `StockTrader/scripts/create_fred_parquet.py`
#

import os
import pandas as pd
import numpy as np
from StockTrader.freddy import fred
from StockTrader.settings import STOCK_TRADER_MARKET_DATA, today, today_last_year, logger


#
# Load/Transform Interest Rates from FRED to Use as Risk-Free Rate Proxy -> Store as Parquet
#


def create_fred_parquet(series="TB3MS", start_date=None, end_date=None, return_df=False):

    #
    # Format Fred Rates Data
    # 	• Remove index
    # 	• Define date and interest rate columns
    # 	• Convert date column to int64 for PySpark
    #

    def format_fred(df):
        df = df.reset_index()
        df.columns = ["fred_date", "fred_rate"]
        df["fred_date"] = pd.to_datetime(df["fred_date"]).astype(np.int64)
        # fred gives units in percents and we need the decimal version for options pricing
        # see: fred.search('TB3MS').iloc[0]
        df["fred_rate"] /= 100
        return df

    #
    # Specify Date Range
    # By default, we use the last 12 months
    #

    if start_date is None:
        start_date = today_last_year
    if end_date is None:
        end_date = today

    try:
        logger.info(
            f"Creating parquet file: seriesid={series}, start={start_date}, end={end_date} [create_fred_parquet]"
        )

        #
        # Retrieve Data from FRED API
        #

        df_fred = fred.get_series(series_id=series, observation_start=start_date, observation_end=end_date)
        if df_fred.empty:
            e_msg = "Ghosted by FRED [create_fred_parquet]"
            logger.error(e_msg)
            raise ValueError(e_msg)

        #
        # Prepare dataframe for local parquet storage
        #

        df_fred = format_fred(df_fred)

        #
        # Create local parquet file
        #

        fpath_parquet = os.path.join(STOCK_TRADER_MARKET_DATA, "fred_data.parquet")
        df_fred.to_parquet(fpath_parquet, index=False)
        logger.info(f"Created parquet file: {fpath_parquet} [create_fred_parquet]")

        #
        # Ask, and ye shall receive
        #

        if return_df:
            return df_fred

    #
    # Configure that shit i dont like
    #

    except ValueError as e_val:
        logger.error(f"ValueError: {e_val} [create_fred_parquet]")
        raise
    except Exception as e:
        logger.error(f"Exception: {e} [create_fred_parquet]")
        raise
