#
# FILE: `StockTrader/scripts/ingest_fred_rate.py`
#

import os
import pandas as pd
from datetime import datetime
from StockTrader.freddy import fred
from StockTrader.settings import STOCK_TRADER_DWH, logger, today, today_last_year
from airflow.exceptions import AirflowSkipException


def ingest_fred_rates(series_id="TB3MS", subdir="fred_af"):
    #
    # Auxillary function to create initial FRED dataset
    #

    def ingest_fred_rates_initial():
        df_fred = fred.get_series(series_id=series_id, observation_start=today_last_year)
        df_fred = df_fred.reset_index()
        df_fred.columns = ["fred_date", "fred_rate"]
        df_fred["created_date"] = today
        df_fred.to_parquet(fpath_parquet, index=False, engine="pyarrow")
        return df_fred

    try:
        #
        # Define landing directory
        #

        dir_landing = os.path.join(STOCK_TRADER_DWH, subdir)
        os.makedirs(dir_landing, exist_ok=True)
        fpath_parquet = os.path.join(dir_landing, f"{series_id}.parquet")

        #
        # If no FRED data exists yet, create the initial upload
        #

        if not os.path.exists(fpath_parquet):
            df_initial = ingest_fred_rates_initial()
            logger.info(f"Initial fred load series={series_id}, n={len(df_initial)}")

        #
        # Define observation_start date for newly published rate data
        # The observation_start date is always YYYY-MM-01
        #

        current_month_start = datetime.today().replace(day=1).strftime("%Y-%m-%d")

        #
        # Retrieve new FRED rate for the current month
        #

        df_rate = fred.get_series(series_id=series_id, observation_start=current_month_start)

        if df_rate.empty:
            logger.info(f"No new FRED data, series={series_id}")
            return

        #
        # If data exists, identify observation_start date
        #

        if os.path.exists(fpath_parquet):
            df_rate = df_rate.reset_index()
            df_rate.columns = ["fred_date", "fred_rate"]
            df_rate["created_date"] = today
            df_existing = pd.read_parquet(fpath_parquet)
            df_concat = pd.concat([df_existing, df_rate]).drop_duplicates(subset=["fred_date"], keep="last")
            df_concat.to_parquet(fpath_parquet, index=False, engine="pyarrow")
            logger.info(f"Added {len(df_concat)-len(df_existing)} new records, series={series_id}")
    except AirflowSkipException:
        raise
    except Exception as e:
        logger.error(f"FRED ingest failed, series={series_id}: {str(e)}")
        raise
