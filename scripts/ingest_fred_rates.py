#
# FILE: `StockTrader/scripts/ingest_fred_rates.py`
#

import os
import pandas as pd
from datetime import datetime
from StockTrader.freddy import fred
from StockTrader.settings import STOCK_TRADER_DWH, logger, today, today_last_year
from airflow.exceptions import AirflowSkipException


def ingest_fred_rates(series_id="TB3MS", subdir="fred_af"):

    #
    # Aux Function - Atomic Writes Using Local Temp File
    #

    def write_parquet_atomic(df, fpath_target):
        fpath_tmp = fpath_target + ".tmp"
        try:
            df.to_parquet(fpath_tmp, index=False, engine="pyarrow")
            os.replace(fpath_tmp, fpath_target)
        except Exception:
            if os.path.exists(fpath_tmp):
                os.remove(fpath_tmp)
            raise

    #
    # Aux Function - Retrieve FRED API Data + Prepare DataFrame
    #

    def fetch_fred_df(observation_start):
        df_api = fred.get_series(series_id=series_id, observation_start=observation_start)

        if df_api is None or df_api.empty:
            return None

        df = df_api.reset_index()
        df.columns = ["fred_date", "fred_rate"]
        df["created_date"] = today

        return df

    logger.info("Starting FRED ingest for rate data, series={series_id} [ingest_fred_rates]")
    try:
        #
        # Define local landing directory/filepath
        #

        dir_landing = os.path.join(STOCK_TRADER_DWH, subdir)
        os.makedirs(dir_landing, exist_ok=True)
        fpath_parquet = os.path.join(dir_landing, f"{series_id}.parquet")

        #
        # Define observation_start_date for past year of rate data
        # The observation_start_date is always YYYY-MM-01
        #

        today_last_year_dt = datetime.strptime(today_last_year, "%Y-%m-%d")
        observation_start_date = today_last_year_dt.replace(day=1).strftime("%Y-%m-%d")

        df_fred = fetch_fred_df(observation_start_date)
        if df_fred is None:
            logger.info(f"No FRED data: series={series_id} [ingest_fred_rates]")
            return

        #
        # Create initial load if no local data exists
        #

        if not os.path.exists(fpath_parquet):
            write_parquet_atomic(df_fred, fpath_parquet)
            logger.info(f"Initial load: series={series_id}, n={len(df_fred)} [ingest_fred_rates]")
            return

        #
        # Incremental append new records
        # - concat new records to existing dataset
        # - deduplicate per fred_date
        #

        df_existing = pd.read_parquet(fpath_parquet)
        n0 = len(df_existing)
        logger.info(f"Found existing FRED data: series={series_id}, n={n0} [ingest_fred_rates]")

        df_concat = pd.concat([df_existing, df_fred]).drop_duplicates(subset=["fred_date"], keep="last")
        n1 = len(df_concat)
        logger.info(f"Concatenated FRED data n={n1} [ingest_fred_rates]")

        write_parquet_atomic(df_concat, fpath_parquet)
        logger.info(f"Appended Δn={n1-n0} records, series={series_id} [ingest_fred_rates]")

        logger.info("Done. [ingest_fred_rates]")

    except AirflowSkipException:
        raise

    except Exception as e:
        logger.error(f"FRED ingest failed, series={series_id}: {str(e)} [ingest_fred_rates]")
        raise
