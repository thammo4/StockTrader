#
# FILE: `StockTrader/scripts/skip_us_holidays.py`
#

import holidays
from airflow.exceptions import AirflowSkipException


#
# Business logic for Airflow Task to prevent options ingestion (etc) when market is closed on hoiliday
#


def skip_us_holidays(**context):
    run_date = context["ds"]
    year = int(run_date[:4])
    us_holidays = holidays.US(years=year)
    if run_date in us_holidays:
        raise AirflowSkipException(f"Skipping run for {us_holidays[run_date]}")
