#
# FILE: `StockTrader/scripts/skip_us_holidays.py`
#

import holidays
from datetime import datetime
from airflow.exceptions import AirflowSkipException


#
# Business logic for Airflow Task to prevent options ingestion (etc) when market is closed on hoiliday
#


def skip_us_holidays(**context):
    run_date = context["ds"]
    year = int(run_date[:4])

    date_dt = datetime.strptime(run_date, "%Y-%m-%d").date()
    us_holidays = holidays.US(years=year)

    if run_date in us_holidays or date_dt in us_holidays:
        holiday_name = us_holidays.get(date_dt, us_holidays.get(run_date, "Holiday"))
        raise AirflowSkipException(f"Skipping run for {holiday_name}")
