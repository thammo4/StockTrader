#
# FILE: `StockTrader/scripts/skip_us_holidays.py`
#

import holidays
from datetime import datetime
from airflow.exceptions import AirflowSkipException


#
# Business logic for Airflow Task to prevent options ingestion (etc) when market is closed on hoiliday
#

#
# USAGE REFERENCE - `holidays`
#

# >>> import datetime
# >>> import holidays
# >>> holidays.US(years='2025')
# {datetime.date(2025, 1, 1): "New Year's Day", datetime.date(2025, 5, 26): 'Memorial Day', datetime.date(2025, 6, 19): 'Juneteenth National Independence Day', datetime.date(2025, 7, 4): 'Independence Day', datetime.date(2025, 9, 1): 'Labor Day', datetime.date(2025, 11, 11): 'Veterans Day', datetime.date(2025, 11, 27): 'Thanksgiving Day', datetime.date(2025, 12, 25): 'Christmas Day', datetime.date(2025, 1, 20): 'Martin Luther King Jr. Day', datetime.date(2025, 2, 17): "Washington's Birthday", datetime.date(2025, 10, 13): 'Columbus Day'}
# >>> holidays.US(years='2025').get(datetime.date(2025,7,4))
# 'Independence Day'


def skip_us_holidays(**context):
    run_date = context["ds"]
    year = int(run_date[:4])

    date_dt = datetime.strptime(run_date, "%Y-%m-%d").date()
    us_holidays = holidays.US(years=year)

    if date_dt in us_holidays:
        holiday_name = us_holidays.get(date_dt)
        raise AirflowSkipException(f"Skipping run for {holiday_name}")
