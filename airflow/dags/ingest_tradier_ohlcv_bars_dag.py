#
# FILE: `StockTrader/airflow/dags/ingest_tradier_ohlcv_bars_dag.py`
#

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.log.logging_mixin import LoggingMixin

from scripts.ingest_tradier_ohlcv_bars import ingest_tradier_ohlcv_bars
from scripts.skip_us_holidays import skip_us_holidays
from utils.get_symbols import get_symbols

log = LoggingMixin().log

#
# Define DAG Parameters
#

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


def dag_me():
    log.info("Starting ingest_tradier_ohlcv_bars_dag")
    with DAG(
        dag_id="ingest_tradier_ohlcv_bars",
        default_args=default_args,
        description="Retrieve current day's OHLCV bar data from Tradier for symbols in largecap_all text file",
        schedule_interval="45 16 * * 1-5",
        start_date=datetime(2025, 5, 1),
        catchup=False,
        max_active_tasks=50,
        tags=["ohlcv_bars", "ingest", "tradier"],
    ) as dag:

        #
        # Check for US Holidays Before Ingestion
        #

        skip_holiday = PythonOperator(task_id="skip_us_holiday", python_callable=skip_us_holidays, provide_context=True)

        #
        # OHLCV Bar Ingestion
        # Retrieve one bar per symbol (lightweight compared to options chains)
        # Simple parallel execution controlled by max_active_tasks
        #

        symbols = get_symbols()
        ingest_tasks = []

        for symbol in symbols:
            task = PythonOperator(
                task_id=f"ingest_{symbol}",
                python_callable=ingest_tradier_ohlcv_bars,
                op_kwargs={"symbol": symbol},
                trigger_rule="all_done",
            )
            ingest_tasks.append(task)
            skip_holiday >> task

    return dag


dag = dag_me()
