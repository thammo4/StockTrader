#
# FILE: `StockTrader/airflow/dags/ingest_tradier_quotes_dag.py`
#

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.log.logging_mixin import LoggingMixin

from scripts.ingest_tradier_quotes import ingest_tradier_quotes
from scripts.skip_us_holidays import skip_us_holidays

log = LoggingMixin().log

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def dag_me():
    log.info("Starting ingest_tradier_quotes_dag")
    with DAG(
        dag_id="ingest_tradier_quotes",
        default_args=default_args,
        description="Retrieve EOD quote data for symbols in largecap_all text file",
        schedule_interval="35 16 * * 1-5",
        start_date=datetime(2025, 5, 1),
        catchup=False,
        tags=["quotes", "ingest", "tradier"],
    ) as dag:
        #
        # Check for US Holiday prior to ingest
        #

        skip_holiday = PythonOperator(task_id="skip_us_holiday", python_callable=skip_us_holidays, provide_context=True)

        #
        # Quote Ingestion
        # Retrieve all quote data in single API call, then write to each symbol-specific parquet file
        #

        ingest_quotes = PythonOperator(
            task_id="ingest_all_quotes", python_callable=ingest_tradier_quotes, op_kwargs={"subdir": "quotes_af"}
        )

        #
        # Dependency Structure
        # First, check for US Holiday
        # If not US Holiday, perform quote ingestion
        # Otherwise, gobble em up
        #

        skip_holiday >> ingest_quotes

    return dag


dag = dag_me()
