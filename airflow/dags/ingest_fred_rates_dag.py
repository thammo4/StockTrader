#
# FILE: `StockTrader/airflow/dags/ingest_fred_rates_dag.py`
#

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.log.logging_mixin import LoggingMixin

from scripts.ingest_fred_rates import ingest_fred_rates

log = LoggingMixin().log


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 5,
    "retry_delay": timedelta(days=1),
}


def dag_me():
    log.info("Starting ingest_fred_rates_dag")
    with DAG(
        dag_id="ingest_fred_rates",
        default_args=default_args,
        description="Ingest monthly Federal Reserve interest rate data",
        schedule_interval="0 15 25-31 * 1",
        start_date=datetime(2025, 5, 1),
        catchup=False,
        tags=["interest_rate", "ingest", "fred"],
    ) as dag:
        ingest_tb3ms = PythonOperator(
            task_id="ingest_tb3ms_rate",
            python_callable=ingest_fred_rates,
            op_kwargs={"series_id": "TB3MS", "subdir": "fred_af"},
        )

    return dag


dag = dag_me()
