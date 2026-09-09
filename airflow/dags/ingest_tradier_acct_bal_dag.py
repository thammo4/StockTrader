#
# FILE: `StockTrader/airflow/dags/ingest_tradier_acct_bal_dag.py`
#

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.log.logging_mixin import LoggingMixin

from scripts.ingest_tradier_acct_bal import ingest_tradier_acct_bal
from scripts.skip_us_holidays import skip_us_holidays

log = LoggingMixin().log

#
# Define DAG Parameters
#

default_args = {
    "owner": "airflow",
    "depends_on_path": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def dag_me():
    log.info("Starting ingest_tradier_acct_bal dag [ingest_tradier_acct_bal]")
    with DAG(
        dag_id="ingest_tradier_acct_bal",
        default_args=default_args,
        description="Retrieve intraday account balance snapshots from Tradier",
        schedule_interval="30 9-16 * * 1-5",
        start_date=datetime(2026, 7, 1),
        catchup=False,
        tags=["acct_bal", "ingest", "tradier", "portfolio"],
    ) as dag:
        skip_holiday = PythonOperator(task_id="skip_us_holiday", python_callable=skip_us_holidays, provide_context=True)
        ingest_acct_bal = PythonOperator(
            task_id="ingest_acct_bal", python_callable=ingest_tradier_acct_bal, op_kwargs={"live": False}
        )

        skip_holiday >> ingest_acct_bal

    return dag


dag = dag_me()
