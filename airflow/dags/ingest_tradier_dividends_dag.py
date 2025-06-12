#
# FILE: `StockTrader/airflow/dags/ingest_tradier_dividends_dag.py`
#

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.log.logging_mixin import LoggingMixin

from scripts.ingest_tradier_dividends import get_symbols, ingest_tradier_dividends

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
    "retry_delay": timedelta(days=1),
}


def dag_me():
    log.info("Starting ingest_tradier_dividends_dag")
    with DAG(
        dag_id="ingest_tradier_dividends",
        default_args=default_args,
        description="Retrieve quarterly dividends from Tradier",
        schedule_interval="0 17 28 1,4,7,10 *",
        start_date=datetime(2025, 1, 1),
        catchup=False,
        max_active_tasks=20,
        tags=["dividends", "ingest", "tradier", "quarterly"],
    ) as dag:
        symbols = get_symbols()
        batches = [symbols[i : i + 10] for i in range(0, len(symbols), 10)]
        task_groups = []

        for idx, batch in enumerate(batches):
            with TaskGroup(group_id=f"batch_{idx}") as group:
                for symbol in batch:
                    PythonOperator(
                        task_id=f"ingest_{symbol}",
                        python_callable=ingest_tradier_dividends,
                        op_kwargs={"symbol": symbol},
                        trigger_rule="all_done",
                    )
            task_groups.append(group)

        for i in range(len(task_groups) - 1):
            task_groups[i] >> task_groups[i + 1]

    return dag


dag = dag_me()
