#
# FILE: `StockTrader/airflow/dags/ingest_tradier_options_dag.py`
#

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.log.logging_mixin import LoggingMixin

from scripts.ingest_tradier_options import get_symbols, ingest_tradier_options
from scripts.skip_us_holidays import skip_us_holidays

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
    log.info("Starting ingest_tradier_options_dag")
    with DAG(
        dag_id="ingest_tradier_options",
        default_args=default_args,
        description="Retrieve current day's options chain data from Tradier for symbols in largecap_all text file",
        schedule_interval="40 16 * * 1-5",
        start_date=datetime(2025, 5, 1),
        catchup=False,
        max_active_tasks=20,
        tags=["options_chain", "ingest", "tradier"],
    ) as dag:

        #
        # Check for US Holidays Before Ingestion
        #

        skip_holiday = PythonOperator(task_id="skip_us_holiday", python_callable=skip_us_holidays, provide_context=True)

        #
        # Retrieve symbols, create batches, and ingest
        #

        symbols = get_symbols()
        batches = [symbols[i : i + 10] for i in range(0, len(symbols), 10)]
        task_groups = []

        for idx, batch in enumerate(batches):
            log.info(f"batch_{idx} [k={len(batch)}]")
            with TaskGroup(group_id=f"batch_{idx}") as group:
                for symbol in batch:
                    PythonOperator(
                        task_id=f"ingest_{symbol}",
                        python_callable=ingest_tradier_options,
                        op_kwargs={"symbol": symbol},
                        trigger_rule="all_done",
                        pool="tradier_options_api_pool",
                    )
            task_groups.append(group)

        #
        # Wave-based processing to provide predictable/manageable resource consumption patterns
        #

        wave_size = 4
        for wave_start in range(0, len(task_groups), wave_size):
            current_wave = task_groups[wave_start : wave_start + wave_size]

            for group in current_wave:
                skip_holiday >> group

            if wave_start + wave_size < len(task_groups):
                next_wave = task_groups[wave_start + wave_size : wave_start + 2 * wave_size]
                for current_group in current_wave:
                    for next_group in next_wave:
                        current_group >> next_group

    return dag


dag = dag_me()
