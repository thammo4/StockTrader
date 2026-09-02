#
# FILE: `StockTrader/airflow/dags/monitor_tradier_portfolio_dag.py`
#

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.log.logging_mixin import LoggingMixin

from scripts.monitor_tradier_portfolio import monitor_tradier_portfolio
from scripts.skip_us_holidays import skip_us_holidays

log = LoggingMixin().log


#
# Define DAG Params
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
    log.info("Starting monitor_tradier_portfolio dag [monitor_tradier_portfolio]")

    from StockTrader.tradier import acct, quotesL

    with DAG(
        dag_id="monitor_tradier_portfolio",
        default_args=default_args,
        description="Intraday portfolio position snapshots and M2M summaries to S3 bucket",
        schedule_interval="*/5 9-16 * * 1-5",
        start_date=datetime(2026, 9, 1),
        catchup=False,
        max_active_runs=1,
        tags=["positions", "m2m", "monitor", "tradier", "portfolio"],
    ) as dag:
        skip_holiday = PythonOperator(task_id="skip_us_holiday", python_callable=skip_us_holidays, provide_context=True)
        run_monitor = PythonOperator(
            task_id="monitor_portfolio",
            python_callable=monitor_tradier_portfolio,
            op_kwargs={"acct_client": acct, "quotes_client": quotesL},
        )

        skip_holiday >> run_monitor
    return dag


dag = dag_me()
