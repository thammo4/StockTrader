#
# FILE: `StockTrader/airflow/dags/monitor_tradier_portfolio_dag.py`
#

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.log.logging_mixin import LoggingMixin

from scripts.monitor_tradier_portfolio import monitor_tradier_portfolio
from scripts.close_premium_capture import close_premium_capture
from scripts.skip_us_holidays import skip_us_holidays

log = LoggingMixin().log


#
# Execution Environment
#
# Single paper/live binding: the account that produces the snapshot and the
# order/account clients that close against it are all derived from LIVE.
#

LIVE = False
DRY_RUN = True


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


#
# Premium-Capture Close Against This Run's Snapshot
#

def _close_premium_capture(ti, **_):
    monitor_out = ti.xcom_pull(task_ids="monitor_portfolio") or {}

    return close_premium_capture(
        snapshot_id=monitor_out.get("snapshot_id"),
        live=LIVE,
        dry_run=DRY_RUN,
    )


def dag_me():
    log.info("Starting monitor_tradier_portfolio dag [monitor_tradier_portfolio]")

    from StockTrader.tradier import acct, acctL, quotesL

    with DAG(
        dag_id="monitor_tradier_portfolio",
        default_args=default_args,
        description="Intraday portfolio position snapshots, M2M summaries, and premium-capture closes",
        schedule_interval="*/5 9-16 * * 1-5",
        start_date=pendulum.datetime(2026, 9, 1, tz="America/New_York"),
        catchup=False,
        max_active_runs=1,
        tags=["positions", "m2m", "monitor", "tradier", "portfolio", "premium_capture"],
    ) as dag:
        skip_holiday = PythonOperator(task_id="skip_us_holiday", python_callable=skip_us_holidays, provide_context=True)
        run_monitor = PythonOperator(
            task_id="monitor_portfolio",
            python_callable=monitor_tradier_portfolio,
            op_kwargs={"acct_client": acctL if LIVE else acct, "quotes_client": quotesL},
        )

        #
        # retries=0: a retry after partial submission would resubmit orders
        #

        run_close = PythonOperator(
            task_id="close_premium_capture",
            python_callable=_close_premium_capture,
            retries=0,
        )

        skip_holiday >> run_monitor >> run_close
    return dag


dag = dag_me()
