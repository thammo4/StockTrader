#
# FILE: `StockTrader/airflow/dags/test_dag.py`
#


import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parents[2]))

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime

from scripts.ingest_options_chain import ingest_options_chain

default_args = {"owner":"airflow", "start_date":datetime(2025,3,17), "retries":1}
with DAG("test_dag", default_args=default_args, schedule_interval="@daily", catchup=False) as dag:
	start = DummyOperator(task_id="start")

start