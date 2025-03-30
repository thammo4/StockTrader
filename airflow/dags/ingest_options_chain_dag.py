# 
# FILE: `StockTrader/airflow/dags/ingest_options_chain_dag.py`
#

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parents[2]))

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from scripts.ingest_options_chain import ingest_options_chain

default_args = {
	"owner": "airflow",
	"depends_on_past": False,
	# "start_date": datetime.today(),
	"start_date": datetime(2025,3,17),
	"retries": 1,
	"retry_delay": timedelta(minutes=2)
}

# cron: minute hour day_of_month month day_of_week

dag = DAG(
	"ingest_options_chain",
	default_args = default_args,
	schedule_interval = "35 14 * * 1-5",
	catchup = False
)

DOW30_DIVIDENDS = ['AXP', 'AMGN', 'AAPL', 'BA', 'CAT', 'CSCO', 'CVX', 'GS', 'HD', 'HON', 'IBM', 'JNJ', 'KO', 'JPM', 'MCD', 'MMM', 'MRK', 'MSFT', 'NKE', 'PG', 'TRV', 'UNH', 'CRM', 'VZ', 'V', 'WMT', 'DIS', 'DOW']

tasks = []
for s in DOW30_DIVIDENDS:
	task = PythonOperator(
		task_id=f"ingest_options_chain_{s}",
		python_callable=ingest_options_chain,
		op_kwargs={"symbol":s},
		dag=dag
	)
	tasks.append(task)