from datetime import datetime, timedelta
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.append("/opt/airflow")

from src.dataset import main as ingest_data

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "grid_pulse_daily_ingest",
    default_args=default_args,
    description="Runs src/dataset.py to fetch EIA data",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ingestion", "src"],
) as dag:
    # We wrap the Typer command in a simple Python function
    def run_ingestion():
        # Typer expects to be run as a script, but we can call the function directly
        # We pass default arguments manually here
        ingest_data(output_filename="demand_history.csv", days_back=1)

    t1 = PythonOperator(
        task_id="run_src_dataset",
        python_callable=run_ingestion,
    )
