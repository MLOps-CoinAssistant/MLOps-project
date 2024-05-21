from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1),
    "retries": 1,
}

dag = DAG("test_dag", default_args=default_args, schedule_interval="@daily")

start = DummyOperator(task_id="start", dag=dag)
