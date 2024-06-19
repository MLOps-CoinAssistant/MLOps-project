from datetime import datetime, timedelta
from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from dags.module.NLP.btc.load_pretrained_model import (
    download_and_log_model,
)


@dag(
    schedule_interval="* * * * *",  # 시작시간을 5의배수분으로 정하고 시작 (utc기준)
    start_date=datetime(2024, 6, 15, 21, 50),
    catchup=False,
    default_args={
        "owner": "ChaCoSpoons",
        "retries": 0,
        "retry_delay": timedelta(minutes=3),
        "execution_timeout": timedelta(minutes=5),
    },
    tags=["huggingface"],
)
def load_pretrained_model_to_mlflow_pipeline():
    start_task = EmptyOperator(task_id="start_task")
    
    download_task = PythonOperator(
        task_id='download_and_log_model',
        provide_context=True,
        python_callable=download_and_log_model,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    end_task = EmptyOperator(task_id="end_task", trigger_rule=TriggerRule.ALL_DONE)

    start_task >> download_task >> end_task

load_pretrained_model_to_mlflow_pipeline()
