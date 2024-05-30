from datetime import datetime, timedelta
from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from dags.module.api.create_table import create_table_fn
from module.email_tasks import get_success_email_operator, get_failure_email_operator
from airflow.utils.trigger_rule import TriggerRule


@dag(
    schedule_interval="0 * * * *",  # 매 시간 2분에 실행
    start_date=datetime(2024, 5, 30),
    catchup=True,  # 이전 실행은 무시합니다.
    default_args={
        "owner": "ChaCoSpoons",
        "retries": 3,
        "retry_delay": timedelta(minutes=3),
        "execution_timeout": timedelta(minutes=5),
    },
    tags=["upbit pipeline"],
)
def price_prediction_pipeline():
    start_task = EmptyOperator(task_id="start_task")

    create_table_task = PythonOperator(
        task_id="create_table_fn",
        python_callable=create_table_fn,
    )

    end_task = EmptyOperator(task_id="end_task", trigger_rule=TriggerRule.ALL_DONE)

    success_email = get_success_email_operator(to_email="raphdoh@naver.com")
    failure_email = get_failure_email_operator(to_email="raphdoh@naver.com")

    start_task >> create_table_task >> end_task
    end_task >> success_email
    [create_table_task, end_task] >> failure_email


price_prediction_pipeline()
