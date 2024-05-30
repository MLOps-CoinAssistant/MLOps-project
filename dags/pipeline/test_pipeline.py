from datetime import datetime, timedelta
from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from dags.module.api.create_table import create_table_fn
from dags.module.api.save_raw_data import save_raw_data_from_API_fn
from module.email_tasks import get_success_email_operator, get_failure_email_operator
from airflow.utils.trigger_rule import TriggerRule
from dags.module.api.preprocess import preprocess_data_fn


@dag(
    schedule_interval="0 * * * *",  # 매 시간 실행
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
def test_pipeline():
    start_task = EmptyOperator(task_id="start_task")

    create_table_task = PythonOperator(
        task_id="create_table_fn",
        python_callable=create_table_fn,
    )

    save_data_task = PythonOperator(
        task_id="save_raw_data_from_API_fn",
        python_callable=save_raw_data_from_API_fn,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    preprocess_task = PythonOperator(
        task_id="preprocess_data_fn",
        python_callable=preprocess_data_fn,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    end_task = EmptyOperator(task_id="end_task", trigger_rule=TriggerRule.ALL_DONE)

    success_email = get_success_email_operator(to_email="raphdoh@naver.com")
    failure_email = get_failure_email_operator(to_email="raphdoh@naver.com")

    start_task >> create_table_task >> save_data_task >> preprocess_task >> end_task
    end_task >> success_email
    [create_table_task, save_data_task, preprocess_task, end_task] >> failure_email


test_pipeline()
