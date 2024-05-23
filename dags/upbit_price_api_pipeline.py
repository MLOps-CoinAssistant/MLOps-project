from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from module import email_tasks
from dags.module.upbit_api_call import run_upbit_api_call_event_loop_policy_sync


@dag(
    schedule_interval="5 * * * *",  # 매 시간 5분에 실행: x시 0분에 생성된 API 데이터를 x시 0분에 호출할 경우, 실패할 수도 있음을 고려
    start_date=datetime(2024, 5, 15),
    catchup=False,  # 이전 실행은 무시
    default_args={
        "owner": "Astro",
        "retries": 3,
        "retry_delay": timedelta(minutes=3),
        "execution_timeout": timedelta(minutes=5),
    },
    tags=["UPBIT", "api", "DB"],
)
def upbit_price_api_pipeline():
    start_task = EmptyOperator(task_id="start_task")
    task_collect_and_load_data = PythonOperator(
        task_id="collect_and_load_coin_price_data",
        python_callable=run_upbit_api_call_event_loop_policy_sync,
    )
    end_task = EmptyOperator(task_id="end_task")

    success_email = email_tasks.get_success_email_operator(to_email="raphdoh@naver.com")
    failure_email = email_tasks.get_failure_email_operator(to_email="raphdoh@naver.com")

    start_task >> task_collect_and_load_data >> end_task
    end_task >> success_email
    [start_task, task_collect_and_load_data, end_task] >> failure_email


upbit_price_api_pipeline()
