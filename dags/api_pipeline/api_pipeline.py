from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from module.upbit_to_cloudsql import collect_and_load_data
from datetime import datetime, timedelta


# Airflow DAG를 정의합니다.
@dag(
    schedule_interval="7 * * * *",  # 매 시간 0분에 실행
    start_date=datetime(2024, 5, 15),
    catchup=False,  # 이전 실행은 무시합니다.
    default_args={
        "owner": "Astro",
        "retries": 3,
        "retry_delay": timedelta(minutes=3),
        "execution_timeout": timedelta(minutes=5),
    },  # 태스크 시간 제한 설정},
    tags=["upbit_to_cloudsql"],
)
def api_pipeline():
    # 데이터 수집 및 적재 작업을 정의하고 Airflow DAG에 추가합니다.
    start_task = EmptyOperator(task_id="start_task")
    task_collect_and_load_data = PythonOperator(
        task_id="collect_and_load_data",
        python_callable=collect_and_load_data,
    )

    end_task = EmptyOperator(task_id="end_task")

    # 작업 순서를 정의합니다.
    start_task >> task_collect_and_load_data >> end_task


# DAG를 생성합니다.
api_pipeline()
