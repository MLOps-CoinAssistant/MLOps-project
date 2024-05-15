from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from module import upbit_to_cloudsql


# Airflow DAG를 정의합니다.
@dag(
    schedule_interval="0 * * * *",  # 매 시간 0분에 실행
    catchup=False,  # 이전 실행은 무시합니다.
    default_args={"owner": "Astro", "retries": 3},
    tags=["upbit_to_cloudsql"],
)  # DAG에 태그를 추가합니다.
def api_pipeline():
    # 데이터 수집 및 적재 작업을 정의하고 Airflow DAG에 추가합니다.
    start_task = EmptyOperator(task_id="start_task")
    task_collect_and_load_data = PythonOperator(
        task_id="collect_and_load_data",
        python_callable=upbit_to_cloudsql.collect_and_load_data,
    )
    end_task = EmptyOperator(task_id="end_task")

    start_task >> task_collect_and_load_data >> end_task


api_pipeline()
