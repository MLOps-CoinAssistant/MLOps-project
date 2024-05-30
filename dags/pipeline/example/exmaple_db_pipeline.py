from airflow.decorators import dag
from pendulum import datetime
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from dags.module.example import example_db


@dag(
    start_date=datetime(2024, 5, 18),
    schedule="@hourly",
    catchup=False,
    doc_md=__doc__,
    default_args={"owner": "Astro", "retries": 3},
    tags=["postgresql", "db"],
)
def example_db_pipeline() -> None:
    start_task: EmptyOperator = EmptyOperator(task_id="start_task")

    get_KRW_BTC_data: PythonOperator = PythonOperator(
        task_id="get_KRW_BTC_data",
        python_callable=example_db.get_data,
    )

    end_task: EmptyOperator = EmptyOperator(task_id="end_task")

    start_task >> get_KRW_BTC_data >> end_task


example_db_pipeline()
