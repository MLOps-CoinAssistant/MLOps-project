from airflow.decorators import dag
from pendulum import datetime
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from module import coin_news_api_call


@dag(
    start_date=datetime(2024, 5, 18),
    schedule_interval="@hourly",
    catchup=False,
    doc_md=__doc__,
    default_args={"owner": "Astro", "retries": 3},
    tags=["postgresql", "db", "news"],
)
def news_api_call_pipeline():
    start_task = EmptyOperator(task_id="start_task")

    create_db_task = PythonOperator(
        task_id="create_database_if_not_exists",
        python_callable=coin_news_api_call.create_database_if_not_exists,
    )

    clear_and_save_news_task = PythonOperator(
        task_id="clear_and_save_news",
        python_callable=coin_news_api_call.clear_and_save_news,
    )

    end_task = EmptyOperator(task_id="end_task")

    start_task >> [create_db_task >> clear_and_save_news_task] >> end_task


news_api_call_pipeline()
