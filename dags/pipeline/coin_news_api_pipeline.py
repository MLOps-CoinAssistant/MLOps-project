from airflow.decorators import dag
from pendulum import datetime
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from module import coin_news_api_call, email_tasks


@dag(
    start_date=datetime(2024, 5, 18),
    schedule_interval="10 * * * *",  # 10분마다 실행되도록 설정
    catchup=False,
    doc_md=__doc__,
    tags=["postgresql", "db", "news"],
)
def coin_news_api_pipeline() -> None:
    start_task: EmptyOperator = EmptyOperator(task_id="start_task")

    create_db_task: PythonOperator = PythonOperator(
        task_id="create_database_if_not_exists",
        python_callable=coin_news_api_call.create_database_if_not_exists,
    )

    clear_and_save_news_task: PythonOperator = PythonOperator(
        task_id="clear_and_save_news",
        python_callable=coin_news_api_call.clear_and_save_news,
    )

    end_task: EmptyOperator = EmptyOperator(task_id="end_task")

    # 이메일 태스크 추가
    success_email = email_tasks.get_success_email_operator(to_email="raphdoh@naver.com")
    failure_email = email_tasks.get_failure_email_operator(to_email="raphdoh@naver.com")

    start_task >> create_db_task >> clear_and_save_news_task >> end_task
    end_task >> success_email
    [create_db_task, clear_and_save_news_task, end_task] >> failure_email


coin_news_api_pipeline()
