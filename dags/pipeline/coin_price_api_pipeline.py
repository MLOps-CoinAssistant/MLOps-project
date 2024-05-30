# from airflow.decorators import dag
# from airflow.operators.python import PythonOperator
# from airflow.operators.empty import EmptyOperator
# from module.coin_price_api_call import collect_and_load_data_sync
# from module.coin_model import predict
# from datetime import datetime, timedelta


# @dag(
#     schedule_interval="2 * * * *",  # 매 시간 2분에 실행
#     start_date=datetime(2024, 5, 15),
#     catchup=False,  # 이전 실행은 무시합니다.
#     default_args={
#         "owner": "Astro",
#         "retries": 3,
#         "retry_delay": timedelta(minutes=3),
#         "execution_timeout": timedelta(minutes=5),
#     },  # 태스크 시간 제한 설정
#     tags=["upbit_to_cloudsql"],
# )
# def coin_price_api_pipeline():
#     start_task = EmptyOperator(task_id="start_task")
#     task_collect_and_load_data = PythonOperator(
#         task_id="collect_and_load_data",
#         python_callable=collect_and_load_data_sync,
#     )
#     task_XGboostRegressor = PythonOperator(
#         task_id="predict",
#         python_callable=predict,
#     )
#     end_task = EmptyOperator(task_id="end_task")

#     start_task >> task_collect_and_load_data >> task_XGboostRegressor >> end_task


# # DAG를 생성합니다.
# coin_price_api_pipeline()
