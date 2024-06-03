from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from dags.module.upbit_price_prediction.btc.create_table import create_table_fn
from dags.module.upbit_price_prediction.btc.save_raw_data import (
    save_raw_data_from_API_fn,
)
from dags.module.email_tasks import (
    get_success_email_operator,
    get_failure_email_operator,
)
from airflow.utils.trigger_rule import TriggerRule
from dags.module.upbit_price_prediction.btc.preprocess import preprocess_data_fn
from dags.module.upbit_price_prediction.btc.classification import (
    train_catboost_model_fn,
    create_model_version,
    transition_model_stage,
)


@dag(
    schedule_interval="2 * * * *",  # 매 시간 실행
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
def btc_price_prediction_pipeline():
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

    train_model_task = PythonOperator(
        task_id="train_catboost_model_fn",
        python_callable=train_catboost_model_fn,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    create_model_task: PythonOperator = PythonOperator(
        task_id="create_model_task_fn",
        python_callable=create_model_version,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    transition_model_task: PythonOperator = PythonOperator(
        task_id="transition_model_task_fn",
        python_callable=transition_model_stage,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    end_task = EmptyOperator(task_id="end_task", trigger_rule=TriggerRule.ALL_DONE)

    email_addr = Variable.get("email_addr")
    success_email = get_success_email_operator(to_email=email_addr)
    failure_email = get_failure_email_operator(to_email=email_addr)

    (
        start_task
        >> create_table_task
        >> save_data_task
        >> preprocess_task
        >> train_model_task
        >> create_model_task
        >> transition_model_task
        >> end_task
    )
    end_task >> success_email

    [
        create_table_task,
        save_data_task,
        preprocess_task,
        train_model_task,
        create_model_task,
        transition_model_task,
        end_task,
    ] >> failure_email


btc_price_prediction_pipeline()
