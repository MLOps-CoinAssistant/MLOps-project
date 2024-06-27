from datetime import datetime, timedelta
from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from dags.module.upbit_price_prediction.btc.delay import delay_start_10
from dags.module.upbit_price_prediction.btc.create_table import create_table_fn
from dags.module.upbit_price_prediction.btc.save_raw_data import (
    save_raw_data_from_API_fn,
)
from dags.module.upbit_price_prediction.btc.preprocess import preprocess_data_fn
from dags.module.email_tasks import (
    get_success_email_operator,
    get_failure_email_operator,
)
from airflow.models import Variable


@dag(
    schedule_interval="*/5 * * * *",  # 5분마다 실행
    start_date=datetime(2024, 6, 27, 0, 0),
    catchup=False,
    default_args={
        "owner": "ChaCoSpoons",
        "retries": 3,
        "retry_delay": timedelta(minutes=3),
        "execution_timeout": timedelta(minutes=10),
    },
    tags=["UPBIT_BTC_KRW"],
)
def data_pipeline():
    start_task = EmptyOperator(task_id="start_task")

    delay_task = PythonOperator(
        task_id="delay_10seconds",
        python_callable=delay_start_10,
    )

    create_table_task = PythonOperator(
        task_id="create_table",
        python_callable=create_table_fn,
    )

    save_data_task = PythonOperator(
        task_id="save_raw_data_from_UPBIT_API",
        python_callable=save_raw_data_from_API_fn,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    preprocess_task = PythonOperator(
        task_id="preprocess_data",
        python_callable=preprocess_data_fn,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    end_task = EmptyOperator(task_id="end_task", trigger_rule=TriggerRule.ALL_DONE)

    email_addr = Variable.get("email_addr")
    success_email = get_success_email_operator(to_email=email_addr)
    failure_email = get_failure_email_operator(to_email=email_addr)

    (
        start_task
        >> delay_task
        >> create_table_task
        >> save_data_task
        >> preprocess_task
        >> end_task
    )
    end_task >> success_email
    [
        delay_task,
        create_table_task,
        save_data_task,
        preprocess_task,
        end_task,
    ] >> failure_email


data_pipeline()
