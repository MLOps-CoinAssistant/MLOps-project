from datetime import datetime, timedelta
from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from dags.module.upbit_price_prediction.btc.delay import delay_start_10
from dags.module.upbit_price_prediction.btc.classification import (
    train_catboost_model_fn,
    create_model_version,
    transition_model_stage,
    get_importance,
)
from dags.module.email_tasks import (
    get_success_email_operator,
    get_failure_email_operator,
)
from airflow.models import Variable


@dag(
    schedule_interval="50 * * * *",  # 매일 utc기준 00시에 실행 (똑같이 딜레이10초)
    start_date=datetime(2024, 6, 27, 0, 0),
    catchup=False,
    default_args={
        "owner": "ChaCoSpoons",
        "retries": 3,
        "retry_delay": timedelta(minutes=3),
        "execution_timeout": timedelta(minutes=60),
    },
    tags=["UPBIT_BTC_KRW"],
)
def model_training_pipeline():
    start_task = EmptyOperator(task_id="start_task")

    delay_task = PythonOperator(
        task_id="delay_10seconds",
        python_callable=delay_start_10,
    )
    # data_pipeline dag의 현재 상태를 탐지해서 최신기준 end_task가 끝나야 이 태스크가 완료된다. (유예시간 1시간)
    wait_for_data_pipeline = ExternalTaskSensor(
        task_id="wait_for_data_pipeline",
        external_dag_id="data_pipeline",
        external_task_id="end_task",
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        mode="reschedule",
        timeout=3600,
        poke_interval=60,  # 60초마다 감지
        execution_date_fn=lambda dt: dt,
    )

    train_model_task = PythonOperator(
        task_id="train_catboost_model",
        python_callable=train_catboost_model_fn,
    )

    get_importance_task = PythonOperator(
        task_id="get_importance",
        python_callable=get_importance,
    )

    create_model_task = PythonOperator(
        task_id="create_model_task",
        python_callable=create_model_version,
    )

    transition_model_task = PythonOperator(
        task_id="transition_model_task",
        python_callable=transition_model_stage,
    )

    end_task = EmptyOperator(task_id="end_task")

    email_addr = Variable.get("email_addr")
    success_email = get_success_email_operator(to_email=email_addr)
    failure_email = get_failure_email_operator(to_email=email_addr)

    start_task >> delay_task >> wait_for_data_pipeline >> train_model_task
    (
        train_model_task
        >> [create_model_task, get_importance_task]
        >> transition_model_task
        >> end_task
        >> success_email
    )
    [
        train_model_task,
        create_model_task,
        get_importance_task,
        transition_model_task,
        end_task,
    ] >> failure_email


model_training_pipeline()
