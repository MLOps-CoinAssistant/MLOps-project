from datetime import datetime, timedelta
from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.docker.operators.docker import DockerOperator
from dags.module.email_tasks import (
    get_success_email_operator,
    get_failure_email_operator,
)
from docker.types import Mount
from info.connections import Connections


@dag(
    schedule_interval="2 * * * *",  # 매 시간 실행
    start_date=datetime(2024, 6, 3),
    catchup=True,  # 이전 실행은 무시합니다.
    default_args={
        "owner": "ChaCoSpoons",
        "retries": 3,
        "retry_delay": timedelta(minutes=3),
        "execution_timeout": timedelta(minutes=5),
    },
    tags=["upbit pipeline"],
)
def btc_price_prediction_pipeline() -> None:
    start_task: EmptyOperator = EmptyOperator(task_id="start_task")

    # PostgresHook을 사용하여 데이터베이스 연결 정보를 가져옵니다.
    hook = PostgresHook(postgres_conn_id=Connections.POSTGRES_DEFAULT.value)
    db_uri = hook.get_uri()

    # 데이터베이스 연결 정보를 환경 변수로 설정합니다.
    common_environment = {
        "AIRFLOW__CORE__SQL_ALCHEMY_CONN": db_uri,
        "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": db_uri,
    }

    create_table_task: DockerOperator = DockerOperator(
        task_id="create_table_fn",
        image="model_test:latest",  # 빌드한 이미지 사용
        api_version="auto",
        auto_remove=True,
        command="create_table.py create_table_fn",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        environment=common_environment,
        mounts=[
            Mount(
                source="/var/run/docker.sock",
                target="/var/run/docker.sock",
                type="bind",
            )
        ],
        trigger_rule=TriggerRule.ALL_DONE,
        user="astro",
    )

    save_data_task: DockerOperator = DockerOperator(
        task_id="save_raw_data_from_API_fn",
        image="model_test:latest",  # 빌드한 이미지 사용
        api_version="auto",
        auto_remove=True,
        command="save_raw_data.py save_raw_data_from_API_fn",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        environment=common_environment,
        mounts=[
            Mount(
                source="/var/run/docker.sock",
                target="/var/run/docker.sock",
                type="bind",
            )
        ],
        trigger_rule=TriggerRule.ALL_DONE,
        user="astro",
    )

    preprocess_task: DockerOperator = DockerOperator(
        task_id="preprocess_data_fn",
        image="model_test:latest",  # 빌드한 이미지 사용
        api_version="auto",
        auto_remove=True,
        command="preprocess.py preprocess_data_fn",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        environment=common_environment,
        mounts=[
            Mount(
                source="/var/run/docker.sock",
                target="/var/run/docker.sock",
                type="bind",
            )
        ],
        trigger_rule=TriggerRule.ALL_DONE,
        user="astro",
    )

    train_model_task: DockerOperator = DockerOperator(
        task_id="train_catboost_model_fn",
        image="model_test:latest",  # 빌드한 이미지 사용
        api_version="auto",
        auto_remove=True,
        command="classification.py train_catboost_model_fn",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        environment=common_environment,
        mounts=[
            Mount(
                source="/var/run/docker.sock",
                target="/var/run/docker.sock",
                type="bind",
            )
        ],
        trigger_rule=TriggerRule.ALL_DONE,
        user="astro",
    )

    end_task: EmptyOperator = EmptyOperator(
        task_id="end_task", trigger_rule=TriggerRule.ALL_DONE
    )

    success_email = get_success_email_operator(to_email="p61515@naver.com")
    failure_email = get_failure_email_operator(to_email="p61515@naver.com")

    (
        start_task
        >> create_table_task
        >> save_data_task
        >> preprocess_task
        >> train_model_task
        >> end_task
    )
    end_task >> success_email
    [
        create_table_task,
        save_data_task,
        preprocess_task,
        train_model_task,
        end_task,
    ] >> failure_email


btc_price_prediction_pipeline()
