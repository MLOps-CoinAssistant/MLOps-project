from airflow.decorators import dag
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from docker.types import Mount

default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    default_args=default_args,
    schedule_interval="2 * * * *",  # 매 시간 2분에 실행,
    catchup=False,
    description="An example ML pipeline using DockerOperator",
    tags=["example"],
)
def ml_pipeline_docker() -> None:
    start_task: EmptyOperator = EmptyOperator(task_id="start_task")

    train_iris_task: DockerOperator = DockerOperator(
        task_id="train_iris_task",
        image="ml-ops-project_c92c7b/airflow:latest",  # 빌드한 이미지 사용
        api_version="auto",
        auto_remove=True,
        command="python /app/example_train.py train_fn_iris",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        mounts=[
            Mount(
                source="/var/run/docker.sock",
                target="/var/run/docker.sock",
                type="bind",
            )
        ],
    )

    iris_model_create_task: DockerOperator = DockerOperator(
        task_id="iris_model_create_task",
        image="ml-ops-project_c92c7b/airflow:latest",  # 빌드한 이미지 사용
        api_version="auto",
        auto_remove=True,
        command="python /app/example_train.py create_model_version --model_name iris_model",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        mounts=[
            Mount(
                source="/var/run/docker.sock",
                target="/var/run/docker.sock",
                type="bind",
            )
        ],
    )

    iris_model_transition_task: DockerOperator = DockerOperator(
        task_id="iris_model_transition_task",
        image="ml-ops-project_c92c7b/airflow:latest",  # 빌드한 이미지 사용
        api_version="auto",
        auto_remove=True,
        command="python /app/example_train.py transition_model_stage --model_name iris_model",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        mounts=[
            Mount(
                source="/var/run/docker.sock",
                target="/var/run/docker.sock",
                type="bind",
            )
        ],
    )

    end_task: EmptyOperator = EmptyOperator(task_id="end_task")

    start_task >> train_iris_task >> [iris_model_create_task, end_task]
    iris_model_create_task >> iris_model_transition_task


ml_pipeline_docker()
