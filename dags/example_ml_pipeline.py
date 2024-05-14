from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from pendulum import datetime
from module import example_train


@dag(
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    doc_md=__doc__,
    default_args={"owner": "Astro", "retries": 3},
    tags=["example"],
)
def ml_pipeline():
    start_task = EmptyOperator(task_id="start_task")

    train_iris_task = PythonOperator(
        task_id="train_iris_task",
        python_callable=example_train.train_fn_iris,
    )

    iris_model_create_task = PythonOperator(
        task_id="iris_model_create_task",
        python_callable=example_train.create_model_version,
        op_kwargs={"model_name": "iris_model"},
    )

    iris_model_transition_task = PythonOperator(
        task_id="iris_model_transition_task",
        python_callable=example_train.transition_model_stage,
        op_kwargs={"model_name": "iris_model"},
    )

    end_task = EmptyOperator(task_id="end_task")

    start_task >> [train_iris_task] >> end_task
    train_iris_task >> iris_model_create_task >> iris_model_transition_task


ml_pipeline()
