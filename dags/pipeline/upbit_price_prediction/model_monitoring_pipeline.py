from datetime import datetime, timedelta
from airflow.decorators import dag
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from dags.module.upbit_price_prediction.btc.check_performance import (
    check_model_performance,
    monitoring,
)
from dags.module.email_tasks import (
    get_success_email_operator,
    get_failure_email_operator,
)
from airflow.models import Variable


@dag(
    schedule_interval="0 * * * *",
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
def model_monitoring_pipeline():
    start_task = EmptyOperator(task_id="start_task")

    check_performance_task = PythonOperator(
        task_id="check_model_performance",
        python_callable=check_model_performance,
    )

    monitoring_task = BranchPythonOperator(
        task_id="monitoring_performance",
        python_callable=monitoring,
        provide_context=True,
    )

    trigger_training = TriggerDagRunOperator(
        task_id="trigger_training",
        trigger_dag_id="model_training_pipeline",
    )

    end_task = EmptyOperator(task_id="end_task")

    email_addr = Variable.get("email_addr")
    success_email = get_success_email_operator(to_email=email_addr)
    failure_email = get_failure_email_operator(to_email=email_addr)

    start_task >> check_performance_task >> monitoring_task
    monitoring_task >> [trigger_training, end_task]
    trigger_training >> end_task
    end_task >> success_email
    [
        check_performance_task,
        monitoring_task,
        trigger_training,
        end_task,
    ] >> failure_email


model_monitoring_pipeline()
