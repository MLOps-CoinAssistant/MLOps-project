from airflow.operators.email import EmailOperator


def get_success_email_operator(to_email: str) -> EmailOperator:
    return EmailOperator(
        task_id="send_success_email",
        to=to_email,
        subject="DAG Success: {{ task_instance.dag_id }}",
        html_content="DAG: {{ task_instance.dag_id }}<br>Task: {{ task_instance.task_id }}<br>Execution Time: {{ ts }}<br>Log URL: {{ task_instance.log_url }}",
        conn_id="smtp_default",
        trigger_rule="all_success",
    )


def get_failure_email_operator(to_email: str) -> EmailOperator:
    return EmailOperator(
        task_id="send_failure_email",
        to=to_email,
        subject="DAG Failure: {{ task_instance.dag_id }}",
        html_content="DAG: {{ task_instance.dag_id }}<br>Task: {{ task_instance.task_id }}<br>Execution Time: {{ ts }}<br>Log URL: {{ task_instance.log_url }}",
        conn_id="smtp_default",
        trigger_rule="one_failed",
    )
