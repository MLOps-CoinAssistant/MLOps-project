import mlflow
from mlflow.tracking import MlflowClient


def check_model_performance(**context):
    ti = context["ti"]
    model_name = "btc_catboost_alpha"
    client = MlflowClient()

    # Get the latest production model version
    latest_version = None
    filter_string = f"name='{model_name}'"
    results = client.search_model_versions(filter_string)
    for mv in results:
        if mv.current_stage == "Production":
            latest_version = mv

    if not latest_version:
        raise Exception("Production model not found")

    run_id = latest_version.run_id
    f1_score = client.get_run(run_id).data.metrics["f1_score"]

    ti.xcom_push(key="f1_score", value=f1_score)
    return f1_score


def monitoring(**kwargs):
    ti = kwargs["ti"]
    f1_score = ti.xcom_pull(task_ids="check_model_performance", key="f1_score")
    if f1_score < 0.61:
        return "trigger_training"
    return "end_task"
