from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from info.connections import Connections
from sqlalchemy import text
import mlflow
import mlflow.pyfunc
import uvloop
import asyncio
import numpy as np
import pandas as pd
from mlflow.tracking import MlflowClient
from sklearn.metrics import f1_score


asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


async def load_latest_data(engine: AsyncSession):
    async with AsyncSession(engine) as session:
        query = text(
            """
            SELECT *
            FROM (
                SELECT *
                  FROM btc_preprocessed
                 ORDER BY time DESC
                 LIMIT 288
                ) AS subquery
            ORDER BY time ASC;
            """
        )
        result = await session.execute(query)
        data = result.fetchall()
        await session.commit()

        columns = result.keys()
        df = pd.DataFrame(data, columns=columns)
        features = df.drop(columns=["time", "label"])
        label = df["label"]

        print(f"Loaded data: {df}")
        print(f"Features: {features}")
        print(f"Label: {label}")

    return features, label


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
            break

    if not latest_version:
        raise Exception("Production model not found")

    run_id = latest_version.run_id
    run = client.get_run(run_id)
    production_f1_score = run.data.metrics.get("f1_score")

    if production_f1_score is None:
        raise Exception("f1_score metric not found for the production model")

    hook: PostgresHook = PostgresHook(
        postgres_conn_id=Connections.POSTGRES_DEFAULT.value
    )
    db_uri = hook.get_uri()
    engine = create_async_engine(
        db_uri.replace("postgresql", "postgresql+asyncpg"), future=True
    )
    latest_data, label = asyncio.run(load_latest_data(engine))
    print(f"latest_data to numpy: {latest_data}")
    print(f"latest_data.columns: {latest_data.columns.tolist()}")  # 컬럼명 확인
    print(f"latest_data.values: {latest_data.values}")  # 데이터 내용 확인

    model_uri = f"models:/{model_name}/{latest_version.version}"
    model = mlflow.pyfunc.load_model(model_uri)
    print(f"model : {model}")
    print(f"label : {label}")

    catboost_model = model._model_impl.cb_model
    print(f"CatBoost model features: {catboost_model.feature_names_}")

    pred = model.predict(latest_data)
    print(f"pred : {pred}")

    # 예측 값의 분포 확인
    print(f"Expected label distribution: {np.bincount(pred)}")

    f1 = f1_score(label, pred, average="micro")
    print(f"f1_score : {f1}")
    # run_id = latest_version.run_id
    # f1_score = client.get_run(run_id).data.metrics["f1_score"]

    ti.xcom_push(key="f1_score_prod", value=production_f1_score)
    ti.xcom_push(key="f1_score", value=f1)


def monitoring(**kwargs):
    ti = kwargs["ti"]
    f1_score = ti.xcom_pull(task_ids="check_model_performance", key="f1_score")
    f1_score_prod = ti.xcom_pull(
        task_ids="check_model_performance", key="f1_score_prod"
    )

    # 프로덕션 모델의 f1_score보다 20% 이상 성능이 차이날 때 모델학습 파이프라인 트리거
    threshold = 0.2

    if (f1_score_prod - f1_score) / f1_score_prod > threshold:
        print(
            "More than 20 \% lower than the f1-score of the production server . trigger model_training_pipeline"
        )
        return "trigger_training"
    return "end_task"
