from airflow.providers.postgres.hooks.postgres import PostgresHook
from catboost import CatBoostClassifier, Pool
from sklearn.metrics import accuracy_score, classification_report, f1_score
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession

from optuna.storages import RDBStorage
from mlflow.tracking import MlflowClient
from mlflow.store.artifact.runs_artifact_repo import RunsArtifactRepository
from info.connections import Connections
from datetime import datetime, timezone
import asyncio
import logging
import optuna
import pandas as pd
import mlflow
import uvloop
import time
import numpy as np
from sklearn.inspection import permutation_importance
import pytz
from sqlalchemy import text
from info.minio_config import MinioConfig
import boto3
from urllib.parse import urlparse


# uvloop를 기본 이벤트 루프로 설정
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def load_data(engine: AsyncSession) -> pd.DataFrame:
    # 이 파일에서는 비동기작업은 여기서 딱1번 하기 때문에 asyncscopedsession, sessionmaker등은 불필요
    async with AsyncSession(engine) as session:
        query = """
        SELECT * FROM (
            SELECT * FROM btc_preprocessed
            ORDER BY time DESC
        ) AS subquery
        ORDER BY time ASC;
        """
        result = await session.execute(query)
        data = result.fetchall()

        columns = result.keys()
        df = pd.DataFrame(data, columns=columns)
    return df


def train_catboost_model_fn(**context: dict) -> None:
    s = time.time()
    study_and_experiment_name = "btc_catboost_alpha"
    mlflow.set_experiment(study_and_experiment_name)
    experiment = mlflow.get_experiment_by_name(study_and_experiment_name)
    experiment_id = experiment.experiment_id
    run_name = f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    ti = context["ti"]
    hook: PostgresHook = PostgresHook(
        postgres_conn_id=Connections.POSTGRES_DEFAULT.value
    )
    db_uri = hook.get_uri()
    ti.xcom_push(key="db_uri", value=db_uri)
    # 데이터 로드
    engine = create_async_engine(
        db_uri.replace("postgresql", "postgresql+asyncpg"), future=True
    )

    df = asyncio.run(load_data(engine))

    X = df.drop(columns=["label", "time"])
    y = df["label"]

    # 시계열 데이터를 시간 순서에 따라 분할
    split_index = int(len(df) * 0.9)  # 90%는 훈련 데이터, 10%는 테스트 데이터
    X_train, X_valid = X[:split_index], X[split_index:]
    y_train, y_valid = y[:split_index], y[split_index:]

    train_pool = Pool(X_train, y_train)
    valid_pool = Pool(X_valid, y_valid)

    # Optuna를 사용한 하이퍼파라미터 최적화
    def objective(trial: optuna.trial.Trial) -> float:
        params = {
            "iterations": trial.suggest_int("iterations", 100, 1000),
            "learning_rate": trial.suggest_float("learning_rate", 1e-5, 1e-1, log=True),
            "depth": trial.suggest_int("depth", 6, 12),
            "l2_leaf_reg": trial.suggest_float("l2_leaf_reg", 1e-5, 10, log=True),
            "bagging_temperature": trial.suggest_float(
                "bagging_temperature", 0.0, 0.1
            ),  # bagging_temperature를 0에 가깝게 설정하여 데이터의 무작위성을 최소화한다.
            "border_count": trial.suggest_int("border_count", 32, 255),  # 분할 수 설정
            "feature_border_type": trial.suggest_categorical(
                "feature_border_type",
                [
                    "Median",
                    "Uniform",
                    "UniformAndQuantiles",
                    "MaxLogSum",
                    "MinEntropy",
                    "GreedyLogSum",
                ],
            ),  # 경계 유형 설정
            "random_strength": trial.suggest_float(
                "random_strength", 1e-3, 10, log=True
            ),  # 랜덤화 강도 조절
        }

        model = CatBoostClassifier(**params, logging_level="Silent")
        model.fit(train_pool, eval_set=valid_pool, early_stopping_rounds=50)
        preds = model.predict(valid_pool)

        # 클래스 분포 확인
        class_distribution = np.bincount(y_valid)
        imbalance_ratio = class_distribution.max() / class_distribution.min()

        # 불균형 정도에 따라 macro, micro 설정
        if imbalance_ratio > 1.5:  # 임계값은 데이터셋에 따라 조정
            average = "macro"
        else:
            average = "micro"

        return f1_score(y_valid, preds, average=average)

    # Optuna 설정
    postgresHook: PostgresHook = PostgresHook(
        postgres_conn_id=Connections.HYPERPARAMETER_STORE.value
    )
    storage: RDBStorage = RDBStorage(url=postgresHook.get_uri())
    study: optuna.study.Study = optuna.create_study(
        study_name=study_and_experiment_name,
        direction="maximize",
        storage=storage,
        load_if_exists=True,
    )
    study.optimize(objective, n_trials=5)

    best_params = study.best_params
    best_metric = study.best_value
    logger.info(f"Best params: {best_params}")
    logger.info(f"Best metric: {best_metric}")

    # 최적 하이퍼파라미터로 모델 학습
    model = CatBoostClassifier(**best_params, logging_level="Silent")
    model.fit(train_pool)

    # 평가 및 로그
    preds = model.predict(valid_pool)
    proba = model.predict_proba(valid_pool)[:, 1]
    average_proba = proba.mean()  # 예측 확률의 평균
    accuracy = accuracy_score(y_valid, preds)
    f1 = f1_score(y_valid, preds, average="micro")
    report = classification_report(y_valid, preds)

    logger.info(f"Validation accuracy: {accuracy}")
    logger.info(f"F1 Score: {f1}")
    logger.info(f"Classification Report:\n{report}")

    metrics = {
        "accuracy": accuracy,
        "f1_score": f1,
        "average_proba": average_proba,
    }

    with mlflow.start_run(experiment_id=experiment_id, run_name=run_name) as run:
        mlflow.log_params(best_params)
        mlflow.log_metrics(metrics)
        model_info = mlflow.catboost.log_model(model, "model")
        logger.info(f" model_info: {model_info}")

        # 모델을 등록하는 부분
        try:
            model_uri = f"runs:/{run.info.run_id}/model"
            registered_model = mlflow.register_model(
                model_uri, study_and_experiment_name
            )
            logger.info(
                f"Model registered: {registered_model.name}, version: {registered_model.version}"
            )
        except Exception as e:
            logger.error(f"Model registration failed: {str(e)}")
            raise
        ti.xcom_push(key="model_name", value=study_and_experiment_name)
        ti.xcom_push(key="run_id", value=run.info.run_id)
        ti.xcom_push(key="model_uri", value=model_uri)
        ti.xcom_push(key="eval_metric", value="f1_score")
        ti.xcom_push(key="registered_model_version", value=registered_model.version)
        logger.info(
            f"Model trained and logged, run_id: {run.info.run_id}, model_uri: {model_uri}, registered_model_version: {registered_model.version}"
        )
        e = time.time()
        es = e - s
        logger.info(f"Total working time : {es:.4f} sec")


def create_model_version(**context: dict) -> None:
    s = time.time()
    ti = context["ti"]
    model_name: str = ti.xcom_pull(key="model_name")
    run_id = ti.xcom_pull(key="run_id")
    model_uri = ti.xcom_pull(key="model_uri")
    eval_metric = ti.xcom_pull(key="eval_metric")
    client = MlflowClient()

    try:
        client.create_registered_model(model_name)
    except Exception:
        logger.info("Model already exists")

    current_metric = client.get_run(run_id).data.metrics[eval_metric]
    model_source = RunsArtifactRepository.get_underlying_uri(model_uri)
    model_version = client.create_model_version(
        model_name, model_source, run_id, description=f"{eval_metric}: {current_metric}"
    )

    ti.xcom_push(key="model_version", value=model_version.version)
    logger.info(f"Model version created: {model_version.version}")
    e = time.time()
    es = e - s
    logger.info(f"Total working time : {es:.4f} sec")


def transition_model_stage(**context: dict) -> None:
    s = time.time()
    ti = context["ti"]
    model_name: str = ti.xcom_pull(key="model_name")
    version = ti.xcom_pull(key="model_version")
    eval_metric = ti.xcom_pull(key="eval_metric")  # eval_metric은 f1 score로 되어 있다.
    client = MlflowClient()

    current_model = client.get_model_version(model_name, version)
    filter_string = f"name='{current_model.name}'"
    results = client.search_model_versions(filter_string)

    production_model = None
    for mv in results:
        if mv.current_stage == "Production":
            production_model = mv

    current_metric = client.get_run(current_model.run_id).data.metrics[eval_metric]

    if production_model is None:
        client.transition_model_version_stage(
            current_model.name, current_model.version, "Production"
        )
        production_model = current_model
        ti.xcom_push(key="production_version", value=production_model.version)
        logger.info(f"Production model deployed: version {production_model.version}")
    else:
        production_metric = client.get_run(production_model.run_id).data.metrics[
            eval_metric
        ]

        # 최근 경향을 최대한 반영하기 위해 7일마다 교체하도록 설정 (과거의 모델 성능이 최고점인 상태가 오래 지속될 경우 최근 데이터 경향을 반영못할 가능성때문)
        update_period_days = 7
        last_update_timestamp = (
            production_model.creation_timestamp / 1000
        )  # 프로덕션 모델의 생성 기준으로 timestamp불러옴. 시간이 밀리초여서 이걸 초로 변환
        last_update_date = datetime.fromtimestamp(
            last_update_timestamp, tz=timezone.utc
        )
        current_date = datetime.now(timezone.utc)
        days_since_last_update = (
            current_date - last_update_date
        ).days  # 모델생성 기준으로 현재시간과의 차이를 계산

        if (
            current_metric > production_metric
            or days_since_last_update >= update_period_days
        ):
            client.transition_model_version_stage(
                current_model.name,
                current_model.version,
                "Production",
                archive_existing_versions=True,
            )
            production_model = current_model
            ti.xcom_push(key="production_version", value=production_model.version)
            logger.info(
                f"Production model deployed: version {production_model.version}"
            )
        elif current_metric >= 0.610:
            client.transition_model_version_stage(
                current_model.name,
                current_model.version,
                "Staging",
                archive_existing_versions=True,
            )
            logger.info(f"Candidate model registered: version {current_model.version}")
        else:
            # Run 정보 가져오기
            run_info = client.get_run(current_model.run_id)
            # 아티팩트 경로 추출
            artifact_uri = run_info.info.artifact_uri
            # MinIO에서 아티팩트 삭제

            parsed_uri = urlparse(artifact_uri)
            bucket_name = parsed_uri.netloc
            artifact_path = parsed_uri.path.lstrip("/")

            s3 = boto3.client(
                "s3",
                endpoint_url=MinioConfig.MINIO_SERVER_URL.value,  # MinIO 엔드포인트
                aws_access_key_id=MinioConfig.MINIO_ACCESS_KEY.value,
                aws_secret_access_key=MinioConfig.MINIO_SECRET_KEY.value,
            )

            # 아티팩트 목록 가져오기
            objects_to_delete = s3.list_objects_v2(
                Bucket=bucket_name, Prefix=artifact_path
            )
            delete_keys = [
                {"Key": obj["Key"]} for obj in objects_to_delete.get("Contents", [])
            ]

            if delete_keys:
                s3.delete_objects(Bucket=bucket_name, Delete={"Objects": delete_keys})
                logger.info(f"Artifacts deleted from MinIO: {artifact_path}")
            else:
                logger.info("Artifacts not found.")

    e = time.time()
    es = e - s
    logger.info(f"Total working time : {es:.4f} sec")


# Permutation Importance 계산
async def get_importance_async(**context):
    ti = context["ti"]
    model_uri = ti.xcom_pull(key="model_uri")
    db_uri = ti.xcom_pull(key="db_uri", task_ids="train_catboost_model")
    experiment_name = ti.xcom_pull(key="model_name")
    run_id = ti.xcom_pull(key="run_id")

    engine = create_async_engine(
        db_uri.replace("postgresql", "postgresql+asyncpg"), future=True
    )

    df = await load_data(engine)
    X = df.drop(columns=["label", "time"])
    y = df["label"]

    split_index = int(len(df) * 0.9)
    X_valid = X[split_index:]
    y_valid = y[split_index:]

    model = mlflow.catboost.load_model(model_uri)

    perm_importance = permutation_importance(
        model, X_valid, y_valid, n_repeats=10, random_state=42
    )
    perm_importance_df = pd.DataFrame(
        {
            "feature": X_valid.columns,
            "importance_mean": perm_importance.importances_mean,
            "importance_std": perm_importance.importances_std,
        }
    ).sort_values(by="importance_mean", ascending=False)

    perm_importance_df["experiment_name"] = experiment_name
    perm_importance_df["run_id"] = run_id
    perm_importance_df["time"] = datetime.now(tz=pytz.timezone("Asia/Seoul"))

    async with engine.begin() as conn:
        await conn.execute(
            text(
                f"""
            CREATE TABLE IF NOT EXISTS btc_importance (
                run_id VARCHAR PRIMARY KEY,
                experiment_name VARCHAR,
                time TIMESTAMP,
                {", ".join([f"{col} FLOAT" for col in perm_importance_df['feature']])}
            )
        """
            )
        )

        insert_query = text(
            f"""
            INSERT INTO btc_importance (run_id, experiment_name, time, {", ".join(perm_importance_df['feature'])})
            VALUES ('{run_id}', '{experiment_name}', '{datetime.now(tz=pytz.timezone('Asia/Seoul'))}', {", ".join(map(str, perm_importance_df['importance_mean']))})
        """
        )
        await conn.execute(insert_query)

    logger.info(f"Permutation Importance saved to database:\n{perm_importance_df}")


def get_importance(**context):
    loop = asyncio.get_event_loop()
    if loop.is_running():
        task = loop.create_task(get_importance_async(**context))
        loop.run_until_complete(task)
    else:
        loop.run_until_complete(get_importance_async(**context))
