from dags.module.upbit_price_prediction.btc.create_table import BtcPreprocessed

from airflow.providers.postgres.hooks.postgres import PostgresHook
from catboost import CatBoostClassifier, Pool
from sklearn.metrics import accuracy_score, classification_report, f1_score
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy import select
from optuna.storages import RDBStorage
from mlflow.tracking import MlflowClient
from mlflow.store.artifact.runs_artifact_repo import RunsArtifactRepository
from info.connections import Connections
from datetime import datetime
import asyncio
import logging
import optuna
import pandas as pd
import mlflow
import uvloop
import time
import numpy as np
from sklearn.inspection import permutation_importance


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
    db_uri = ti.xcom_pull(key="db_uri", task_ids="create_table_fn")
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
    eval_metric = ti.xcom_pull(key="eval_metric")
    client = MlflowClient()

    current_model = client.get_model_version(model_name, version)
    filter_string = f"name='{current_model.name}'"
    results = client.search_model_versions(filter_string)

    production_model = None
    for mv in results:
        if mv.current_stage == "Production":
            production_model = mv

    if production_model is None:
        client.transition_model_version_stage(
            current_model.name, current_model.version, "Production"
        )
        production_model = current_model
    else:
        current_metric = client.get_run(current_model.run_id).data.metrics[eval_metric]
        production_metric = client.get_run(production_model.run_id).data.metrics[
            eval_metric
        ]

        if current_metric > production_metric:
            client.transition_model_version_stage(
                current_model.name,
                current_model.version,
                "Production",
                archive_existing_versions=True,
            )
            production_model = current_model

    ti.xcom_push(key="production_version", value=production_model.version)
    logger.info(f"Production model deployed: version {production_model.version}")
    e = time.time()
    es = e - s
    logger.info(f"Total working time : {es:.4f} sec")


# Permutation Importance 계산
def get_importance(**context):
    # TODO: DB에 XAI 결과를 저장할 테이블이 있는지 검사하고, 없다면 새로 만들어서 저장하는 로직 추가
    ti = context["ti"]
    model_uri = ti.xcom_pull(key="model_uri")
    db_uri = ti.xcom_pull(key="db_uri", task_ids="create_table_fn")

    engine = create_async_engine(
        db_uri.replace("postgresql", "postgresql+asyncpg"), future=True
    )

    df = asyncio.run(load_data(engine))
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

    logger.info(f"Permutation Importance:\n{perm_importance_df}")
