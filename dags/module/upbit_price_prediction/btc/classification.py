import pandas as pd
from catboost import CatBoostClassifier, Pool
from sklearn.metrics import accuracy_score, classification_report, f1_score
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook
import optuna
from optuna.storages import RDBStorage
import mlflow
from mlflow.tracking import MlflowClient
from mlflow.store.artifact.runs_artifact_repo import RunsArtifactRepository
from info.connections import Connections

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def train_catboost_model_fn(
    hook: PostgresHook = PostgresHook(
        postgres_conn_id=Connections.POSTGRES_DEFAULT.value
    ),
    **context: dict,
) -> None:
    mlflow.set_experiment("btc_catboost_model")

    # 데이터 로드
    engine = create_engine(hook.get_uri())
    Query = """
        SELECT * FROM btc_preprocessed
    """

    with Session(bind=engine) as session:
        result = session.execute(Query)
        df = pd.DataFrame(result.all(), columns=result.keys())

    X = df.drop(columns=["label"])
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
            "depth": trial.suggest_int("depth", 4, 10),
            "l2_leaf_reg": trial.suggest_float("l2_leaf_reg", 1e-5, 10, log=True),
            "bagging_temperature": trial.suggest_float("bagging_temperature", 0.0, 1.0),
            "random_strength": trial.suggest_float(
                "random_strength", 1e-5, 10, log=True
            ),
            "od_type": "Iter",
            "od_wait": 100,
        }

        model = CatBoostClassifier(**params, logging_level="Info")
        model.fit(train_pool, eval_set=valid_pool, early_stopping_rounds=50)
        preds = model.predict(valid_pool)
        return f1_score(y_valid, preds, average="micro")

    # Optuna 설정
    postgresHook: PostgresHook = PostgresHook(
        postgres_conn_id=Connections.HYPERPARAMETER_STORE.value
    )
    storage: RDBStorage = RDBStorage(url=postgresHook.get_uri())
    study_name = "btc_catboost_model"
    study: optuna.study.Study = optuna.create_study(
        study_name=study_name,
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
    accuracy = accuracy_score(y_valid, preds)
    f1 = f1_score(y_valid, preds, average="micro")
    report = classification_report(y_valid, preds)

    logger.info(f"Validation accuracy: {accuracy}")
    logger.info(f"F1 Score: {f1}")
    logger.info(f"Classification Report:\n{report}")

    metrics = {
        "accuracy": accuracy,
        "f1_score": f1,
    }

    with mlflow.start_run() as run:
        mlflow.log_params(best_params)
        mlflow.log_metrics(metrics)
        model_info = mlflow.catboost.log_model(model, "model")

        # 모델을 등록하는 부분
        try:
            model_uri = f"runs:/{run.info.run_id}/model"
            registered_model = mlflow.register_model(model_uri, "btc_catboost_model")
            logger.info(
                f"Model registered: {registered_model.name}, version: {registered_model.version}"
            )
        except Exception as e:
            logger.error(f"Model registration failed: {str(e)}")
            raise

        context["ti"].xcom_push(key="run_id", value=run.info.run_id)
        context["ti"].xcom_push(key="model_uri", value=model_uri)
        context["ti"].xcom_push(key="eval_metric", value="f1_score")
        context["ti"].xcom_push(
            key="registered_model_version", value=registered_model.version
        )
        logger.info(
            f"Model trained and logged, run_id: {run.info.run_id}, model_uri: {model_uri}, registered_model_version: {registered_model.version}"
        )


def create_model_version(model_name: str, **context: dict) -> None:
    run_id = context["ti"].xcom_pull(key="run_id")
    model_uri = context["ti"].xcom_pull(key="model_uri")
    eval_metric = context["ti"].xcom_pull(key="eval_metric")
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

    context["ti"].xcom_push(key="model_version", value=model_version.version)
    logger.info(f"Model version created: {model_version.version}")


def transition_model_stage(model_name: str, **context: dict) -> None:
    version = context["ti"].xcom_pull(key="model_version")
    eval_metric = context["ti"].xcom_pull(key="eval_metric")
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

    context["ti"].xcom_push(key="production_version", value=production_model.version)
    logger.info(f"Production model deployed: version {production_model.version}")
