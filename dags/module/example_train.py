from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.experimental import enable_iterative_imputer
from sklearn.impute import IterativeImputer
from sklearn.preprocessing import StandardScaler, OneHotEncoder, LabelEncoder
from sklearn.ensemble import RandomForestClassifier
from sklearn.compose import ColumnTransformer
from sklearn.metrics import f1_score
from sklearn.pipeline import Pipeline
from sklearn.datasets import load_iris
import optuna
from optuna.storages import RDBStorage
import mlflow
from mlflow.tracking import MlflowClient
from mlflow.store.artifact.runs_artifact_repo import RunsArtifactRepository
from dags.module.info.connections import AirflowConnections


# def train_fn(**context):
#     mlflow.set_experiment("hospital_model")
#     hook = PostgresHook(postgres_conn_id="postgres_default")
#     postgresHook = PostgresHook(postgres_conn_id="postgres-dev-optuna")
#     conn = hook.get_conn()
#     stmt = """
#             SELECT *
#               FROM hospital_train
#             """
#     data = pd.read_sql(stmt, conn)
#     label = data["OC"]
#     data.drop(columns=["OC", "inst_id", "openDate"], inplace=True)
#     x_train, x_valid, y_train, y_valid = train_test_split(
#         data, label, test_size=0.3, shuffle=True, stratify=label
#     )
#     x_train = x_train.reset_index(drop=True)
#     x_valid = x_valid.reset_index(drop=True)
#     cat_columns = data.select_dtypes(include="object").columns
#     num_columns = data.select_dtypes(exclude="object").columns
#     print("Categorical columns: ", cat_columns)
#     print("Numerical columns: ", num_columns)
#     preprocessor = ColumnTransformer(
#         transformers=[
#             ("impute", IterativeImputer(), num_columns),
#             ("scaler", StandardScaler(), num_columns),
#             ("encoding", OneHotEncoder(handle_unknown="ignore", sparse_output=False), cat_columns),
#         ]
#     )
#     le = LabelEncoder()
#     y_train = le.fit_transform(y_train)
#     y_valid = le.transform(y_valid)
#     x_train = preprocessor.fit_transform(x_train)
#     x_valid = preprocessor.transform(x_valid)
#     def objective(trial):
#         n_estimators = trial.suggest_int("n_estimators", 2, 100)
#         max_depth = int(trial.suggest_int("max_depth", 1, 32))
#         model = RandomForestClassifier(n_estimators=n_estimators, max_depth=max_depth)
#         model.fit(x_train, y_train)
#         return f1_score(y_valid, model.predict(x_valid))
#     storage = RDBStorage(url=postgresHook.get_uri())
#     study = optuna.create_study(
#         study_name="hospital_model",
#         direction="maximize",
#         storage=storage,
#         load_if_exists=True,
#     )
#     study.optimize(objective, n_trials=10)
#     best_params = study.best_params
#     best_metric = study.best_value
#     print("Best params: ", best_params)
#     print("Best metric: ", best_metric)
#     model = RandomForestClassifier(**best_params)
#     model.fit(x_train, y_train)
#     print("Validation Score: ", f1_score(y_valid, model.predict(x_valid)))
#     pipeline = Pipeline([("preprocessor", preprocessor), ("model", model)])
#     metrics = {
#         "f1_score": f1_score(y_valid, model.predict(x_valid)),
#     }
#     with mlflow.start_run():
#         mlflow.log_params(best_params)
#         mlflow.log_metrics(metrics)
#         model_info = mlflow.sklearn.log_model(pipeline, "model")
#     context["ti"].xcom_push(key="run_id", value=model_info.run_id)
#     context["ti"].xcom_push(key="model_uri", value=model_info.model_uri)
#     context["ti"].xcom_push(key="eval_metric", value="f1_score")
#     print(
#         f"Done Train model, run_id: {model_info.run_id}, model_uri: {model_info.model_uri}"
#     )


def train_fn_iris(
    hook=PostgresHook(postgres_conn_id=AirflowConnections.DEVELOPMENT.value), **context
):
    mlflow.set_experiment("iris_model")
    iris = load_iris()
    data = iris.data
    label = iris.target
    x_train, x_valid, y_train, y_valid = train_test_split(
        data, label, test_size=0.3, shuffle=True, stratify=label
    )

    def objective(trial):
        n_estimators = trial.suggest_int("n_estimators", 2, 100)
        max_depth = int(trial.suggest_int("max_depth", 1, 32))
        model = RandomForestClassifier(n_estimators=n_estimators, max_depth=max_depth)
        model.fit(x_train, y_train)
        return f1_score(y_valid, model.predict(x_valid), average="micro")

    postgresHook = PostgresHook(
        postgres_conn_id=AirflowConnections.DEVELOPMENT_HYPERPARAMETER_STORE.value
    )
    storage = RDBStorage(url=postgresHook.get_uri())
    study = optuna.create_study(
        study_name="iris_model",
        direction="maximize",
        storage=storage,
        load_if_exists=True,
    )
    study.optimize(objective, n_trials=10)
    best_params = study.best_params
    best_metric = study.best_value
    print("Best params: ", best_params)
    print("Best metric: ", best_metric)
    model = RandomForestClassifier(**best_params)
    model.fit(x_train, y_train)
    print(
        "Validation Score: ", f1_score(y_valid, model.predict(x_valid), average="micro")
    )
    metrics = {
        "f1_score": f1_score(y_valid, model.predict(x_valid), average="micro"),
    }
    with mlflow.start_run():
        mlflow.log_params(best_params)
        mlflow.log_metrics(metrics)
        model_info = mlflow.sklearn.log_model(model, "model")
    context["ti"].xcom_push(key="run_id", value=model_info.run_id)
    context["ti"].xcom_push(key="model_uri", value=model_info.model_uri)
    context["ti"].xcom_push(key="eval_metric", value="f1_score")
    print(
        f"Done Train model, run_id: {model_info.run_id}, model_uri: {model_info.model_uri}"
    )


def create_model_version(model_name: str, **context):
    run_id = context["ti"].xcom_pull(key="run_id")
    model_uri = context["ti"].xcom_pull(key="model_uri")
    eval_metric = context["ti"].xcom_pull(key="eval_metric")
    client = MlflowClient()
    try:
        client.create_registered_model(model_name)
    except Exception as e:
        print("Model already exists")
    current_metric = client.get_run(run_id).data.metrics[eval_metric]
    model_source = RunsArtifactRepository.get_underlying_uri(model_uri)
    model_version = client.create_model_version(
        model_name, model_source, run_id, description=f"{eval_metric}: {current_metric}"
    )
    context["ti"].xcom_push(key="model_version", value=model_version.version)
    print(f"Done Create model version, model_version: {model_version}")


def transition_model_stage(model_name: str, **context):
    version = context["ti"].xcom_pull(key="model_version")
    eval_metric = context["ti"].xcom_pull(key="eval_metric")
    client = MlflowClient()
    production_model = None
    current_model = client.get_model_version(model_name, version)
    filter_string = f"name='{current_model.name}'"
    results = client.search_model_versions(filter_string)
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
    print(
        f"Done Deploy Production_Model, production_version: {production_model.version}"
    )
