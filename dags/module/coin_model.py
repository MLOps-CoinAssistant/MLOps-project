from airflow.providers.postgres.hooks.postgres import PostgresHook
from dags.module.info.connections import Connections
from sqlalchemy import create_engine, text
from sklearn.model_selection import train_test_split, TimeSeriesSplit
from sklearn.preprocessing import StandardScaler, QuantileTransformer
from sklearn.metrics import mean_squared_error, r2_score
from sklearn.ensemble import RandomForestRegressor
from xgboost import XGBRegressor

from lightgbm import LGBMRegressor
from sqlalchemy.orm import Session
from typing import Tuple, List, Any

import optuna
import pandas as pd
import numpy as np
import time

"""
eda 결과
전체적으로 결측치는 존재하지 않았고, 시간순으로 잘 정렬되어있다.
거래량과 가격의 상관관계는 크지 않았다.
volume 컬럼에서 0인 데이터가 3개 발견되었고 이 부분은 삭제하기 보다는 누락되는 시간이 없도록 하기 위해 선형보간법으로 채워줄 예정
volume 컬럼에서 이상치라고 할 수 있는 큰 값들이 존재하긴 하지만 많지 않고, 거래량은 비트코인 특성상 큰 값도 유의미하다고 생각해서
삭제까진 하지 않고,  유지하고 스케일링만 할 예정
가격 변동성을 확인할 수 있는 컬럼을 추가해서 확인해 보았다.
일일 가격 변동폭 : df_x_train['price_diff'] = df_x_train['high'] - df_x_train['low']
일일 로그수익률 : df_x_train['log_return'] = np.log(df_x_train['close'] / df_x_train['close'].shift(1))
여기서 로그수익률 컬럼을 만드는 과정에서 첫번째 인덱스에 nan값이 들어감.
scatterplot 확인결과 대부분의 거래가 거래량이 낮은 거래량(1000이하)에서 이루어지고 있음.
3000 이상을 이상치로 판단하면 될것 같다.


전처리 단계에서 할 일

1. volume = 0 인 컬럼 선형보간법 적용하기
2. volume > 3000 이상인 값은 3000으로 일괄 적용한 후 로그스케일링 적용하기
3. 'price_diff', 'log_return' 컬럼 추가하기
4. time 컬럼은 year, month, day, hour, 등의 피쳐로 나눠서 시간의 주기성을 반영하기 위해
시간을 24시간을 주기로 사인과 코사인 변환하여 시간대 정보를 캡쳐
"""

# PostgreSQL 연결 정보를 환경 변수에서 가져옵니다.
postgres_hook = PostgresHook(postgres_conn_id=Connections.POSTGRES_DEFAULT.value)

# 데이터베이스 연결 엔진을 생성합니다.
engine = create_engine(postgres_hook.get_uri())

# 세션을 생성하여 데이터베이스 연결
with Session(bind=engine) as session:
    query = text("SELECT * FROM btc_ohlc")

    # 쿼리 실행 및 결과를 pandas 데이터프레임으로 변환
    result = session.execute(query)
    df = pd.DataFrame(result.all(), columns=result.keys())
    session.commit()


def preprocess(
    x_train: pd.DataFrame, x_test: pd.DataFrame
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    df_x_train = x_train.copy()
    df_x_test = x_test.copy()

    df_x_train = df_x_train.reset_index(drop=True)
    df_x_test = df_x_test.reset_index(drop=True)

    # 새로운 컬럼 생성
    df_x_train["price_diff"] = df_x_train["high"] - df_x_train["low"]
    df_x_test["price_diff"] = df_x_test["high"] - df_x_test["low"]

    df_x_train["log_return"] = np.log(
        df_x_train["close"] / df_x_train["close"].shift(1)
    )
    df_x_test["log_return"] = np.log(df_x_test["close"] / df_x_test["close"].shift(1))

    df_x_train["year"] = df_x_train["time"].dt.year
    df_x_train["month"] = df_x_train["time"].dt.month
    df_x_train["day"] = df_x_train["time"].dt.day
    df_x_train["hour"] = df_x_train["time"].dt.hour

    df_x_test["year"] = df_x_test["time"].dt.year
    df_x_test["month"] = df_x_test["time"].dt.month
    df_x_test["day"] = df_x_test["time"].dt.day
    df_x_test["hour"] = df_x_test["time"].dt.hour

    # 주기성 피처 생성 (24시간 주기)
    df_x_train["hour_sin"] = np.sin(2 * np.pi * df_x_train["hour"] / 24)
    df_x_train["hour_cos"] = np.cos(2 * np.pi * df_x_train["hour"] / 24)
    df_x_test["hour_sin"] = np.sin(2 * np.pi * df_x_test["hour"] / 24)
    df_x_test["hour_cos"] = np.cos(2 * np.pi * df_x_test["hour"] / 24)

    # time 컬럼 제거
    df_x_train.drop(columns=["time"], inplace=True)
    df_x_test.drop(columns=["time"], inplace=True)

    # 결측치 처리
    df_x_train["log_return"].fillna(df_x_train["log_return"].mean(), inplace=True)
    df_x_test["log_return"].fillna(df_x_test["log_return"].mean(), inplace=True)

    # 이상치 처리
    df_x_train["volume"] = df_x_train["volume"].apply(lambda x: 3000 if x > 3000 else x)
    df_x_test["volume"] = df_x_test["volume"].apply(lambda x: 3000 if x > 3000 else x)

    # 스케일링을 위한 피쳐 분리
    features = df_x_train.columns
    features_without_volume = features.drop("volume")

    # 스케일링
    scaler = StandardScaler()
    df_x_train[features_without_volume] = scaler.fit_transform(
        df_x_train[features_without_volume]
    )
    df_x_test[features_without_volume] = scaler.transform(
        df_x_test[features_without_volume]
    )

    # Quantile 스케일링 적용 (volume)
    # Quantile 변환은 가장 자주 발생하는 값(the most frequent values.) 주위로 분포를 조정하며,
    # 이상치의 영향을 감소시켜주는 특징도 있습니다
    q_scaler = QuantileTransformer(output_distribution="normal")
    df_x_train["volume"] = q_scaler.fit_transform(
        df_x_train["volume"].values.reshape(-1, 1)
    )
    df_x_test["volume"] = q_scaler.transform(df_x_test["volume"].values.reshape(-1, 1))

    return df_x_train, df_x_test


def split_data(
    df: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.Series, pd.Series, pd.Series]:
    # train:test = 8:2 로 분리
    label = df["close"]
    test_size = 0.2

    # train:test = 8:2 로 분리. 시계열 데이터의 시간 순서는 중요하기 때문에 shuffle=False
    x_train, x_test, y_train, y_test = train_test_split(
        df, label, test_size=test_size, shuffle=False
    )

    return x_train, x_test, y_train, y_test


# XGBRegressor, RandomForestRegressor, LGBMRegressor
def objective(trial, model_class, x_train: pd.DataFrame, y_train: pd.Series) -> float:
    if model_class == XGBRegressor:
        params = {
            "n_estimators": trial.suggest_int("n_estimators", 50, 200),
            "learning_rate": trial.suggest_float("learning_rate", 0.001, 0.1, log=True),
            "max_depth": trial.suggest_int("max_depth", 2, 10),
            "subsample": trial.suggest_float("subsample", 0.5, 1.0),
            "colsample_bytree": trial.suggest_float("colsample_bytree", 0.5, 1.0),
            "gamma": trial.suggest_float("gamma", 0, 10),
            "alpha": trial.suggest_float("alpha", 1, 10),
            "lambda": trial.suggest_float("lambda", 1, 10),
        }
    elif model_class == RandomForestRegressor:
        params = {
            "n_estimators": trial.suggest_int("n_estimators", 50, 200),
            "max_depth": trial.suggest_int("max_depth", 2, 10),
            "min_samples_split": trial.suggest_int("min_samples_split", 2, 10),
            "min_samples_leaf": trial.suggest_int("min_samples_leaf", 1, 10),
        }
    elif model_class == LGBMRegressor:
        params = {
            "n_estimators": trial.suggest_int("n_estimators", 50, 200),
            "learning_rate": trial.suggest_float("learning_rate", 0.001, 0.1, log=True),
            "max_depth": trial.suggest_int("max_depth", 2, 10),
            "num_leaves": trial.suggest_int("num_leaves", 20, 50),
            "min_child_samples": trial.suggest_int("min_child_samples", 5, 20),
            "gamma": trial.suggest_float("gamma", 0, 10),
            "alpha": trial.suggest_float("alpha", 1, 10),
            "lambda": trial.suggest_float("lambda", 1, 10),
        }

    val_scores_mse = []
    val_scores_r2 = []
    tscv = TimeSeriesSplit(n_splits=3)

    for trn_idx, val_idx in tscv.split(x_train):
        x_trn, y_trn = x_train.iloc[trn_idx], y_train.iloc[trn_idx]
        x_val, y_val = x_train.iloc[val_idx], y_train.iloc[val_idx]

        model = model_class(**params)

        if model_class == XGBRegressor:
            model.fit(
                x_trn,
                y_trn,
                eval_set=[(x_val, y_val)],
                verbose=False,
                early_stopping_rounds=10,
            )
        elif model_class == LGBMRegressor:
            model.fit(x_trn, y_trn, eval_set=[(x_val, y_val)], eval_metric="rmse")
        else:
            model.fit(x_trn, y_trn)

        y_val_pred = model.predict(x_val)
        val_mse = mean_squared_error(y_val, y_val_pred)
        val_r2 = r2_score(y_val, y_val_pred)

        val_scores_mse.append(val_mse)
        val_scores_r2.append(val_r2)

    mean_mse = np.mean(val_scores_mse)
    mean_r2 = np.mean(val_scores_r2)

    trial.set_user_attr("mean_mse", mean_mse)
    trial.set_user_attr("mean_r2", mean_r2)

    return mean_mse


# 로깅 콜백 클래스 정의
class LoggingCallback:
    def __init__(self, n_trials: int):
        self.n_trials = n_trials

    def __call__(self, study, trial):
        trial_number = trial.number + 1
        print(
            f"Trial {trial_number}/{self.n_trials} completed. Best value (min MSE): {study.best_value:.4f}"
        )


# 튜닝 함수
def tune_model(model_class, x_train, y_train, n_trials=30):
    study = optuna.create_study(direction="minimize")  # MSE 최소화
    logging_callback = LoggingCallback(n_trials)  # 로깅 콜백 설정
    study.optimize(
        lambda trial: objective(trial, model_class, x_train, y_train),
        n_trials=n_trials,
        callbacks=[logging_callback],
    )
    best_params = study.best_trial.params
    best_model = model_class(**best_params)
    best_model.fit(x_train, y_train)
    return best_model


# OOF 앙상블 예측 함수
def oof_predict(
    models: List[Any], x_train: pd.DataFrame, x_test: pd.DataFrame, y_train: pd.Series
) -> Tuple[np.ndarray, np.ndarray]:
    tscv = TimeSeriesSplit(n_splits=3)
    oof_train = np.zeros((x_train.shape[0], len(models)))
    oof_test = np.zeros((x_test.shape[0], len(models)))
    oof_test_skf = np.empty((3, x_test.shape[0], len(models)))

    for i, model in enumerate(models):
        for j, (train_idx, val_idx) in enumerate(tscv.split(x_train)):
            x_trn, y_trn = x_train.iloc[train_idx], y_train.iloc[train_idx]
            x_val, y_val = x_train.iloc[val_idx], y_train.iloc[val_idx]

            model.fit(x_trn, y_trn)
            oof_train[val_idx, i] = model.predict(x_val)
            oof_test_skf[j, :, i] = model.predict(x_test)

        oof_test[:, i] = oof_test_skf[:, :, i].mean(axis=0)

    return oof_train, oof_test


# 메인으로 실행 될 함수
def predict() -> None:
    # 데이터 분리 함수 호출
    start_time = time.time()
    x_train, x_test, y_train, y_test = split_data(df)

    # 데이터 전처리
    x_train, x_test = preprocess(x_train, x_test)

    n_trials = 10  # 시도 횟수
    logging_callback = LoggingCallback(n_trials)  # 로깅 콜백 설정

    models = {
        "XGB": XGBRegressor,
        "RandomForest": RandomForestRegressor,
        "LGBM": LGBMRegressor,
    }

    tuned_models = []
    for model_name, model_class in models.items():
        print(f"Tuning {model_name}...")
        tuned_model = tune_model(model_class, x_train, y_train, n_trials)
        tuned_models.append(tuned_model)
        print(f"Best parameters for {model_name}: {tuned_model.get_params()}")

    # Out-of-Fold 앙상블
    oof_train, oof_test = oof_predict(tuned_models, x_train, x_test, y_train)

    # 앙상블 예측
    ensemble_preds = oof_test.mean(axis=1)

    # 성능 평가
    test_mse = mean_squared_error(y_test, ensemble_preds)
    test_r2 = r2_score(y_test, ensemble_preds)

    print(f"Ensemble Test Mean Squared Error: {test_mse:.4f}")
    print(f"Ensemble Test R-squared: {test_r2:.4f}")

    elapsed_time = time.time() - start_time
    print(f"Elapsed time for modeling task: {elapsed_time:.2f} seconds")
