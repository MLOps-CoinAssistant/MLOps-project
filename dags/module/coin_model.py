from airflow.providers.postgres.hooks.postgres import PostgresHook
from dags.module.info.connections import Connections
from sqlalchemy import create_engine, text
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
from sklearn.preprocessing import StandardScaler
from sklearn.preprocessing import QuantileTransformer
from sklearn.model_selection import KFold
from sklearn.metrics import mean_squared_error, r2_score
from xgboost import XGBRegressor
from sqlalchemy.orm import Session
from typing import Tuple

import pandas as pd
import numpy as np

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
    x_train: pd.DataFrame, x_valid: pd.DataFrame, x_test: pd.DataFrame
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    df_x_train = x_train.copy()
    df_x_valid = x_valid.copy()
    df_x_test = x_test.copy()

    df_x_train = df_x_train.reset_index(drop=True)
    df_x_valid = df_x_valid.reset_index(drop=True)
    df_x_test = df_x_test.reset_index(drop=True)

    # 새로운 컬럼 생성
    df_x_train["price_diff"] = df_x_train["high"] - df_x_train["low"]
    df_x_valid["price_diff"] = df_x_valid["high"] - df_x_valid["low"]
    df_x_test["price_diff"] = df_x_test["high"] - df_x_test["low"]

    df_x_train["log_return"] = np.log(
        df_x_train["close"] / df_x_train["close"].shift(1)
    )
    df_x_valid["log_return"] = np.log(
        df_x_valid["close"] / df_x_valid["close"].shift(1)
    )
    df_x_test["log_return"] = np.log(df_x_test["close"] / df_x_test["close"].shift(1))

    df_x_train["year"] = df_x_train["time"].dt.year
    df_x_train["month"] = df_x_train["time"].dt.month
    df_x_train["day"] = df_x_train["time"].dt.day
    df_x_train["hour"] = df_x_train["time"].dt.hour

    df_x_valid["year"] = df_x_valid["time"].dt.year
    df_x_valid["month"] = df_x_valid["time"].dt.month
    df_x_valid["day"] = df_x_valid["time"].dt.day
    df_x_valid["hour"] = df_x_valid["time"].dt.hour

    df_x_test["year"] = df_x_test["time"].dt.year
    df_x_test["month"] = df_x_test["time"].dt.month
    df_x_test["day"] = df_x_test["time"].dt.day
    df_x_test["hour"] = df_x_test["time"].dt.hour
    # 주기성 피처 생성 (24시간 주기)
    df_x_train["hour_sin"] = np.sin(2 * np.pi * df_x_train["hour"] / 24)
    df_x_train["hour_cos"] = np.cos(2 * np.pi * df_x_train["hour"] / 24)
    df_x_valid["hour_sin"] = np.sin(2 * np.pi * df_x_valid["hour"] / 24)
    df_x_valid["hour_cos"] = np.cos(2 * np.pi * df_x_valid["hour"] / 24)
    df_x_test["hour_sin"] = np.sin(2 * np.pi * df_x_test["hour"] / 24)
    df_x_test["hour_cos"] = np.cos(2 * np.pi * df_x_test["hour"] / 24)

    # time 컬럼 제거
    df_x_train.drop(columns=["time"], inplace=True)
    df_x_valid.drop(columns=["time"], inplace=True)
    df_x_test.drop(columns=["time"], inplace=True)

    # 결측치 처리
    # log_return 컬럼에서 NaN 값을 0으로 대체 (첫번째 행이 NaN값으로 옴)
    df_x_train["log_return"].fillna(0, inplace=True)
    df_x_valid["log_return"].fillna(0, inplace=True)
    df_x_test["log_return"].fillna(0, inplace=True)

    # 이상치 처리
    # volume이 3000 이상인 데이터는 3000으로 대체
    df_x_train["volume"] = df_x_train["volume"].apply(lambda x: 3000 if x > 3000 else x)
    df_x_valid["volume"] = df_x_valid["volume"].apply(lambda x: 3000 if x > 3000 else x)
    df_x_test["volume"] = df_x_test["volume"].apply(lambda x: 3000 if x > 3000 else x)

    # 스케일링을 위한 피쳐 분리
    features = df_x_train.columns
    features_without_volume = features.drop("volume")

    # 스케일링

    # Standard 스케일링 적용 (volume을 제외한 모든 피쳐)
    scaler = StandardScaler()
    df_x_train[features_without_volume] = scaler.fit_transform(
        df_x_train[features_without_volume]
    )
    df_x_valid[features_without_volume] = scaler.transform(
        df_x_valid[features_without_volume]
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
    df_x_valid["volume"] = q_scaler.transform(
        df_x_valid["volume"].values.reshape(-1, 1)
    )
    df_x_test["volume"] = q_scaler.transform(df_x_test["volume"].values.reshape(-1, 1))

    return df_x_train, df_x_valid, df_x_test


def split_data(
    df: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.Series, pd.Series, pd.Series]:
    # train : valid : test = 60 : 20 : 20 으로 분리
    label = df["close"]
    train_size = 0.6
    valid_size = 0.2
    test_size = 0.2

    # 쪼개어진 Train, Valid, Test 데이터의 비율은 (6:2:2), 내부 난수 값 42, 데이터를 쪼갤 때 섞으며 label 값으로 Stratify 하는 코드.
    # 먼저 train과 temp로 분리 (6:4)
    x_train, x_temp, y_train, y_temp = train_test_split(
        df, label, test_size=(1 - train_size), random_state=42, shuffle=True
    )
    # temp를 valid와 test로 분리 (6:2:2)
    x_valid, x_test, y_valid, y_test = train_test_split(
        x_temp,
        y_temp,
        test_size=(test_size / (test_size + valid_size)),
        random_state=42,
        shuffle=True,
    )
    return x_train, x_valid, x_test, y_train, y_valid, y_test


def XGboostRegressor(x_train: pd.DataFrame, y_train: pd.Series) -> XGBRegressor:
    # 모델 예측
    # XGboost Regressor 를 사용하여 K-fold 교차검증 수행
    # 모델 평가 지표 : MSE, R-squared

    val_scores_mse = []
    val_scores_r2 = []
    kf = KFold(n_splits=5, shuffle=True, random_state=42)

    for i, (trn_idx, val_idx) in enumerate(kf.split(x_train)):
        x_trn, y_trn = x_train.iloc[trn_idx], y_train.iloc[trn_idx]
        x_val, y_val = x_train.iloc[val_idx], y_train.iloc[val_idx]

        # 전처리 (필요한 경우)
        # x_trn, x_val, x_test = preprocess(x_trn, x_val, x_test)

        # 모델 정의
        model = XGBRegressor(
            n_estimators=100,
            learning_rate=0.1,
            max_depth=5,
            random_state=42,
            subsample=0.8,
            colsample_bytree=0.8,
        )

        # 모델 학습
        model.fit(
            x_trn,
            y_trn,
            eval_set=[(x_val, y_val)],
            eval_metric="rmse",
            early_stopping_rounds=10,
            verbose=False,
        )

        # 예측
        y_trn_pred = model.predict(x_trn)
        y_val_pred = model.predict(x_val)

        # 훈련, 검증 데이터 평가
        trn_mse = mean_squared_error(y_trn, y_trn_pred)
        val_mse = mean_squared_error(y_val, y_val_pred)
        trn_r2 = r2_score(y_trn, y_trn_pred)
        val_r2 = r2_score(y_val, y_val_pred)

        print(f"{i} Fold, train MSE: {trn_mse:.4f}, validation MSE: {val_mse:.4f}")
        print(
            f"{i} Fold, train R-squared: {trn_r2:.4f}, validation R-squared: {val_r2:.4f}"
        )

        val_scores_mse.append(val_mse)
        val_scores_r2.append(val_r2)

    # 교차 검증 성능 평균 계산하기
    print("Cross Validation Score of MSE: {:.4f}".format(np.mean(val_scores_mse)))
    print("Cross Validation Score of R-squared: {:.4f}".format(np.mean(val_scores_r2)))

    return model


# 메인으로 실행 될 함수
def predict() -> None:
    # 데이터 분리 함수 호출
    x_train, x_valid, x_test, y_train, y_valid, y_test = split_data(df)

    # 데이터 전처리
    x_train, x_valid, x_test = preprocess(x_train, x_valid, x_test)
    print(x_train.columns)

    # 모델 학습
    model = XGboostRegressor(x_train, y_train)

    # 테스트 데이터에 대한 예측
    y_test_pred = model.predict(x_test)

    # 성능 평가
    test_mse = mean_squared_error(y_test, y_test_pred)
    test_r2 = r2_score(y_test, y_test_pred)

    print(f"Test Mean Squared Error: {test_mse:.4f}")
    print(f"Test R-squared: {test_r2:.4f}")
