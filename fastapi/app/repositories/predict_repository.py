from catboost import CatBoostClassifier
from app.core.minio import (
    get_model_uri,
    load_model_and_metadata,
    get_latest_model_path,
    get_production_model_uri,
)
from app.core.db.session import AsyncScopedSession
from app.core.redis import RedisCacheDecorator
from app.core.errors import error

from app.models.schemas.predict import BtcPredictionResp, BtcPredictionNoConfidenceResp
from app.models.schemas.data import BtcOhlcvResp
from app.models.db.model import BtcPreprocessed

from app.repositories.data_repository import DataRepository
from sqlalchemy import select
from typing import List

import mlflow.pyfunc
import pandas as pd
import logging

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class PredictRepository:
    def __init__(self, data_repository: DataRepository):
        self.data_repository = data_repository

    @RedisCacheDecorator(ttl=300)
    async def predict(self) -> List[BtcPredictionResp]:
        """
        가장 최근의 학습에서 저장된 모델로 예측한 결과
        """
        try:
            bucket_name = "mlflow"
            model_path = get_latest_model_path(bucket_name)  # 최신 모델 경로 가져오기
            logger.info(f"Model path from MinIO: {model_path}")

            model_uri = get_model_uri(bucket_name, model_path)
            logger.info(f"Constructed model URI: {model_uri}")

            average_proba = load_model_and_metadata(model_uri)

            # 예측 결과와 확률 계산
            # 예측 결과 : 모델이 예측한 결과의 평균을 0.5를 기준으로 다음 정각의 가격을 "상승" or "하락" 으로 판단
            # 확률 : 이 예측이 정확한 정도를 "확률 (%)" 로 표기

            if average_proba is None:
                raise error.PredictModelNotFoundException()

            if average_proba >= 0.5:
                prediction: str = "상승"
                confidence: float = round(
                    average_proba * 100, 1
                )  # 소수점 한 자리로 반올림
            else:
                prediction: str = "하락"
                confidence: float = round(
                    (100 - average_proba * 100), 1
                )  # 소수점 한 자리로 반올림

            return [BtcPredictionResp(prediction=prediction, confidence=confidence)]

        except error.MinioObjectNotFoundException as e:
            logger.error(e)
            raise

        except error.PredictModelNotFoundException as e:
            logger.error(e)
            raise

        except Exception as e:
            logger.error(e)
            raise error.PredictModelNotFoundException() from e

    @RedisCacheDecorator()
    async def predict_product(self) -> List[BtcPredictionNoConfidenceResp]:
        """
        현재 프로덕션 서버의 모델로 예측한 결과
        """
        try:

            # 예측 결과와 확률 계산
            # 예측 결과 : 모델이 예측한 결과의 평균을 0.5를 기준으로 다음 정각의 가격을 "상승" or "하락" 으로 판단
            # 확률 : 이 예측이 정확한 정도를 "확률 (%)" 로 표기

            market = "KRW-BTC"
            model_uri = get_production_model_uri()  # 프로덕션 모델 경로 가져오기
            logger.debug(f"Constructed model URI: {model_uri}")

            model = mlflow.pyfunc.load_model(model_uri)

            if model is None:
                raise error.PredictModelNotFoundException()

            # 최신 BTC ohlcv 데이터를 가져옴
            latest_btc_ohlcv: BtcOhlcvResp = (
                await self.data_repository.get_latest_ohlcv_data(market)
            )
            logger.info(f"Latest OHLCV data: {latest_btc_ohlcv}")
            logger.info(f"type of Latest OHLCV data: {type(latest_btc_ohlcv)}")

            async with AsyncScopedSession() as session:
                stmt = (
                    select(BtcPreprocessed)
                    .order_by(BtcPreprocessed.time.desc())
                    .limit(1)
                )
                result = await session.execute(stmt)
                latest_btc_preprocessed: BtcPreprocessed = result.scalar()

                if not latest_btc_preprocessed:
                    raise error.BtcPreprocessedNotFoundException()

            # 시간 차이를 확인
            time_difference = abs(
                (latest_btc_ohlcv.time - latest_btc_preprocessed.time).total_seconds()
            )
            if time_difference > 300:  # 5분
                raise error.TimeDifferenceTooLargeException()

            logger.info(f"Latest preprocessed data: {latest_btc_preprocessed}")
            logger.info(
                f"type of Latest preprocessed data: {type(latest_btc_preprocessed)}"
            )

            preprocessed_data = BtcPreprocessed(
                time=latest_btc_ohlcv.time,
                open=latest_btc_ohlcv.open,
                high=latest_btc_ohlcv.high,
                low=latest_btc_ohlcv.low,
                close=latest_btc_ohlcv.close,
                volume=latest_btc_ohlcv.volume,
                label=latest_btc_preprocessed.label,
                ma_7=latest_btc_preprocessed.ma_7,
                ma_14=latest_btc_preprocessed.ma_14,
                ma_30=latest_btc_preprocessed.ma_30,
                rsi_14=latest_btc_preprocessed.rsi_14,
                rsi_over=latest_btc_preprocessed.rsi_over,
            )

            logger.info(f"preprocessed_data : {preprocessed_data}")

            df = pd.DataFrame(
                [
                    {
                        "open": preprocessed_data.open,
                        "high": preprocessed_data.high,
                        "low": preprocessed_data.low,
                        "close": preprocessed_data.close,
                        "volume": preprocessed_data.volume,
                        "ma_7": preprocessed_data.ma_7,
                        "ma_14": preprocessed_data.ma_14,
                        "ma_30": preprocessed_data.ma_30,
                        "rsi_14": preprocessed_data.rsi_14,
                        "rsi_over": preprocessed_data.rsi_over,
                    }
                ]
            )

            pred = model.predict(df)
            logger.debug(f"Prediction: {pred}")

            if pred[0] == 1:
                prediction: str = "상승"
            else:
                prediction: str = "하락"

            return [BtcPredictionNoConfidenceResp(prediction=prediction)]

        except (
            error.MinioObjectNotFoundException,
            error.PredictModelNotFoundException,
        ) as e:
            logger.error(e)
            raise

        except Exception as e:
            logger.error(e)
            raise error.PredictModelNotFoundException() from e
