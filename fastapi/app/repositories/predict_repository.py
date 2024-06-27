from app.core.minio import (
    get_model_uri,
    load_model_and_metadata,
    get_latest_model_path,
    get_production_model_uri,
)
from app.core.redis import RedisCacheDecorator
from app.core.errors import error
from app.models.schemas.common import BaseResponse, HttpResponse
from app.models.schemas.predict import BtcPredictionResp
from app import repositories
from typing import List
import logging

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class PredictRepository:
    @RedisCacheDecorator()
    async def predict(self) -> HttpResponse:
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

        except error.MinioServiceUnavailableException as e:
            logger.error(e)
            raise error.MinioServiceUnavailableException() from e

        except error.MinioObjectNotFoundException as e:
            logger.error(e)
            raise error.MinioObjectNotFoundException() from e

        except error.PredictModelNotFoundException as e:
            logger.error(e)
            raise error.PredictModelNotFoundException() from e

        except Exception as e:
            logger.error(e)
            raise error.PredictModelNotFoundException() from e

    @RedisCacheDecorator()
    async def predict_product(self) -> HttpResponse:
        try:

            # 예측 결과와 확률 계산
            # 예측 결과 : 모델이 예측한 결과의 평균을 0.5를 기준으로 다음 정각의 가격을 "상승" or "하락" 으로 판단
            # 확률 : 이 예측이 정확한 정도를 "확률 (%)" 로 표기

            market = "KRW-BTC"
            model_uri = get_production_model_uri()  # 프로덕션 모델 경로 가져오기
            logger.info(f"Constructed model URI: {model_uri}")

            model, _ = load_model_and_metadata(model_uri)

            if model is None:
                raise error.PredictModelNotFoundException()

            # 최신 BTC ohlcv 데이터를 가져옴
            latest_ohlcv = await self.data_repository.get_latest_ohlcv_data(
                market=market
            )
            logger.info(f"Latest OHLCV data: {latest_ohlcv}")

            # 모델을 사용하여 예측 수행
            features = [latest_ohlcv.dict()]
            average_proba = model.predict_proba(features)[:, 1].mean()

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

        except error.MinioServiceUnavailableException as e:
            logger.error(e)
            raise error.MinioServiceUnavailableException() from e

        except error.MinioObjectNotFoundException as e:
            logger.error(e)
            raise error.MinioObjectNotFoundException() from e

        except error.MLflowServiceUnavailableException as e:
            logger.error(e)
            raise error.MLflowServiceUnavailableException() from e

        except error.PredictModelNotFoundException as e:
            logger.error(e)
            raise error.PredictModelNotFoundException() from e

        except Exception as e:
            logger.error(e)
            raise error.PredictModelNotFoundException() from e
