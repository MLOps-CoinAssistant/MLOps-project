from fastapi import APIRouter
from app.core.minio import get_model_uri, load_model_and_metadata, get_latest_model_path
from app.models.schemas.common import BaseResponse, HttpResponse
from app.models.schemas.predict import BtcPredictionResp
from typing import List
import logging

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/predict", response_model=BaseResponse[List[BtcPredictionResp]])
async def predict() -> HttpResponse:

    bucket_name = "mlflow"
    model_path = get_latest_model_path(bucket_name)  # 최신 모델 경로 가져오기
    logger.info(f"Model path from MinIO: {model_path}")

    model_uri = get_model_uri(bucket_name, model_path)
    logger.info(f"Constructed model URI: {model_uri}")

    average_proba = load_model_and_metadata(model_uri)

    # 예측 결과와 확률 계산
    # 예측 결과 : 모델이 예측한 결과의 평균을 0.5를 기준으로 다음 정각의 가격을 "상승" or "하락" 으로 판단
    # 확률 : 이 예측이 정확한 정도를 "확률 (%)" 로 표기

    prediction: str = "상승" if average_proba >= 0.5 else "하락"
    confidence: float = round(average_proba * 100, 1)  # 소수점 한 자리로 반올림

    response_data: List[BtcPredictionResp] = [
        BtcPredictionResp(prediction=prediction, confidence=confidence)
    ]

    # 데이터를 dictionary로 변환하여 alias 적용
    response_content = [data.dict(by_alias=True) for data in response_data]

    return HttpResponse(content=response_content)
