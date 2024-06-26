from fastapi import APIRouter, Depends
from dependency_injector.wiring import inject, Provide
from app.core.container import Container
from app.services.predict_service import PredictService
from app.models.schemas.common import BaseResponse, HttpResponse, ErrorResponse
from app.models.schemas.predict import BtcPredictionResp
from typing import List
import logging

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

router = APIRouter()


@router.get(
    "/btc",
    response_model=BaseResponse[List[BtcPredictionResp]],
    responses={400: {"model": ErrorResponse}},
)
@inject
async def predict(
    predict_service: PredictService = Depends(Provide[Container.predict_service]),
) -> HttpResponse:

    response_data = await predict_service.predict()
    response_content = [data.dict(by_alias=True) for data in response_data]
    return HttpResponse(content=response_content)


@router.get(
    "/btc/product",
    response_model=BaseResponse[List[BtcPredictionResp]],
    responses={400: {"model": ErrorResponse}},
)
@inject
async def predict_product(
    predict_service: PredictService = Depends(Provide[Container.predict_service]),
) -> HttpResponse:

    response_data = await predict_service.predict_product()
    response_content = [data.dict(by_alias=True) for data in response_data]
    return HttpResponse(content=response_content)
