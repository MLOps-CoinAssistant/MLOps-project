from fastapi import APIRouter, Depends, Query
from dependency_injector.wiring import inject, Provide
from app.services.data_service import DataService
from app.core.container import Container
from app.core.logger import logger
from app.core.redis import RedisCacheDecorator

from app.models.schemas.common import BaseResponse, HttpResponse, ErrorResponse
from app.models.schemas.data_test import BtcOhlcvResp, BtcPreprocessedResp
from typing import List

router = APIRouter()


@router.get(
    "/btc_ohlcv/",
    response_model=BaseResponse[List[BtcOhlcvResp]],
    responses={400: {"model": ErrorResponse}},
)
@inject
async def read_btc_ohlcv(
    skip: int = Query(0, ge=0, description="Start point"),
    limit: int = Query(10, ge=1, description="End point"),
    data_service: DataService = Depends(Provide[Container.data_service]),
) -> HttpResponse:
    logger.info(f"Received request with skip={skip} and limit={limit}")
    response_data = await data_service.get_btc_ohlcv(skip, limit)
    return HttpResponse(content=response_data)


@router.get(
    "/btc_preprocessed/",
    response_model=BaseResponse[List[BtcPreprocessedResp]],
    responses={400: {"model": ErrorResponse}},
)
@inject
async def read_btc_preprocessed(
    skip: int = Query(0, ge=0, description="Start point"),
    limit: int = Query(10, ge=1, description="End point"),
    data_service: DataService = Depends(Provide[Container.data_service]),
) -> HttpResponse:
    """
    btc_preprocessed 데이터 개수: 105120
    """

    response_data = await data_service.get_btc_preprocessed(skip, limit)
    return HttpResponse(content=response_data)


@router.get(
    "/btc_ohlcv/latest/",
    response_model=BaseResponse[BtcOhlcvResp],
    responses={400: {"model": ErrorResponse}},
)
@RedisCacheDecorator()
@inject
async def get_latest_ohlcv_data(
    market: str = "KRW-BTC",
    data_service: DataService = Depends(Provide[Container.data_service]),
) -> HttpResponse:

    response_data = await data_service.get_latest_ohlcv_data(market)
    return HttpResponse(content=response_data)
