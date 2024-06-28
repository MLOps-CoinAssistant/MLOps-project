from fastapi import APIRouter, Depends, Query
from dependency_injector.wiring import inject, Provide
from app.services.xai_service import XaiService
from app.core.container import Container

from app.models.schemas.common import BaseResponse, HttpResponse, ErrorResponse
from app.models.schemas.xai import BTCFeatureImportancesResp
from typing import List

router = APIRouter()


# XAI 결과 호출 엔드포인트
@router.get(
    "/importances",
    response_model=BaseResponse[List[BTCFeatureImportancesResp]],
    responses={400: {"model": ErrorResponse}},
)
@inject
async def get_importances(
    skip: int = Query(0, ge=0, description="Start point"),
    limit: int = Query(10, ge=1, description="End point"),
    xai_service: XaiService = Depends(Provide[Container.xai_service]),
) -> HttpResponse:
    """
    학습된 모든 모델들에 대하여 XAI를 통해 각 feautre들이 모델 예측에 미치는 영향을 계산하여 중요도를 시각적으로 표시하는 기능.

    값이 클수록 해당 feature의 중요도가 크다는걸 의미합니다.
    """
    response_data = await xai_service.get_importances(skip, limit)
    return HttpResponse(content=response_data)
