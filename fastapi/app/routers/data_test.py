from fastapi import APIRouter
from app.core.db.session import AsyncScopedSession
from app.models.db.model import BtcOhlcv, BtcPreprocessed
from sqlalchemy.future import select
from app.models.schemas.common import BaseResponse, HttpResponse
from app.models.schemas.data_test import BtcOhlcvResp, BtcPreprocessedResp
from typing import List

router = APIRouter()


@router.get("/btc_ohlcv/", response_model=BaseResponse[List[BtcOhlcvResp]])
async def read_btc_ohlcv(skip: int = 0, limit: int = 10) -> HttpResponse:
    async with AsyncScopedSession() as session:
        stmt = select(BtcOhlcv).offset(skip).limit(limit)
        result = (await session.execute(stmt)).scalars().all()

        response_data = [
            BtcOhlcvResp(
                time=record.time,
                open=record.open,
                high=record.high,
                low=record.low,
                close=record.close,
                volume=record.volume,
            )
            for record in result
        ]

        return HttpResponse(content=response_data)


@router.get(
    "/btc_preprocessed/", response_model=BaseResponse[List[BtcPreprocessedResp]]
)
async def read_btc_ohlcv(skip: int = 0, limit: int = 10) -> HttpResponse:
    async with AsyncScopedSession() as session:
        stmt = select(BtcPreprocessed).offset(skip).limit(limit)
        result = (await session.execute(stmt)).scalars().all()

        response_data = [
            BtcOhlcvResp(
                time=record.time,
                open=record.open,
                high=record.high,
                low=record.low,
                close=record.close,
                volume=record.volume,
            )
            for record in result
        ]

        return HttpResponse(content=response_data)
