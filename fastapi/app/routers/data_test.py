from fastapi import APIRouter
from app.core.logger import logger
from app.core.db.session import AsyncScopedSession
from app.core.redis import RedisCacheDecorator
from app.core.errors import error
from app.models.db.model import BtcOhlcv, BtcPreprocessed
from sqlalchemy import select, func
from app.models.schemas.common import BaseResponse, HttpResponse, ErrorResponse
from app.models.schemas.data_test import BtcOhlcvResp, BtcPreprocessedResp
from typing import List

router = APIRouter()


@router.get(
    "/btc_ohlcv/",
    response_model=BaseResponse[List[BtcOhlcvResp]],
    responses={400: {"model": ErrorResponse}},
)
@RedisCacheDecorator()
async def read_btc_ohlcv(skip: int = 0, limit: int = 10) -> HttpResponse:
    async with AsyncScopedSession() as session:
        try:
            # 전체 데이터 개수 조회
            total_count: int = await session.scalar(select(func.count(BtcOhlcv.time)))

            if skip >= total_count:
                raise error.OutOfRangeException()

            if skip + limit > total_count:
                limit = total_count - skip

            stmt = select(BtcOhlcv).offset(skip).limit(limit)
            result: List[BtcOhlcv] = (await session.execute(stmt)).scalars().all()

            response_data: List[BtcOhlcvResp] = [
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

        except error.OutOfRangeException as e:
            logger.error(e)
            raise

        except Exception as e:
            logger.error(e)
            raise error.BtcOhlcvNotFoundException()


@router.get(
    "/btc_preprocessed/",
    response_model=BaseResponse[List[BtcPreprocessedResp]],
    responses={400: {"model": ErrorResponse}},
)
@RedisCacheDecorator()
async def read_btc_preprocessed(skip: int = 0, limit: int = 10) -> HttpResponse:
    """
    btc_preprocessed 데이터 개수: 105120
    """
    async with AsyncScopedSession() as session:
        try:
            total_count: int = await session.scalar(
                select(func.count(BtcPreprocessed.time))
            )

            if skip < 0 or limit <= 0 or skip >= total_count:
                raise error.OutOfRangeException()

            if skip + limit > total_count:
                limit = max(total_count - skip, 0)

            stmt = (
                select(BtcPreprocessed)
                .order_by(BtcPreprocessed.time)
                .offset(skip)
                .limit(limit)
            )
            result: List[(BtcPreprocessed)] = (
                (await session.execute(stmt)).scalars().all()
            )

            if not result:
                raise error.BtcPreprocessedNotFoundException()

            response_data: List[BtcPreprocessedResp] = [
                BtcPreprocessedResp(
                    time=record.time,
                    open=record.open,
                    high=record.high,
                    low=record.low,
                    close=record.close,
                    volume=record.volume,
                    ma_7=record.ma_7,
                    ma_14=record.ma_14,
                    ma_30=record.ma_30,
                    rsi_14=record.rsi_14,
                    rsi_over=record.rsi_over,
                )
                for record in result
            ]

            return HttpResponse(content=response_data)

        except Exception as e:
            logger.error(e)
            raise
