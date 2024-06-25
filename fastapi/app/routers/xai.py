from fastapi import APIRouter, HTTPException
from app.core.logger import logger
from app.core.db.session import AsyncScopedSession
from app.core.redis import RedisCacheDecorator
from app.core.errors import error
from app.models.db.model import BTCFeatureImportances
from sqlalchemy import select, func
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
@RedisCacheDecorator()
async def get_importances(skip: int = 0, limit: int = 10) -> HttpResponse:
    async with AsyncScopedSession() as session:
        try:
            total_count: int = await session.scalar(
                select(func.count(BTCFeatureImportances.run_id))
            )
            if skip < 0 or limit <= 0 or skip >= total_count:
                raise error.OutOfRangeException()

            if skip + limit > total_count:
                limit = max(total_count - skip, 0)

            stmt = select(BTCFeatureImportances).offset(skip).limit(limit)
            result: List[(BTCFeatureImportances)] = (
                (await session.execute(stmt)).scalars().all()
            )

            if not result:
                raise error.BtcFeatureImportancesNotFoundException

            response_data: List[BTCFeatureImportancesResp] = [
                BTCFeatureImportancesResp(
                    run_id=record.run_id,
                    experiment_name=record.experiment_name,
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

        except error.OutOfRangeException as e:
            logger.error(e)
            raise HTTPException(status_code=400, detail="Out of range error")
        except error.BtcFeatureImportancesNotFoundException as e:
            logger.error(e)
            raise HTTPException(status_code=404, detail="Importances not found")
        except Exception as e:
            logger.error(e)
            raise HTTPException(status_code=500, detail="Internal server error")
