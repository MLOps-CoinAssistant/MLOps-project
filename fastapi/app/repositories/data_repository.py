from app.core.db.session import AsyncScopedSession
from app.core.redis import RedisCacheDecorator
from sqlalchemy import select, func
from app.core.config import config
from app.models.db.model import BtcOhlcv, BtcPreprocessed
from app.models.schemas.data_test import BtcOhlcvResp, BtcPreprocessedResp
from app.core.errors import error
from app.core.logger import logger
from typing import List, Tuple, Dict


from aiohttp import ClientSession
from datetime import datetime
import asyncio
import aiohttp
import jwt
import uuid


class DataRepository:
    async def get_btc_ohlcv(self, skip: int, limit: int) -> List[BtcOhlcvResp]:
        logger.debug("Creating a new database session")
        async with AsyncScopedSession() as session:
            try:
                logger.debug("Executing count query")
                total_count: int = await session.scalar(
                    select(func.count(BtcOhlcv.time))
                )
                logger.debug(f"Total count of BtcOhlcv: {total_count}")

                if skip >= total_count:
                    raise error.OutOfRangeException()

                if skip + limit > total_count:
                    limit = total_count - skip

                logger.debug(
                    f"Executing select query with skip={skip} and limit={limit}"
                )
                stmt = select(BtcOhlcv).offset(skip).limit(limit)
                result: List[BtcOhlcv] = (await session.execute(stmt)).scalars().all()
                logger.debug(f"Query result: {result}")

                return [
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

            except error.OutOfRangeException as e:
                logger.error(e)
                raise

            except Exception as e:
                logger.error(e)
                raise error.BtcOhlcvNotFoundException()

    async def get_btc_preprocessed(
        self, skip: int, limit: int
    ) -> List[BtcPreprocessedResp]:
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
                result: List[BtcPreprocessed] = (
                    (await session.execute(stmt)).scalars().all()
                )

                if not result:
                    raise error.BtcPreprocessedNotFoundException()

                return [
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

            except error.OutOfRangeException as e:
                logger.error(e)
                raise

            except Exception as e:
                logger.error(e)
                raise error.BtcPreprocessedNotFoundException()

    # JWT 생성 함수
    # @RedisCacheDecorator()
    def generate_jwt_token(self, access_key: str, secret_key: str) -> str:
        payload = {"access_key": access_key, "nonce": str(uuid.uuid4())}
        jwt_token = jwt.encode(payload, secret_key, algorithm="HS256")
        authorization_token = f"Bearer {jwt_token}"
        return authorization_token

    @RedisCacheDecorator()
    async def fetch_ohlcv_data(
        self,
        session: ClientSession,
        market: str,
        to: str,
        count: int,
        minutes: int,
        retry=5,
    ) -> Tuple[list, Dict[str, str]]:
        """
        Upbit API를 호출하여 OHLCV 데이터를 가져오는 함수
        """
        url = f"https://api.upbit.com/v1/candles/minutes/{minutes}?market={market}&to={to}&count={count}"
        headers = {
            "Accept": "application/json",
            "Authorization": self.generate_jwt_token(
                config.UPBIT_ACCESS_KEY, config.UPBIT_SECRET_KEY
            ),
        }
        backoff = 1
        for _ in range(retry):
            try:
                async with session.get(url, headers=headers) as response:
                    if response.status == 401:
                        raise error.InvalidAPIKey()
                    response.raise_for_status()
                    data = await response.json()
                    if isinstance(data, list):
                        return data, response.headers
                    else:
                        logger.error(f"Unexpected response format: {data}")
            except aiohttp.ClientError as e:
                if response.status == 429 or response.status >= 500:
                    logger.warning(
                        f"API request failed: {e}, retrying in {backoff} seconds..."
                    )
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 30)  # Exponential backoff
                else:
                    logger.error(f"API request failed: {e}")
                    raise error.UpbitServiceUnavailableException()
        return [], {}

    @RedisCacheDecorator()
    async def get_latest_ohlcv_data(self, market: str) -> BtcOhlcvResp:
        """
        현재 시간 기준으로 BTC ohlcv 데이터를 1개 가져오는 함수
        """
        async with ClientSession() as session:
            to = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
            try:
                data, _ = await self.fetch_ohlcv_data(session, market, to, 1, 1)
                if not data:
                    raise error.UpbitServiceUnavailableException()
                ohlcv_data = data[0]  # 최신 데이터 1개만 사용
                return BtcOhlcvResp(
                    time=ohlcv_data["candle_date_time_kst"],
                    open=ohlcv_data["opening_price"],
                    high=ohlcv_data["high_price"],
                    low=ohlcv_data["low_price"],
                    close=ohlcv_data["trade_price"],
                    volume=ohlcv_data["candle_acc_trade_volume"],
                )

            except error.InvalidAPIKey as e:
                logger.error(e)
                raise

            except error.UpbitServiceUnavailableException as e:
                logger.error(e)
                raise

            except Exception as e:
                logger.error(e)
                raise
