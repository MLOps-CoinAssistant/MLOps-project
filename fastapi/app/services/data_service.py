from app import repositories
from aiohttp import ClientSession

from app.models.schemas.data import BtcOhlcvResp, BtcPreprocessedResp

from app.core.logger import logger
from typing import List


class DataService:
    def __init__(self, data_repository: repositories.DataRepository):
        self.data_repository = data_repository

    async def get_btc_ohlcv(self, skip: int, limit: int) -> List[BtcOhlcvResp]:
        return await self.data_repository.get_btc_ohlcv(skip=skip, limit=limit)

    async def get_btc_preprocessed(
        self, skip: int, limit: int
    ) -> List[BtcPreprocessedResp]:
        return await self.data_repository.get_btc_preprocessed(skip=skip, limit=limit)

    async def get_latest_ohlcv_data(self, market: str) -> BtcOhlcvResp:
        """
        현재 시간 기준으로 BTC ohlcv 데이터를 1개 가져오는 함수
        """
        return await self.data_repository.get_latest_ohlcv_data(market=market)

    async def fetch_ohlcv_data(
        self, session: ClientSession, market: str, to: str, count: int, minutes: int
    ):
        return await self.data_repository.fetch_ohlcv_data(
            session, market, to, count, minutes
        )
