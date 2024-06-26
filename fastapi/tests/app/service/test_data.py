import pytest
from unittest.mock import AsyncMock

from app.services.data_service import DataService
from app.models.db.model import BtcOhlcv, BtcPreprocessed
from app.models.schemas.data_test import BtcOhlcvResp

from datetime import datetime


@pytest.mark.asyncio
async def test_get_btc_ohlcv(
    data_repository_mock: AsyncMock,
    data_service_mock: AsyncMock,
):

    btc_ohlcv_resp = BtcOhlcvResp(
        time="2023-01-01T00:00:00",
        open=100,
        high=110,
        low=90,
        close=105,
        volume=1000.0,
    )
    data_repository_mock.get_btc_ohlcv.return_value = [btc_ohlcv_resp]

    # data_service_mock.data_repository = data_repository_mock

    skip = 0
    limit = 10

    result = await data_service_mock.get_btc_ohlcv(skip=skip, limit=limit)

    assert result != None
    assert len(result) == 1
    assert result[0].time == datetime.strptime(
        "2023-01-01T00:00:00", "%Y-%m-%dT%H:%M:%S"
    )
    assert result[0].open == 100
    assert result[0].high == 110
    assert result[0].low == 90
    assert result[0].close == 105
    assert result[0].volume == 1000.0
