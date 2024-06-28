import pytest
from unittest.mock import AsyncMock

from app.models.schemas.data import BtcOhlcvResp, BtcPreprocessedResp
from app.core.db.session import ping_db, close_db

from datetime import datetime


# db 연결 테스트
@pytest.mark.asyncio
async def test_db_connection():
    try:
        await ping_db()
        assert True
    except Exception as e:
        assert False, f"DB connection failed: {str(e)}"


@pytest.mark.asyncio
async def test_db_close():
    try:
        await close_db()
        assert True
    except Exception as e:
        assert False, f"DB close failed: {str(e)}"


# get_btc_ohlcv 조회 테스트
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


# get_btc_preprocessed 조회 테스트
@pytest.mark.asyncio
async def test_get_btc_preprocessed(
    data_repository_mock: AsyncMock,
    data_service_mock: AsyncMock,
):

    btc_preprocessed_resp = BtcPreprocessedResp(
        time="2023-01-01T00:00:00",
        open=100,
        high=110,
        low=90,
        close=105,
        volume=1000.0,
        ma_7=101.0,
        ma_14=102.0,
        ma_30=103.0,
        rsi_14=50.0,
        rsi_over=1.3,
    )

    data_repository_mock.get_btc_preprocessed.return_value = [btc_preprocessed_resp]

    skip = 0
    limit = 10

    result = await data_service_mock.get_btc_preprocessed(skip=skip, limit=limit)

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
    assert result[0].ma_7 == 101.0
    assert result[0].ma_14 == 102.0
    assert result[0].ma_30 == 103.0
    assert result[0].rsi_14 == 50.0
    assert result[0].rsi_over == 1.3
