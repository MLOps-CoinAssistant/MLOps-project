import pytest
from datetime import datetime
from dependency_injector.providers import Object

from httpx import AsyncClient
from unittest.mock import AsyncMock
from app.core.container import Container
from app.core.errors import error
from app.models.db.model import BtcOhlcv, BtcPreprocessed
from app.models.schemas.data_test import BtcOhlcvResp, BtcPreprocessedResp

from app.services import DataService


@pytest.mark.parametrize(
    "skip,limit",
    [
        (0, 10),
    ],
)
async def test_get_btc_ohlcv_200(
    container: Container,
    async_client: AsyncClient,
    data_service_mock: AsyncMock,
    skip: int,
    limit: int,
):

    btc_ohlcv_resp = [
        BtcOhlcvResp(
            time="2023-01-01T00:00:00",
            open=100,
            high=110,
            low=90,
            close=105,
            volume=1000.0,
        )
        for _ in range(10)
    ]

    data_service_mock.data_repository.get_btc_ohlcv.return_value = btc_ohlcv_resp
    container.data_service.override(data_service_mock)

    # container.data_service.override(Object(data_service_mock))

    url = f"/v1/data/btc_ohlcv/?skip={skip}&limit={limit}"
    response = await async_client.get(url)
    print(f"Response status code: {response.status_code}")
    print(f"Response content: {response.content.decode()}")
    json_response = response.json()
    print(f"json_response = {json_response}")
    assert response.status_code == 200
    assert len(json_response["data"]) == 10
    for item in json_response["data"]:
        assert item["time"] == "2023-01-01T00:00:00"
        assert item["open"] == 100
        assert item["high"] == 110
        assert item["low"] == 90
        assert item["close"] == 105
        assert item["volume"] == 1000.0


@pytest.mark.parametrize(
    "skip,limit",
    [
        (0, 10),
    ],
)
async def test_get_btc_preprocessed_200(
    container: Container,
    async_client: AsyncClient,
    data_service_mock: DataService,
    skip: int,
    limit: int,
):
    # Setup
    # Mock data
    btc_preprocessed_resp = [
        BtcPreprocessedResp(
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
        for _ in range(10)
    ]

    data_service_mock.data_repository.get_btc_preprocessed.return_value = (
        btc_preprocessed_resp
    )
    container.data_service.override(data_service_mock)

    # Run
    url = f"/v1/data/btc_preprocessed/?skip={skip}&limit={limit}"
    response = await async_client.get(url)
    # json_response = response.json()

    # Debugging outputs
    print(f"Response status code: {response.status_code}")
    print(f"Response content: {response.content.decode()}")

    # Assert
    assert response.status_code == 200
    json_response = response.json()
    print(f"json_response = {json_response}")
    assert len(json_response["data"]) == 10
    for item in json_response["data"]:
        assert item["time"] == "2023-01-01T00:00:00"
        assert item["open"] == 100
        assert item["high"] == 110
        assert item["low"] == 90
        assert item["close"] == 105
        assert item["volume"] == 1000.0
        assert item["ma_7"] == 101.0
        assert item["ma_14"] == 102.0
        assert item["ma_30"] == 103.0
        assert item["rsi_14"] == 50.0
        assert item["rsi_over"] == 1.3
