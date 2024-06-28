import pytest
from datetime import datetime

from httpx import AsyncClient

from unittest.mock import AsyncMock
from app.core.container import Container
from app.core.errors import error
from app.models.schemas.data import BtcOhlcvResp, BtcPreprocessedResp

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

    url = f"/v1/data/btc_ohlcv/?skip={skip}&limit={limit}"
    response = await async_client.get(url)
    json_response = response.json()

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

    url = f"/v1/data/btc_preprocessed/?skip={skip}&limit={limit}"
    response = await async_client.get(url)

    json_response = response.json()

    assert response.status_code == 200
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


@pytest.mark.asyncio
async def test_get_latest_ohlcv_data_200(
    container: Container,
    data_service_mock: DataService,
    data_repository_mock: AsyncMock,
):
    latest_ohlcv_data = BtcOhlcvResp(
        time=datetime.strptime("2023-01-01T00:00:00", "%Y-%m-%dT%H:%M:%S"),
        open=100,
        high=110,
        low=90,
        close=105,
        volume=1000.0,
    )
    data_repository_mock.get_latest_ohlcv_data.return_value = latest_ohlcv_data
    data_service_mock.data_repository = data_repository_mock
    container.data_service.override(data_service_mock)

    market = "KRW-BTC"
    result = await data_service_mock.get_latest_ohlcv_data(market)

    assert result.time.strftime("%Y-%m-%dT%H:%M:%S") == "2023-01-01T00:00:00"
    assert result.open == 100
    assert result.high == 110
    assert result.low == 90
    assert result.close == 105
    assert result.volume == 1000.0


@pytest.mark.parametrize(
    "skip,limit,expected_error",
    [
        (10000, 10, error.ERROR_400_OUT_OF_RANGE),  # skip >= total_count
    ],
)
async def test_get_btc_ohlcv_400_out_of_range(
    container: Container,
    async_client: AsyncClient,
    data_service_mock: AsyncMock,
    skip: int,
    limit: int,
    expected_error: str,
):

    data_service_mock.data_repository.get_btc_ohlcv.side_effect = (
        error.OutOfRangeException()
    )
    container.data_service.override(data_service_mock)

    url = f"/v1/data/btc_ohlcv/?skip={skip}&limit={limit}"
    response = await async_client.get(url)
    json_response = response.json()

    assert response.status_code == 400
    assert json_response["statusCode"] == expected_error
    assert json_response["message"] == "Your request is out of the allowed range."


@pytest.mark.parametrize(
    "skip,limit,expected_error",
    [
        (0, 10, error.ERROR_400_BTC_RAW_DATA_NOT_FOUND),  # Data not found scenario
    ],
)
async def test_get_btc_ohlcv_400_not_found(
    container: Container,
    async_client: AsyncClient,
    data_service_mock: AsyncMock,
    skip: int,
    limit: int,
    expected_error: str,
):
    # side_effect 는 unittest.mock 라이브러리의 기능.
    # -> 모킹된 함수가 호출될 때 특정 행동(예외발생, 함수호출, 여러 값 반환 등)을 지정할 수 있음. (여기서는 exception)
    data_service_mock.data_repository.get_btc_ohlcv.side_effect = (
        error.BtcOhlcvNotFoundException()
    )
    container.data_service.override(data_service_mock)

    url = f"/v1/data/btc_ohlcv/?skip={skip}&limit={limit}"
    response = await async_client.get(url)
    json_response = response.json()

    # Assert
    assert response.status_code == 400
    assert json_response["statusCode"] == expected_error
    assert json_response["message"] == "BtcOhlcv not found."


@pytest.mark.parametrize(
    "skip,limit,expected_error",
    [
        (10000, 10, error.ERROR_400_OUT_OF_RANGE),  # skip >= total_count
    ],
)
async def test_get_btc_preprocessed_400_out_of_range(
    container: Container,
    async_client: AsyncClient,
    data_service_mock: AsyncMock,
    skip: int,
    limit: int,
    expected_error: str,
):

    data_service_mock.data_repository.get_btc_preprocessed.side_effect = (
        error.OutOfRangeException()
    )
    container.data_service.override(data_service_mock)

    url = f"/v1/data/btc_preprocessed/?skip={skip}&limit={limit}"
    response = await async_client.get(url)
    json_response = response.json()

    assert response.status_code == 400
    assert json_response["statusCode"] == expected_error
    assert json_response["message"] == "Your request is out of the allowed range."


@pytest.mark.parametrize(
    "skip,limit,expected_error",
    [
        (0, 10, error.ERROR_400_BTC_PREPROCESSED_DATA_NOT_FOUND),
    ],
)
async def test_get_btc_preprocessed_400_not_found(
    container: Container,
    async_client: AsyncClient,
    data_service_mock: AsyncMock,
    skip: int,
    limit: int,
    expected_error: str,
):

    data_service_mock.data_repository.get_btc_preprocessed.side_effect = (
        error.BtcPreprocessedNotFoundException()
    )
    container.data_service.override(data_service_mock)

    url = f"/v1/data/btc_preprocessed/?skip={skip}&limit={limit}"
    response = await async_client.get(url)
    json_response = response.json()

    # Assert
    assert response.status_code == 400
    assert json_response["statusCode"] == expected_error
    assert json_response["message"] == "BtcPreprocessed not found."


@pytest.mark.asyncio
async def test_get_latest_ohlcv_data_400(
    container: Container,
    data_service_mock: DataService,
    data_repository_mock: AsyncMock,
):
    # 예외 발생하도록 설정
    data_repository_mock.get_latest_ohlcv_data.side_effect = (
        error.BtcOhlcvNotFoundException()
    )
    data_service_mock.data_repository = data_repository_mock
    container.data_service.override(data_service_mock)

    market = "KRW-BTC"
    # pytest.raises()안의 에러가 발생해야만 테스트가 통과됨
    with pytest.raises(error.BtcOhlcvNotFoundException):
        await data_service_mock.get_latest_ohlcv_data(market)


@pytest.mark.asyncio
async def test_get_latest_ohlcv_data_401(
    container: Container,
    data_service_mock: DataService,
    data_repository_mock: AsyncMock,
):
    data_repository_mock.get_latest_ohlcv_data.side_effect = error.InvalidAPIKey()
    data_service_mock.data_repository = data_repository_mock
    container.data_service.override(data_service_mock)

    market = "KRW-BTC"
    with pytest.raises(error.InvalidAPIKey):
        await data_service_mock.get_latest_ohlcv_data(market)
