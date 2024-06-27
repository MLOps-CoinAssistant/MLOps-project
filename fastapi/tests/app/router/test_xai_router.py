import pytest
from httpx import AsyncClient
from unittest.mock import AsyncMock
from app.core.container import Container
from app.models.schemas.xai import BTCFeatureImportancesResp
from app.core.errors import error


@pytest.mark.parametrize(
    "skip,limit",
    [
        (0, 10),
    ],
)
async def test_get_importances_200(
    container: Container,
    async_client: AsyncClient,
    xai_service_mock: AsyncMock,
    skip: int,
    limit: int,
):
    importances_resp = [
        BTCFeatureImportancesResp(
            run_id="test_run_id",
            experiment_name="test_experiment",
            time="2023-01-01T00:00:00",
            open=100,
            high=110,
            low=90,
            close=105,
            volume=1000.0,
            ma_7=101,
            ma_14=102,
            ma_30=103,
            rsi_14=50.0,
            rsi_over=1.3,
        )
        for _ in range(10)
    ]

    xai_service_mock.xai_repository.get_importances.return_value = importances_resp
    container.xai_service.override(xai_service_mock)

    url = f"/v1/xai/importances?skip={skip}&limit={limit}"
    response = await async_client.get(url)
    json_response = response.json()

    assert response.status_code == 200
    assert len(json_response["data"]) == 10
    for item in json_response["data"]:
        assert item["run_id"] == "test_run_id"
        assert item["experiment_name"] == "test_experiment"
        assert item["time"] == "2023-01-01T00:00:00"
        assert item["open"] == 100
        assert item["high"] == 110
        assert item["low"] == 90
        assert item["close"] == 105
        assert item["volume"] == 1000.0
        assert item["ma_7"] == 101
        assert item["ma_14"] == 102
        assert item["ma_30"] == 103
        assert item["rsi_14"] == 50.0
        assert item["rsi_over"] == 1.3


@pytest.mark.parametrize(
    "skip,limit,expected_error",
    [
        (10000, 10, error.ERROR_400_OUT_OF_RANGE),  # skip >= total_count
    ],
)
async def test_get_importances_400_out_of_range(
    container: Container,
    async_client: AsyncClient,
    xai_service_mock: AsyncMock,
    skip: int,
    limit: int,
    expected_error: str,
):
    xai_service_mock.xai_repository.get_importances.side_effect = (
        error.OutOfRangeException()
    )
    container.xai_service.override(xai_service_mock)

    url = f"/v1/xai/importances?skip={skip}&limit={limit}"
    response = await async_client.get(url)
    json_response = response.json()

    assert response.status_code == 400
    assert json_response["statusCode"] == expected_error
    assert json_response["message"] == "Your request is out of the allowed range."


@pytest.mark.parametrize(
    "skip,limit,expected_error",
    [
        (
            0,
            10,
            error.ERROR_400_BTC_FEATURE_IMPORTACES_NOT_FOUND,
        ),  # Data not found scenario
    ],
)
async def test_get_importances_400_not_found(
    container: Container,
    async_client: AsyncClient,
    xai_service_mock: AsyncMock,
    skip: int,
    limit: int,
    expected_error: str,
):
    xai_service_mock.xai_repository.get_importances.side_effect = (
        error.BtcFeatureImportancesNotFoundException()
    )
    container.xai_service.override(xai_service_mock)

    url = f"/v1/xai/importances?skip={skip}&limit={limit}"
    response = await async_client.get(url)
    json_response = response.json()

    assert response.status_code == 400
    assert json_response["statusCode"] == expected_error
    assert json_response["message"] == "BtcFeatureImportances is not found."
