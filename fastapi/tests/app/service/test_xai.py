import pytest
from unittest.mock import AsyncMock

from app.models.schemas.xai import BTCFeatureImportancesResp
from datetime import datetime


# get_importances 조회 테스트
@pytest.mark.asyncio
async def test_get_importances(
    xai_repository_mock: AsyncMock,
    xai_service_mock: AsyncMock,
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
    xai_repository_mock.get_importances.return_value = importances_resp

    skip = 0
    limit = 10

    result = await xai_service_mock.get_importances(skip=skip, limit=limit)

    assert result is not None
    assert len(result) == 10
    for item in result:
        assert item.run_id == "test_run_id"
        assert item.experiment_name == "test_experiment"
        assert item.time == datetime.strptime(
            "2023-01-01T00:00:00", "%Y-%m-%dT%H:%M:%S"
        )
        assert item.open == 100
        assert item.high == 110
        assert item.low == 90
        assert item.close == 105
        assert item.volume == 1000.0
        assert item.ma_7 == 101
        assert item.ma_14 == 102
        assert item.ma_30 == 103
        assert item.rsi_14 == 50.0
        assert item.rsi_over == 1.3
