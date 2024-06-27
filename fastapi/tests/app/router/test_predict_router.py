import pytest
from httpx import AsyncClient
from unittest.mock import AsyncMock
from app.core.container import Container
from app.models.schemas.predict import BtcPredictionResp
from app.core.errors import error
from app.services import PredictService


@pytest.mark.asyncio
async def test_predict_200(
    container: Container,
    async_client: AsyncClient,
    predict_service_mock: PredictService,
    predict_repository_mock: AsyncMock,
):
    prediction_response = [BtcPredictionResp(prediction="상승", confidence=75.0)]

    predict_repository_mock.predict.return_value = prediction_response
    predict_service_mock.predict_repository = predict_repository_mock
    container.predict_service.override(predict_service_mock)

    url = "/v1/predict/btc"
    response = await async_client.get(url)
    json_response = response.json()

    assert response.status_code == 200
    assert len(json_response["data"]) == 1
    assert json_response["data"][0]["예측 결과"] == "상승"
    assert json_response["data"][0]["확률 (%)"] == 75.0


@pytest.mark.asyncio
async def test_predict_product_200(
    container: Container,
    async_client: AsyncClient,
    predict_service_mock: PredictService,
    predict_repository_mock: AsyncMock,
):
    prediction_response = [BtcPredictionResp(prediction="상승", confidence=75.0)]

    predict_repository_mock.predict_product.return_value = prediction_response
    predict_service_mock.predict_repository = predict_repository_mock
    container.predict_service.override(predict_service_mock)

    url = "/v1/predict/btc/product"
    response = await async_client.get(url)
    json_response = response.json()
    print(f"json_response : {json_response}")

    assert response.status_code == 200
    assert len(json_response["data"]) == 1
    assert json_response["data"][0]["예측 결과"] == "상승"
    assert json_response["data"][0]["확률 (%)"] == 75.0


@pytest.mark.asyncio
async def test_predict_exceptions(
    container: Container,
    predict_service_mock: PredictService,
    predict_repository_mock: AsyncMock,
):
    # 예외 모킹 설정
    predict_repository_mock.predict.side_effect = error.PredictModelNotFoundException
    predict_service_mock.predict_repository = predict_repository_mock
    container.predict_service.override(predict_service_mock)

    with pytest.raises(error.PredictModelNotFoundException):
        await predict_service_mock.predict()


@pytest.mark.asyncio
async def test_predict_product_exceptions(
    container: Container,
    predict_service_mock: PredictService,
    predict_repository_mock: AsyncMock,
):
    # 예외 모킹 설정
    predict_repository_mock.predict_product.side_effect = (
        error.MLflowServiceUnavailableException
    )
    predict_service_mock.predict_repository = predict_repository_mock
    container.predict_service.override(predict_service_mock)

    with pytest.raises(error.MLflowServiceUnavailableException):
        await predict_service_mock.predict_product()
