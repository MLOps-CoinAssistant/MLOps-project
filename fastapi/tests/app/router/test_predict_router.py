import pytest
from httpx import AsyncClient
from unittest.mock import AsyncMock
from app.core.container import Container
from app.core.errors import error
from app.services import PredictService

from app.models.schemas.predict import BtcPredictionResp, BtcPredictionNoConfidenceResp
from app.models.schemas.data import BtcOhlcvResp
from app.models.db.model import BtcPreprocessed

from datetime import datetime, timedelta


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
    data_repository_mock: AsyncMock,
):
    prediction_response = [
        BtcPredictionNoConfidenceResp(prediction="상승"),
        BtcPredictionNoConfidenceResp(prediction="하락"),
    ]

    now = datetime.now()
    latest_btc_ohlcv = BtcOhlcvResp(
        time=now, open=1000, high=1100, low=900, close=1050, volume=10.0
    )

    latest_btc_preprocessed = BtcPreprocessed(
        time=now - timedelta(minutes=3),  # 3분 차이로 설정
        open=1000,
        high=1100,
        low=900,
        close=1050,
        volume=10.0,
        label=1,
        ma_7=1000,
        ma_14=1000,
        ma_30=1000,
        rsi_14=50.0,
        rsi_over=50.0,
    )

    predict_repository_mock.predict_product.return_value = prediction_response
    predict_service_mock.predict_repository = predict_repository_mock

    data_repository_mock.get_latest_ohlcv_data.return_value = latest_btc_ohlcv
    predict_repository_mock.return_value = latest_btc_preprocessed

    container.predict_service.override(predict_service_mock)

    url = "/v1/predict/btc/product"
    response = await async_client.get(url)
    json_response = response.json()
    print(f"json_response : {json_response}")

    assert response.status_code == 200
    assert len(json_response["data"]) == 2
    assert json_response["data"][0]["예측 결과"] == "상승"
    assert json_response["data"][1]["예측 결과"] == "하락"
    assert (
        abs((latest_btc_ohlcv.time - latest_btc_preprocessed.time).total_seconds())
        <= 300
    )


@pytest.mark.asyncio
async def test_predict_400(
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
async def test_predict_400(
    container: Container,
    async_client: AsyncClient,
    predict_service_mock: PredictService,
    predict_repository_mock: AsyncMock,
):

    # 예외 모킹 설정 및 테스트 실행
    error_cases = [
        (error.PredictModelNotFoundException(), "40002", "PredictModel not found."),
        (error.MinioObjectNotFoundException(), "40006", "MinIO object not found."),
    ]

    for exception_instance, code, message in error_cases:
        predict_repository_mock.predict.side_effect = exception_instance
        predict_service_mock.predict_repository = predict_repository_mock
        container.predict_service.override(predict_service_mock)

        url = "/v1/predict/btc"
        response = await async_client.get(url)
        json_response = response.json()

        assert response.status_code == 400
        assert json_response["statusCode"] == code
        assert json_response["message"] == message


@pytest.mark.asyncio
async def test_predict_product_400(
    container: Container,
    async_client: AsyncClient,
    predict_service_mock: PredictService,
    predict_repository_mock: AsyncMock,
):

    # 예외 모킹 설정 및 테스트 실행
    error_cases = [
        (error.BtcOhlcvNotFoundException(), "40000", "BtcOhlcv not found."),
        (
            error.BtcPreprocessedNotFoundException(),
            "40001",
            "BtcPreprocessed not found.",
        ),
        (error.PredictModelNotFoundException(), "40002", "PredictModel not found."),
        (
            error.BtcFeatureImportancesNotFoundException(),
            "40003",
            "BtcFeatureImportances is not found.",
        ),
        (
            error.OutOfRangeException(),
            "40004",
            "Your request is out of the allowed range.",
        ),
        (
            error.TimeDifferenceTooLargeException(),
            "40005",
            "Time difference is too large.",
        ),
        (error.MinioObjectNotFoundException(), "40006", "MinIO object not found."),
    ]

    for exception_instance, code, message in error_cases:
        predict_repository_mock.predict_product.side_effect = exception_instance
        predict_service_mock.predict_repository = predict_repository_mock
        container.predict_service.override(predict_service_mock)

        url = "/v1/predict/btc/product"
        response = await async_client.get(url)
        json_response = response.json()

        assert response.status_code == 400
        assert json_response["statusCode"] == code
        assert json_response["message"] == message
