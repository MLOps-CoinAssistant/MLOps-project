import pytest
from unittest.mock import AsyncMock

from httpx import AsyncClient

from app.main import create_app
from app.core.container import Container
from app.services import DataService, PredictService, XaiService
from app.repositories import DataRepository, PredictRepository, XaiRepository


@pytest.fixture
def container() -> Container:
    return Container()


@pytest.fixture
def async_client(container) -> AsyncClient:
    app = create_app(container)
    return AsyncClient(app=app, base_url="http://test")


@pytest.fixture
def data_repository_mock():
    return AsyncMock(spec=DataRepository)


@pytest.fixture
def predict_repository_mock():
    return AsyncMock(spec=PredictRepository)


@pytest.fixture
def xai_repository_mock():
    return AsyncMock(spec=XaiRepository)


@pytest.fixture
def data_service_mock(data_repository_mock):
    return DataService(data_repository=data_repository_mock)


@pytest.fixture
def predict_service_mock(predict_repository_mock):
    return PredictService(predict_repository=predict_repository_mock)


@pytest.fixture
def xai_service_mock(xai_repository_mock):
    return XaiService(xai_repository=xai_repository_mock)
