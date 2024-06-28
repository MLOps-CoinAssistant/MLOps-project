from dependency_injector import providers, containers
from app.core.db.session import AsyncScopedSession
from app import services
from app import repositories
from app.core.config import Config


class Container(containers.DeclarativeContainer):
    config: Config = providers.Configuration()

    wiring_config = containers.WiringConfiguration(
        packages=[
            "app.routers",
        ]
    )

    data_repository = providers.Factory(repositories.DataRepository)
    predict_repository = providers.Factory(
        repositories.PredictRepository, data_repository=data_repository
    )
    xai_repository = providers.Factory(repositories.XaiRepository)

    db_session = providers.Factory(AsyncScopedSession)
    data_service = providers.Factory(
        services.DataService, data_repository=data_repository
    )
    predict_service = providers.Factory(
        services.PredictService, predict_repository=predict_repository
    )
    xai_service = providers.Factory(services.XaiService, xai_repository=xai_repository)
