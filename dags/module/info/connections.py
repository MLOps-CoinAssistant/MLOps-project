from enum import Enum


class AirflowConnections(Enum):
    DEVELOPMENT = "development-postgres-default"
    DEVELOPMENT_HYPERPARAMETER_STORE = "development-hyperparameter-store"

    PRODUCTION = "production-postgres-default"
    PRODUCTION_HYPERPARAMETER_STORE = "production-hyperparameter-store"
