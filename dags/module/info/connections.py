from enum import Enum


class AirflowConnections(Enum):
    # POSTGRES_DEFAULT = "development-postgres-default"
    POSTGRES_DEFAULT = "production-postgres-default"
    # HYPERPARAMETER_STORE = "development-hyperparameter-store"
    HYPERPARAMETER_STORE = "production-hyperparameter-store"
