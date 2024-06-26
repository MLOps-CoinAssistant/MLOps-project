import os
import logging
from typing import Dict, Any
from dotenv import load_dotenv
from pydantic_settings import BaseSettings, SettingsConfigDict

# 로깅 설정
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# 환경 변수 로드 (FastAPI용 .env 파일 로드)
env_path = os.path.join(os.path.dirname(__file__), "..", "..", "fastapi", ".env")
load_dotenv(env_path)


class Settings(BaseSettings):
    AWS_ACCESS_KEY_ID: str
    AWS_SECRET_ACCESS_KEY: str
    MLFLOW_TRACKING_URI: str
    MLFLOW_TRACKING_URI_LOCAL: str
    MLFLOW_TRACKING_PORT: int
    MLFLOW_S3_ENDPOINT_URL: str
    MLFLOW_S3_ENDPOINT_MAIN_PORT: int
    MLFLOW_S3_ENDPOINT_SUB_PORT: int
    MINIO_ROOT_USER: str
    MINIO_ROOT_PASSWORD: str
    MLFLOW_DB_HOST: str
    DB_TYPE: str
    DB_USER: str
    DB_PW: str
    DB_HOST: str
    DB_PORT: int
    DB_DEFAULT_NAME: str
    ARTIFACT_ROOT: str
    MLFLOW_SERVER_HOST: str
    ARTIFACT_MODEL_REGISTRY_PATH: str
    AIRFLOW_SMTP_USER: str
    SMTP_MAIL_ADDRESS: str
    AIRFLOW_SMTP_PASSWORD: str
    UVICORN_PORT: int
    UPBIT_ACCESS_KEY: str
    UPBIT_SECRET_KEY: str
    ENV: str

    model_config = SettingsConfigDict(
        env_file=os.path.join(os.path.dirname(__file__), "..", "..", "fastapi", ".env"),
        extra="allow",
    )


settings = Settings()

DB_URL: str = (
    f"{settings.DB_TYPE}+asyncpg://{settings.DB_USER}:{settings.DB_PW}@{settings.DB_HOST}:{settings.DB_PORT}/{settings.DB_DEFAULT_NAME}"
)


class Config(BaseSettings):
    DB_URL: str = DB_URL
    ENV: str = "local"
    TITLE: str = "Prediction Bitcoin Price"
    VERSION: str = "0.1.0"
    APP_HOST: str = "http://localhost:8000"
    OPENAPI_URL: str = "/openapi.json"
    DOCS_URL: str = "/docs"
    REDOC_URL: str = "/redoc"

    LOG_LEVEL: int = logging.DEBUG

    UPBIT_ACCESS_KEY: str = settings.UPBIT_ACCESS_KEY
    UPBIT_SECRET_KEY: str = settings.UPBIT_SECRET_KEY
    MINIO_SERVER_URL: str = settings.MLFLOW_S3_ENDPOINT_URL
    MINIO_ACCESS_KEY: str = settings.AWS_ACCESS_KEY_ID
    MINIO_SECRET_KEY: str = settings.AWS_SECRET_ACCESS_KEY

    REDIS_HOST: str = "localhost"
    REDIS_PORT: int = 6379

    @property
    def fastapi_kwargs(self) -> Dict[str, Any]:
        return {
            "title": self.TITLE,
            "version": self.VERSION,
            "servers": [{"url": self.APP_HOST, "description": settings.ENV}],
            "openapi_url": self.OPENAPI_URL,
            "docs_url": self.DOCS_URL,
            "redoc_url": self.REDOC_URL,
        }


class TestConfig(Config):
    ENV: str = "test"
    DB_URL: str = DB_URL


class LocalConfig(Config):
    ENV: str = "local"


class ProductionConfig(Config):
    LOG_LEVEL: int = logging.INFO
    APP_HOST: str = "prediction.bitcoin.com"
    OPENAPI_URL: str = "/openapi.json"
    DOCS_URL: str = ""
    REDOC_URL: str = ""
    ENV: str = "prod"


def get_config():
    env = settings.ENV
    config_type = {
        "test": TestConfig(),
        "local": LocalConfig(),
        "prod": ProductionConfig(),
    }
    return config_type[env]


def is_local():
    return get_config().ENV == "local"


config: Config = get_config()
