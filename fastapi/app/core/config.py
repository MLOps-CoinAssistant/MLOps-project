import os
import logging
from typing import Dict, Any
from dotenv import load_dotenv
load_dotenv()

from pydantic_settings import BaseSettings

# 로깅 설정
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

class Settings(BaseSettings):
    DB_TYPE: str = os.getenv('DB_TYPE')
    DB_USER: str = os.getenv('DB_USER')
    DB_PW: str = os.getenv('DB_PW')
    DB_HOST: str = os.getenv('DB_HOST')
    DB_PORT: int = os.getenv('DB_PORT')
    DB_NAME: str = os.getenv('DB_DEFAULT_NAME')
    
    class Config:
       env_file = '.env'
       env_file_encoding = 'utf-8'
       

settings = Settings()
DB_URL: str = f"{settings.DB_TYPE}+asyncpg://{settings.DB_USER}:{settings.DB_PW}@{settings.DB_HOST}:{settings.DB_PORT}/{settings.DB_NAME}"

class Config(BaseSettings):
    DB_URL: str = DB_URL
    ENV: str = "dev"
    TITLE: str = "Prediction Bitcoin Price"
    VERSION: str = "0.1.0"
    APP_HOST: str = "http://localhost:8000"
    OPENAPI_URL: str = "/openapi.json"
    DOCS_URL: str = "/docs"
    REDOC_URL: str = "/redoc"

    LOG_LEVEL: int = logging.DEBUG

    MINIO_SERVER_URL: str = os.getenv("MLFLOW_S3_ENDPOINT_URL")
    MINIO_ACCESS_KEY: str = os.getenv("AWS_ACCESS_KEY_ID")
    MINIO_SECRET_KEY: str = os.getenv("AWS_SECRET_ACCESS_KEY")

    REDIS_HOST: str = "localhost"
    REDIS_PORT: int = 6379

    @property
    def fastapi_kwargs(self) -> Dict[str, Any]:
        return {
            "title": self.TITLE,
            "version": self.VERSION,
            "servers": [
                {"url": self.APP_HOST, "description": os.getenv("ENV", "local")}
            ],
            "openapi_url": self.OPENAPI_URL,
            "docs_url": self.DOCS_URL,
            "redoc_url": self.REDOC_URL,
        }


class TestConfig(Config):
    DB_URL: str = DB_URL


class LocalConfig(Config):
    pass


class ProductionConfig(Config):
    LOG_LEVEL: int = logging.INFO
    APP_HOST: str = "prediction.bitcoin.com"

    OPENAPI_URL: str = "/openapi.json"
    DOCS_URL: str = ""
    REDOC_URL: str = ""


def get_config():
    env = os.getenv("ENV", "local")
    config_type = {
        "test": TestConfig(),
        "local": LocalConfig(),
        "prod": ProductionConfig(),
    }
    return config_type[env]


def is_local():
    return get_config().ENV == "local"


config: Config = get_config()