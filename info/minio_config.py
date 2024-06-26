import os
from enum import Enum


class MinioConfig(Enum):
    MINIO_SERVER_URL: str = os.getenv("MLFLOW_S3_ENDPOINT_URL")
    MINIO_ACCESS_KEY: str = os.getenv("AWS_ACCESS_KEY_ID")
    MINIO_SECRET_KEY: str = os.getenv("AWS_SECRET_ACCESS_KEY")
