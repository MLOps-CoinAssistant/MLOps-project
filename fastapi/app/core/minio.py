from minio import Minio
import mlflow
import mlflow.pyfunc
from app.core.config import config
from typing import Tuple
import os
import logging

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# MinIO 클라이언트 설정
minio_client = Minio(
    endpoint=config.MINIO_SERVER_URL.replace("http://", ""),  # MinIO 서버 주소
    access_key=config.MINIO_ACCESS_KEY,  # 접근 키
    secret_key=config.MINIO_SECRET_KEY,  # 비밀 키
    secure=False,  # HTTPS를 사용하지 않으려면 False로 설정
)


def get_model_uri(bucket_name: str, model_path: str) -> str:
    """
    MinIO에서 모델의 URI를 가져옵니다.
    """
    model_directory = os.path.dirname(model_path)
    uri = f"s3://{bucket_name}/{model_directory}"
    logger.info(f"Constructed model URI: {uri}")
    return uri


# def get_local_model_path() -> str:
#     """
#     로컬 파일 시스템에서 가장 최근에 업로드된 모델의 경로를 가져옵니다.
#     """
#     model_directory = "/home/ubuntu/model_registry/mlflow"
#     latest_model_path = None
#     latest_timestamp = 0

#     logger.info(f"Scanning directory: {model_directory}")

#     for root, dirs, files in os.walk(model_directory):
#         for file in files:
#             if file == "MLmodel":
#                 file_path = os.path.join(root, file)
#                 file_timestamp = os.path.getmtime(file_path)
#                 logger.info(f"Found MLmodel file: {file_path}, modified at {file_timestamp}")
#                 if file_timestamp > latest_timestamp:
#                     latest_timestamp = file_timestamp
#                     latest_model_path = file_path

#     if latest_model_path is None:
#         logger.error("No model files found in the local model registry")
#         raise Exception("No model files found in the local model registry")

#     logger.info(f"Latest model path: {latest_model_path}")
#     return latest_model_path


def load_model_and_metadata(model_uri: str) -> Tuple[mlflow.pyfunc.PyFuncModel, float]:
    """
    모델 URI에서 ML 모델과 메타데이터를 로드합니다.
    """
    logger.info(f"Loading model from URI: {model_uri}")

    model = mlflow.pyfunc.load_model(model_uri)
    logger.info("1")
    client = mlflow.tracking.MlflowClient()
    logger.info("2")
    run_id = model.metadata.run_id
    logger.info("3")
    logger.info(f"run_id = {run_id}")
    run = client.get_run(run_id)
    logger.info("4")
    average_proba = run.data.metrics.get("average_proba")
    logger.info("5")
    return average_proba


def get_latest_model_path(bucket_name: str) -> str:
    """
    MinIO 버킷에서 가장 최근에 업로드된 모델의 경로를 가져옵니다.
    """
    objects = minio_client.list_objects(bucket_name, recursive=True)
    latest_object = None
    for obj in objects:
        if latest_object is None or obj.last_modified > latest_object.last_modified:
            latest_object = obj

    if latest_object is None:
        raise Exception("No objects found in the bucket")

    return latest_object.object_name
