from minio import Minio
import mlflow
import mlflow.pyfunc
from mlflow.tracking import MlflowClient
from minio.error import S3Error
from app.core.config import config
from app.core.errors import error
from typing import Tuple
import os
import logging

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# MinIO 클라이언트 설정
minio_client = Minio(
    endpoint=config.MINIO_SERVER_URL.replace("http://", ""),  # MinIO 서버 주소
    access_key=config.MINIO_ACCESS_KEY,
    secret_key=config.MINIO_SECRET_KEY,
    secure=False,
)


def get_model_uri(bucket_name: str, model_path: str) -> str:
    """
    MinIO에서 모델의 URI를 가져옵니다.
    """
    try:
        model_directory = os.path.dirname(model_path)
        uri = f"s3://{bucket_name}/{model_directory}"
        logger.info(f"Constructed model URI: {uri}")
        return uri
    except Exception as e:
        logger.error(e)
        raise error.MinioServiceUnavailableException()


def load_model_and_metadata(model_uri: str) -> Tuple[mlflow.pyfunc.PyFuncModel, float]:
    """
    모델 URI에서 ML 모델과 메타데이터를 로드합니다.
    """
    try:
        logger.info(f"Loading model from URI: {model_uri}")

        model = mlflow.pyfunc.load_model(model_uri)
        client = mlflow.tracking.MlflowClient()
        run_id = model.metadata.run_id
        run = client.get_run(run_id)
        average_proba = run.data.metrics.get("average_proba")
        return average_proba

    except mlflow.exceptions.MlflowException as e:
        logger.error(e)
        raise error.PredictModelNotFoundException()

    except Exception as e:
        logger.error(e)
        raise error.MLflowServiceUnavailableException()


def get_production_model_uri() -> str:
    """
    MLflow를 통해 프로덕션에 있는 모델의 URI를 가져옵니다.
    """
    try:
        client = MlflowClient()
        name = f"name='btc_catboost_alpha'"
        results = client.search_model_versions(name)

        production_model = None
        for mv in results:
            if mv.current_stage == "Production":
                production_model = mv
                break

        if production_model is None:
            raise error.PredictModelNotFoundException()

        model_uri = production_model.source
        return model_uri

    except Exception as e:
        logger.error(e)
        raise error.MLflowServiceUnavailableException()


def get_latest_model_path(bucket_name: str) -> str:
    """
    MinIO 버킷에서 가장 최근에 업로드된 모델의 경로를 가져옵니다.
    """
    try:
        objects = minio_client.list_objects(bucket_name, recursive=True)
        latest_object = None
        for obj in objects:
            if latest_object is None or obj.last_modified > latest_object.last_modified:
                latest_object = obj

        if latest_object is None:
            raise error.MinioObjectNotFoundException()

        return latest_object.object_name

    except S3Error as e:
        logger.error(e)
        raise error.MinioServiceUnavailableException()

    except Exception as e:
        logger.error(e)
        raise error.MinioServiceUnavailableException()
