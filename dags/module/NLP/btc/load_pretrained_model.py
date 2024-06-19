from google.cloud import storage
import mlflow.pyfunc
import mlflow
import os
import logging
# from sentence_transformers import SentenceTransformer
# import shutil


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# GCS 설정
bucket_name = "pre-trained-model-bucket" # GCS 버킷 이름
source_dir = "/usr/local/pre-trained/"  # Airflow 컨테이너 로컬 모델 파일 경로
destination_dir = "model"  # GCS 버킷 내부 경로
study_and_experiment_name = "pre-trained_NLP_model" 
local_model_path = "/tmp/pre-trained" # Airflow 컨테이너 로컬에 다운로드 할 모델 경로

# GCS 버킷과 서버 모두에 pre-trained 모델이 없을 경우
# 직접 다운로드하기 위한 원본 경로와 대상 경로 설정
source_base_dir = os.path.expanduser('~/.cache/huggingface/hub')
target_base_dir = '/home/ubuntu/pre-trained' # 실제 서버 내부 로컬
target_model_name = 'msmarco-distilbert-base-v4'

def upload_to_gcs(bucket_name, source_dir, destination_dir):
    """로컬 디렉토리의 파일들을 GCS 버킷에 업로드"""
    storage_client = storage.Client() 
    bucket = storage_client.bucket(bucket_name) # GCS 버킷 객체 생성
    
    uploaded_files = []
    
    logger.info(f"Starting file upload from {source_dir} to bucket {bucket_name} in directory {destination_dir}")

    if not os.path.exists(source_dir):
        logger.error(f"Source directory {source_dir} does not exist.")
        return uploaded_files
    
    for root, _, files in os.walk(source_dir): # 디렉토리 탐색. root: 현재 디렉토리, files: 파일 리스트. 
        logger.info(f"Current directory: {root}, files: {files}")
        for file in files:
            local_file_path = os.path.join(root, file)
            logger.info(f"Found file: {local_file_path}")
            if os.path.isfile(local_file_path): # 파일인지 확인
                relative_path = os.path.relpath(local_file_path, source_dir) # 상대 경로
                gcs_blob_name = os.path.join(destination_dir, relative_path) # GCS 경로
                blob = bucket.blob(gcs_blob_name) # GCS blob 객체 생성. blob 객체: GCS 버킷 내 파일을 가리키는 포인터
            
                # 파일이 GCS에 이미 존재하는지 확인
                if blob.exists():
                    logger.info(f"File {gcs_blob_name} already exists in GCS, skipping upload.")
                else:
                    logger.info(f"Preparing to upload {local_file_path} to {gcs_blob_name}")

                    # 업로드 시도 및 예외 처리
                    try:
                        blob.upload_from_filename(local_file_path)
                        logger.info(f"File {local_file_path} uploaded to {gcs_blob_name}.")
                        uploaded_files.append(gcs_blob_name)
                    except Exception as e:
                        logger.error(f"Failed to upload {local_file_path} to GCS: {e}")
            else:
                logger.error(f"Path is not a file: {local_file_path}")
    logger.info(f"Uploaded files: {uploaded_files}")
    return uploaded_files

def list_gcs_files(bucket_name, destination_dir):
    """GCS 버킷의 파일 목록을 반환"""
    storage_client = storage.Client()
    blobs = storage_client.list_blobs(bucket_name, prefix=destination_dir)
    gcs_files = [blob.name for blob in blobs]
    return gcs_files

def check_model_exists(model_name):
    """기존 모델이 있는지 확인"""
    client = mlflow.tracking.MlflowClient()
    try:
        latest_versions = client.get_latest_versions(model_name) # 모델 이름으로 최신 버전 가져오기
        if latest_versions:
            logger.info(f"Model {model_name} already exists.")
            return True
        else:
            logger.info(f"Model {model_name} does not exist.")
            return False
    except mlflow.exceptions.RestException:
        logger.info(f"Model {model_name} does not exist.")
        return False

def get_existing_run_id(experiment_name):
    """기존 실험의 run ID를 가져옴"""
    client = mlflow.tracking.MlflowClient()
    experiment = client.get_experiment_by_name(experiment_name)
    if experiment:
        experiment_id = experiment.experiment_id
        runs = client.search_runs([experiment_id], order_by=["attribute.start_time DESC"]) # 실험의 모든 run을 가져옴. 최신순으로 정렬
        if runs:
            return runs[0].info.run_id # 가장 최근의 run ID 반환
    return None

def log_model_to_mlflow(run_id, bucket_name, destination_dir):
    """GCS 파일들을 MLflow 모델로 로깅"""
    try:
        mlflow.log_artifact(local_model_path, artifact_path="model") # 로컬 모델 파일을 MLflow 아티팩트로 로깅. artifact_path: MLflow 아티팩트 경로
        logger.info(f"Model artifacts logged from {local_model_path}")
    except Exception as e:
        logger.error(f"Failed to log model to MLflow: {e}")
        raise
    
def register_model(run_id, model_name):
    """모델을 등록"""
    model_uri = f"runs:/{run_id}/model" # MLflow 모델 URI
    try:
        registered_model = mlflow.register_model(model_uri, model_name) # 모델 등록
        logger.info(f"Model registered: {registered_model.name}, version: {registered_model.version}")
    except Exception as e:
        logger.error(f"Model registration failed: {e}")
        raise

def download_from_gcs(bucket_name, gcs_blob_name, local_file_path):
    """GCS에서 파일을 컨테이너 로컬로 다운로드"""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(gcs_blob_name)
    blob.download_to_filename(local_file_path) # GCS 파일을 로컬로 다운로드
    logger.info(f"Downloaded {gcs_blob_name} to {local_file_path}")
    
def download_and_log_model(**context):
    # 서버에 pre-trained 폴더가 있는지 확인하고 없으면 GCS에서 다운로드
    if not os.path.exists(source_dir):
        logger.warning(f"Source directory {source_dir} does not exist. Skipping upload to GCS.")
        uploaded_files = list_gcs_files(bucket_name, destination_dir)
    else:
        # GCS로 파일 업로드
        logger.info("Uploading files to GCS")
        uploaded_files = upload_to_gcs(bucket_name, source_dir, destination_dir)
        if not uploaded_files:
            logger.warning("No new files were uploaded to GCS. Checking existing files in GCS.")
            uploaded_files = list_gcs_files(bucket_name, destination_dir)
        else:
            logger.info("Files to be logged to MLflow: " + ", ".join(uploaded_files))
        
    mlflow.set_experiment(study_and_experiment_name)
    
    model_exists = check_model_exists(study_and_experiment_name)
    run_id = get_existing_run_id(study_and_experiment_name)
    
    if run_id:
        logger.info(f"Using existing run ID: {run_id}")
    else:
        with mlflow.start_run() as run:
            run_id = run.info.run_id
            logger.info(f"Starting a new run with run ID: {run_id}")
            
    # MLflow 실행 시작
    with mlflow.start_run(run_id=run_id) as run:
        logger.info("Logging artifacts to MLflow")
        
        # GCS 파일들을 로컬로 다운로드하고 MLflow 아티팩트로 로깅
        os.makedirs(local_model_path, exist_ok=True)
        
        for file in uploaded_files:
            gcs_blob_name = file
            
            local_file_path = os.path.join(local_model_path, os.path.basename(file))
            # 로컬에 파일이 있는지 확인
            if os.path.exists(local_file_path):
                logger.info(f"File {local_file_path} already exists locally, skipping download.")
            else:
                # 파일이 로컬에 없으면 GCS에서 다운로드
                download_from_gcs(bucket_name, gcs_blob_name, local_file_path)
            try:
                mlflow.log_artifact(local_file_path, artifact_path="model")
                logger.info(f"Model artifacts logged from {local_model_path}")
            except Exception as e:
                logger.error(f"Failed to log model to MLflow: {e}")
                raise
        
        if not model_exists:
            # 모델 로깅
            log_model_to_mlflow(run_id, bucket_name, destination_dir)
            # 모델 등록
            register_model(run_id, study_and_experiment_name)
        else:
            logger.info(f"Model {study_and_experiment_name} already exists in the registry. Skipping registration.")
            logger.info(f"Alternatively, It would be used with the previous run {run.info.run_id}.")