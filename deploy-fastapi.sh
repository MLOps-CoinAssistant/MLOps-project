#!/bin/bash

# .env 파일 로드
if [ -f .env ]; then
    export $(cat .env | grep -v '^#' | xargs)
fi

# docker image build
    # docker build -t $REGION-docker.pkg.dev/$PROJECT_ID/$IMAGE_NAME/latest .
        # -f ./fastapi/Dockerfile

# docker push
    # docker push $REGION-docker.pkg.dev/$PROJECT_ID/$IMAGE_NAME/latest

# docker run on local
    # docker run --env-file .env -p 8000:8000 $REGION-docker.pkg.dev/$PROJECT_ID/$IMAGE_NAME/latest # 로컬

    # HEALTH CHECK
        # docker run --env-file .env -p 8001:8000 $REGION-docker.pkg.dev/$PROJECT_ID/$IMAGE_NAME/latest

cd fastapi

# gcloud builds submit --tag $REGION-docker.pkg.dev/$PROJECT_ID/$IMAGE_NAME/latest .
gcloud builds submit --tag $REGION-docker.pkg.dev/$PROJECT_ID/$IMAGE_NAME/latest .

gcloud run deploy fastapi-app \
    --image $REGION-docker.pkg.dev/$PROJECT_ID/$IMAGE_NAME/latest \
    --region $REGION \
    --platform managed \
    --vpc-connector default \
    --allow-unauthenticated \
    --concurrency 80 \
    --set-env-vars AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY,MLFLOW_TRACKING_URI=$MLFLOW_TRACKING_URI,MLFLOW_TRACKING_URI_LOCAL=$MLFLOW_TRACKING_URI_LOCAL,MLFLOW_TRACKING_PORT=$MLFLOW_TRACKING_PORT,MLFLOW_S3_ENDPOINT_URL=$MLFLOW_S3_ENDPOINT_URL,MLFLOW_S3_ENDPOINT_MAIN_PORT=$MLFLOW_S3_ENDPOINT_MAIN_PORT,MLFLOW_S3_ENDPOINT_SUB_PORT=$MLFLOW_S3_ENDPOINT_SUB_PORT,MINIO_ROOT_USER=$MINIO_ROOT_USER,MINIO_ROOT_PASSWORD=$MINIO_ROOT_PASSWORD,MLFLOW_DB_HOST=$MLFLOW_DB_HOST,DB_TYPE=$DB_TYPE,DB_USER=$DB_USER,DB_PW=$DB_PW,DB_HOST=$DB_HOST,DB_DEFAULT_NAME=$DB_DEFAULT_NAME,ARTIFACT_ROOT=$ARTIFACT_ROOT,MLFLOW_SERVER_HOST=$MLFLOW_SERVER_HOST,ARTIFACT_MODEL_REGISTRY_PATH=$ARTIFACT_MODEL_REGISTRY_PATH,AIRFLOW_SMTP_USER=$AIRFLOW_SMTP_USER,SMTP_MAIL_ADDRESS=$SMTP_MAIL_ADDRESS,AIRFLOW_SMTP_PASSWORD=$AIRFLOW_SMTP_PASSWORD,UPBIT_ACCESS_KEY=$UPBIT_ACCESS_KEY,UPBIT_SECRET_KEY=$UPBIT_SECRET_KEY,ENV=$ENV,DB_PORT=$DB_PORT,UVICORN_PORT=$UVICORN_PORT,db_url=$REDIS_HOST_FOR_CLOUD_RUN,REDIS_HOST=$REDIS_HOST_FOR_CLOUD_RUN,REDIS_PORT=$REDIS_PORT
