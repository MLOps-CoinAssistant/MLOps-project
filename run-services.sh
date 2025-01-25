#!/bin/bash
# sudo chmod 764 start-services.sh

# 스크립트 실행 중 에러 발생 시 중지
set -e
# 에러 발생 시 메시지 출력
trap 'echo "An error occurred. Exiting..."; exit 1;' ERR

# 환경 변수 로드
set -a
source .env
set +a

# Conda 초기화
eval "$(conda shell.bash hook)"
source ~/.bashrc

# 사용할 conda 환경 이름
ENV_NAME="mlops-pipeline"

# 현재 conda 환경 확인
if ! conda env list | grep -q "$ENV_NAME"; then
    echo "Creating new conda environment: $ENV_NAME"
    conda create -n "$ENV_NAME" python=3.11 -y
fi

# Conda 환경 활성화
echo "Activating conda environment: $ENV_NAME"
conda activate "$ENV_NAME"

# mlflow 서비스가 이미 실행 중인지 확인
if ! docker ps -a | grep -q mlflow; then
    echo "Starting MLflow service..."
    docker-compose -f mlflow-compose.yaml build --no-cache
    docker-compose -f mlflow-compose.yaml up -d
fi

# ml-ops-proj 서비스가 이미 실행 중인지 확인
if ! docker ps -a | grep -q ml-ops-proj; then
    echo "Starting Airflow service..."
    export $(grep -v '^#' .env | xargs)
    astro dev start --compose-file compose.yaml -e .env
fi

# backend 서비스가 이미 실행 중인지 확인
# if ! ps -a | grep -q fastapi && ! ps -a | grep -q uvicorn; then
#     ./run-local-backend.sh &
# fi

# Docker 상태 확인
echo "Current Docker containers:"
docker ps -a

# Astro Dev 로그 확인
# astro dev logs --webserver &
# astro dev logs --scheduler &
