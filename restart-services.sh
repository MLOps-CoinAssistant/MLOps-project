#!/bin/bash

# 현재 conda 환경 확인
# 사용할 conda 환경 이름
ENV_NAME="mlops-pipeline"

# 현재 conda 환경 확인
CURRENT_ENV=$(conda info --envs | grep '*' | awk '{print $1}')
if [ "$CURRENT_ENV" != "$ENV_NAME" ]; then
    if conda env list | grep -q "$ENV_NAME"; then
        conda activate "$ENV_NAME"
    else
        conda create -n "$ENV_NAME" python=3.11 -y
        conda activate "$ENV_NAME"
    fi
fi

# mlflow 서비스가 이미 실행 중인지 확인
if docker ps -a | grep -q mlflow; then
    docker-compose -f mlflow-compose.yaml down -d
fi
docker-compose -f mlflow-compose.yaml up -d

# Airflow ml-ops-proj 서비스가 이미 실행 중인지 확인
if docker ps -a | grep -q ml-ops-proj; then
    # export $(grep -v '^#' .env | xargs)
    # astro dev start --compose-file compose.yaml -e .env
    astro dev stop && astro dev kill
fi
export $(grep -v '^#' .env | xargs)
astro dev start --compose-file compose.yaml -e .env

# Docker 상태 확인
docker ps -a

# Astro Dev 로그 확인
# astro dev logs --webserver &
# astro dev logs --scheduler &