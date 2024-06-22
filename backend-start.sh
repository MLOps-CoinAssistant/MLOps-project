#!/bin/bash

# Conda 초기화
eval "$(conda shell.bash hook)"

# Conda 환경이 있는지 확인하고, 없으면 생성
if conda env list | grep -q "fastapi"; then
    conda activate fastapi
else
    conda create -n fastapi python=3.11 -y
    conda activate fastapi
fi

# 작업 디렉토리 변경
cd fastapi

# Poetry 설치 및 종속성 설치
pip install poetry
poetry install --no-root

# Redis 컨테이너가 이미 실행 중인지 확인하고, 실행 중이지 않으면 실행
if [ ! "$(docker ps -q -f name=redis)" ]; then
    if [ "$(docker ps -aq -f status=exited -f name=redis)" ]; then
        # Cleanup
        docker rm redis
    fi
    # Run Redis container
    docker run --name redis -p 6379:6379 -d redis:7.2
fi

export ENV=local
export AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}
export AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}
export MLFLOW_S3_ENDPOINT_URL=${MLFLOW_S3_ENDPOINT_URL}:${MLFLOW_S3_ENDPOINT_MAIN_PORT}
export MLFLOW_TRACKING_URI=${MLFLOW_TRACKING_URI_LOCAL}:${MLFLOW_TRACKING_PORT}

# FastAPI 서버 시작
poetry run uvicorn app.main:app --reload
