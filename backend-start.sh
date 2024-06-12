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
poetry install

# 환경 변수 설정
export AWS_ACCESS_KEY_ID=None
export AWS_SECRET_ACCESS_KEY=None
export MLFLOW_S3_ENDPOINT_URL=http://host:9000
export MLFLOW_TRACKING_URI=http://host:5000

# FastAPI 서버 시작
uvicorn app.main:app --reload