#!/bin/bash

# Conda 초기화
eval "$(conda shell.bash hook)"

# Conda 환경 활성화
conda activate fastapi

# 작업 디렉토리 변경
cd fastapi

# 환경 변수 설정
export AWS_ACCESS_KEY_ID=None
export AWS_SECRET_ACCESS_KEY=None
export MLFLOW_S3_ENDPOINT_URL=http://host:9000
export MLFLOW_TRACKING_URI=http://host:5000

# FastAPI 서버 시작
uvicorn app.main:app --reload