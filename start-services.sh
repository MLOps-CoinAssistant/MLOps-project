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

# 현재 conda 환경 확인
CURRENT_ENV=$(conda info --envs | grep '*' | awk '{print $1}')
if [ "$CURRENT_ENV" != "MLOps-project" ]; then
    if conda env list | grep -q "MLOps-project"; then
        conda activate MLOps-project
    else
        conda create -n MLOps-project python=3.11 -y
        conda activate MLOps-project
    fi
fi

# mlflow 서비스가 이미 실행 중인지 확인
if ! docker ps -a | grep -q mlflow; then
    docker-compose -f mlflow-compose.yaml up -d
fi

# ml-ops-proj 서비스가 이미 실행 중인지 확인
if ! docker ps -a | grep -q ml-ops-proj; then
    export $(grep -v '^#' .env | xargs)
    astro dev start --compose-file compose.yaml -e .env
fi

# backend 서비스가 이미 실행 중인지 확인
if ! ps -a | grep -q backend && ! ps -a | grep -q uvicorn; then
    ./backend-start.sh &
fi

# Docker 상태 확인
docker ps -a

# Astro Dev 로그 확인
astro dev logs --webserver &
astro dev logs --scheduler &
