#!/bin/bash
# sudo chmod 764 start-services.sh

# 환경 변수 로드
set -a
source .env
set +a

# Conda 초기화
eval "$(conda shell.bash hook)"

# Conda 환경이 있는지 확인하고, 없으면 생성
if conda env list | grep -q "MLOps-project"; then
    conda activate MLOps-project
else
    conda create -n MLOps-project python=3.11 -y
    conda activate MLOps-project
fi

# Docker Compose로 mlflow 서비스 시작
docker-compose -f mlflow-compose.yaml up -d

# Astro Dev 서비스 시작
export $(grep -v '^#' .env | xargs)
astro dev start --compose-file compose.yaml -e .env

# backend-start.sh 스크립트 실행
./backend-start.sh &

docker ps -a

astro dev logs --webserver & 
astro dev logs --scheduler &