#!/bin/bash
# sudo chmod 764 stop-services.sh

# 환경 변수 로드
set -a
source .env
set +a

# Conda 초기화
eval "$(conda shell.bash hook)"

# backend-start.sh로 실행된 프로세스 중지
backend_process=$(ps -a | grep backend | awk '{print $1}')
if [ -n "$backend_process" ]; then
  kill -9 $backend_process
fi

uvicorn_process=$(ps -a | grep uv | awk '{print $1}')
if [ -n "$uvicorn_process" ]; then
  kill -9 $uvicorn_process
fi

uvicorn_docker_process=$(docker ps -a | grep entrypoint | awk '{print $1}')
if [ -n "$uvicorn_docker_process" ]; then
  docker stop $uvicorn_docker_process && docker rm $uvicorn_docker_process
  # kill -9 $uvicorn_docker_process
fi

# 포트 8000을 사용 중인 모든 프로세스 종료
pids=$(sudo lsof -t -i :8000)
if [ -n "$pids" ]; then
  sudo kill -9 $pids
fi

conda deactivate

# Astro Dev 서비스 중지
astro dev kill

# Docker Compose로 mlflow 서비스 중지
docker-compose -f mlflow-compose.yaml down

# mlflow-server 관련 컨테이너 중지
mlflow_containers=$(docker ps -a | grep mlflow | awk '{print $1}')
if [ -n "$mlflow_containers" ]; then
  docker stop $mlflow_containers
  docker rm $mlflow_containers
fi

conda deactivate

# 현재 실행 중인 모든 컨테이너 목록 표시
docker ps -a
ps -a
