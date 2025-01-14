#!/bin/bash

# Conda 초기화 및 환경 설정
eval "$(conda shell.bash hook)"
source ~/.bashrc

# 현재 디렉토리 저장
CURRENT_DIR=$(pwd)

echo " Stopping all services..."
./stop-services.sh

echo " Starting all services..."
./run-services.sh

# 원래 디렉토리로 복귀
cd $CURRENT_DIR

echo " All services have been restarted successfully!"