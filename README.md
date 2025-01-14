# BTC 가격 예측 MLOps 프로젝트

비트코인(BTC) 가격을 예측하기 위한 MLOps 파이프라인을 구축한 프로젝트입니다.

## 프로젝트 개요

이 프로젝트는 MLOps 기술을 활용하여 비트코인 가격을 예측하는 시스템을 구축합니다. 실시간으로 수집되는 비트코인 가격 데이터를 기반으로 머신러닝 모델을 학습하고, 예측 결과를 제공합니다.

### 사용 기술

- **Airflow**: 데이터 파이프라인 및 모델 학습 자동화
- **MLflow**: 모델 버전 관리 및 실험 추적
- **MinIO**: 모델 아티팩트 저장소
- **FastAPI**: 예측 서비스 API 제공
- **PostgreSQL**: 데이터 저장소
- **Redis**: 캐시 서버
- **Docker**: 컨테이너화 및 서비스 배포
- **Github Actions**: CI/CD 파이프라인

## 시스템 아키텍처

1. **데이터 수집 및 전처리**
   - Airflow DAG을 이용한 Upbit API 데이터 수집
   - 수집된 데이터 전처리 및 Feature Engineering

2. **모델 학습 및 관리**
   - MLflow를 이용한 모델 실험 관리
   - MinIO에 모델 아티팩트 저장
   - 자동화된 모델 재학습 파이프라인

3. **예측 서비스**
   - FastAPI 기반 REST API 제공
   - 실시간 예측 및 모델 설명 기능
   - Redis를 통한 예측 결과 캐싱

## 시작하기

### 필수 요구사항

- Docker 및 Docker Compose
- Python 3.11+

### GCP
- [development] Compute Engine / Airflow를 위한 인스턴스
    - mlflow-db-host(MinIO) 서버의 SSH public key 입력
    - 로컬 머신의 SSH public key 입력
- [development] Compute Engine / mlflow-db-host(MinIO)를 위한 인스턴스
    - Airflow 서버의 SSH public key 입력
    - 로컬 머신의 SSH public key 입력
- [development] CloudSQL / Airflow를 위한 default DB
- [development] CloudSQL / Hyperparameter Store를 위한 default DB
- GCP 무료 크레딧 이용 시, origin(source) 인스턴스 -> target 인스턴스로 워크플로우가 동작하도록 구성
    - 이 경우, **레포지토리에서 Secret 부분의 SSH_HOST에 target 인스턴스의 IP 적용**

### 설치 및 실행

1. 저장소 클론
```bash
git clone [repository-url]
cd MLOps-project
```

2. 환경 변수 설정(.env)
```bash
PROJECT_PATH=/home/ubuntu
REPOSITORY_NAME=MLOps-project

SSH_HOST=[GCP 또는 AWS 인스턴스의 IP]
SSH_USERNAME=[인스턴스의 사용자명]

ARTIFACT_ROOT=s3://mlflow/
MLFLOW_SERVER_HOST=0.0.0.0
ARTIFACT_MODEL_REGISTRY_PATH=/home/ubuntu/model_registry

ENV=local
# SSH_PRIVATE_KEY=

DB_TYPE=postgresql
DB_USER=[DB 사용자명]
DB_PW=[DB 패스워드]
DB_HOST=[DB 서버의 IP: GCP의 Cloud SQL 등]
DB_PORT=[DB 서버의 PORT: postgresql의 경우 5432]
DB_DEFAULT_NAME=[DB명: postgres]

AWS_ACCESS_KEY_ID=[MLflow Admin의 아이디]
AWS_SECRET_ACCESS_KEY=[MLflow Admin의 비밀번호]

MLFLOW_TRACKING_URI=http://host.docker.internal
MLFLOW_TRACKING_URI_LOCAL=http://localhost

MLFLOW_S3_ENDPOINT_URL=http://[MinIO 서버의 내부 IP]
MLFLOW_S3_ENDPOINT_MAIN_PORT=9000
MLFLOW_S3_ENDPOINT_SUB_PORT=9001
MINIO_ROOT_USER=[MinIO 서버의 사용자명]
MINIO_ROOT_PASSWORD=[MinIO 서버의 패스워드]
MLFLOW_TRACKING_EXTERNAL_PORT=5001
MLFLOW_TRACKING_INTERNAL_PORT=5000
MLFLOW_DB_HOST=[DB 서버의 IP: GCP의 Cloud SQL 등]

UVICORN_PORT=8000

REDIS_HOST=
REDIS_PORT=
```

#### Github repository/Secrets and Variables/Actions
- Compute Engine 인스턴스 또는 CloudSQL 구성에 변화가 있을 시, 다음의 변수들을 수정한다.
    - DB_PW
    - DB_HOST
    - MLFLOW_S3_ENDPOINT_URL
    - MLFLOW_DB_HOST


3. 서비스 실행
```bash
./run-services.sh
```

4. 서비스 중지
```bash
./stop-services.sh
```

5. 서비스 재시작(중지 및 실행)
```bash
./restart-services.sh
```

## 주요 기능

1. **BTC 가격 예측**
   - 다음 시간대(5분 이후)의 가격 상승/하락 예측

2. **모델 성능 모니터링**
   - MLflow를 통한 모델 성능 추적
   - Feature Importance 분석 (XAI)

3. **자동화된 파이프라인**
   - 데이터 수집 및 전처리 자동화
   - 모델 학습 및 배포 자동화

## API 엔드포인트

- `GET /predict`: 다음 시간대(5분 이후)의 BTC 가격 예측
- `GET /data/btc-ohlcv`: 수집된 BTC 가격 데이터 조회
- `GET /xai/importance`: 모델 특성 중요도 조회

## 라이선스

This project is licensed under the MIT License - see the LICENSE file for details.
