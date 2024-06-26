# .env 파일 로드
# export $(grep -v '^#' ../.env | xargs)

# uvicorn 명령어로 FastAPI 애플리케이션 실행
poetry run uvicorn app.main:app --host 0.0.0.0 --port $UVICORN_PORT
