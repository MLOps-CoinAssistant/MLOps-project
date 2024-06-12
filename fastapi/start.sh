export AWS_ACCESS_KEY_ID=None
export AWS_SECRET_ACCESS_KEY=None
export MLFLOW_S3_ENDPOINT_URL=http://localhost:9000
export MLFLOW_TRACKING_URI=http://host:5000

uvicorn app.main:app --reload