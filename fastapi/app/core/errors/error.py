ERROR_400_BTC_RAW_DATA_NOT_FOUND = "40000"
ERROR_400_BTC_PREPROCESSED_DATA_NOT_FOUND = "40001"
ERROR_400_PREDICT_MODEL_NOT_FOUND = "40002"
ERROR_400_BTC_FEATURE_IMPORTACES_NOT_FOUND = "40003"
ERROR_400_OUT_OF_RANGE = "40003"
ERROR_401_INVALID_API_KEY = "40100"
ERROR_404_MINIO_OBJECT_NOT_FOUND = "40400"
ERROR_503_UPBIT_SERVICE_UNAVAILABLE = "50300"
ERROR_503_MINIO_SERVICE_UNAVAILABLE = "50301"
ERROR_503_MLFLOW_SERVICE_UNAVAILABLE = "50302"


class BaseAPIException(Exception):
    def __init__(self, code: str, message: str):
        self.code = code
        self.message = message


class BtcOhlcvNotFoundException(BaseAPIException):
    def __init__(self):
        super().__init__(
            code=ERROR_400_BTC_RAW_DATA_NOT_FOUND, message="BtcOhlcv not found."
        )


class BtcPreprocessedNotFoundException(BaseAPIException):
    def __init__(self):
        super().__init__(
            code=ERROR_400_BTC_PREPROCESSED_DATA_NOT_FOUND,
            message="BtcPreprocessed not found.",
        )


class BtcFeatureImportancesNotFoundException(BaseAPIException):
    def __init__(self):
        super().__init__(
            code=ERROR_400_BTC_FEATURE_IMPORTACES_NOT_FOUND,
            message="BtcFeatureImportances is not found.",
        )


class OutOfRangeException(BaseAPIException):
    def __init__(self):
        super().__init__(
            code=ERROR_400_OUT_OF_RANGE,
            message="Your request is out of the allowed range.",
        )


class PredictModelNotFoundException(BaseAPIException):
    def __init__(self):
        super().__init__(
            code=ERROR_400_PREDICT_MODEL_NOT_FOUND, message="PredictModel not found."
        )


class UpbitServiceUnavailableException(BaseAPIException):
    def __init__(self):
        super().__init__(
            code=ERROR_503_UPBIT_SERVICE_UNAVAILABLE,
            message="Upbit service is unavailable.",
        )


class BaseAuthException(Exception):
    def __init__(self, code: str, message: str):
        self.code = code
        self.message = message


class InvalidAPIKey(BaseAuthException):
    def __init__(self):
        super().__init__(code=ERROR_401_INVALID_API_KEY, message="Invalid API Key")


class MinioServiceUnavailableException(BaseAPIException):
    def __init__(self):
        super().__init__(
            code=ERROR_503_MINIO_SERVICE_UNAVAILABLE,
            message="MinIO service is unavailable.",
        )


class MLflowServiceUnavailableException(BaseAPIException):
    def __init__(self):
        super().__init__(
            code=ERROR_503_MLFLOW_SERVICE_UNAVAILABLE,
            message="MLflow service is unavailable.",
        )


class MinioObjectNotFoundException(BaseAPIException):
    def __init__(self):
        super().__init__(
            code=ERROR_404_MINIO_OBJECT_NOT_FOUND, message="MinIO object not found."
        )
