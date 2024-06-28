ERROR_400_BTC_RAW_DATA_NOT_FOUND = "40000"
ERROR_400_BTC_PREPROCESSED_DATA_NOT_FOUND = "40001"
ERROR_400_PREDICT_MODEL_NOT_FOUND = "40002"
ERROR_400_BTC_FEATURE_IMPORTACES_NOT_FOUND = "40003"
ERROR_400_OUT_OF_RANGE = "40004"
ERROR_404_TIME_DIFF_TOO_LARGE = "40005"
ERROR_404_MINIO_OBJECT_NOT_FOUND = "40006"
ERROR_401_INVALID_API_KEY = "40100"


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


class BaseAuthException(Exception):
    def __init__(self, code: str, message: str):
        self.code = code
        self.message = message


class InvalidAPIKey(BaseAuthException):
    def __init__(self):
        super().__init__(code=ERROR_401_INVALID_API_KEY, message="Invalid API Key")


class MinioObjectNotFoundException(BaseAPIException):
    def __init__(self):
        super().__init__(
            code=ERROR_404_MINIO_OBJECT_NOT_FOUND, message="MinIO object not found."
        )


class TimeDifferenceTooLargeException(BaseAPIException):
    def __init__(self):
        super().__init__(
            code=ERROR_404_TIME_DIFF_TOO_LARGE, message="Time difference is too large."
        )
