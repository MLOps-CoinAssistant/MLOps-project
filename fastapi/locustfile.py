from locust import HttpUser, TaskSet, task, between


class ApiTaskSet(TaskSet):

    @task
    def get_btc_ohlcv(self):
        self.client.get("/v1/data/btc_ohlcv/")

    @task
    def get_btc_preprocessed(self):
        self.client.get("/v1/data/btc_preprocessed/")

    @task
    def get_latest_ohlcv(self):
        self.client.get("/v1/data/btc_ohlcv/latest/")

    @task
    def predict_btc(self):
        self.client.get("/v1/predict/btc")

    @task
    def predict_product(self):
        self.client.get("/v1/predict/btc/product")

    @task
    def get_importances(self):
        self.client.get("/v1/xai/importances")

    @task
    def read_root(self):
        self.client.get("/")

    @task
    def get_metrics(self):
        self.client.get("/metrics")


class ApiUser(HttpUser):
    tasks = [ApiTaskSet]
    wait_time = between(1, 5)
