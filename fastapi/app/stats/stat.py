from prometheus_client import Counter, Histogram


successful_requests = Counter(
    "successful_requests", "Number of successful requests", ["method", "endpoint"]
)

failed_requests = Counter(
    "failed_requests", "Number of failed requests", ["method", "endpoint"]
)

request_latency = Histogram(
    "request_latency_seconds",
    "Request latency",
    ["method", "endpoint"],
    buckets=[0.01, 0.1, 0.5, 1],
)
