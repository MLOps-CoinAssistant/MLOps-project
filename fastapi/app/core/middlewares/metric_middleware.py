import time

from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware

from app.stats.stat import successful_requests, failed_requests, request_latency


class MetricMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        method = request.method
        endpoint = request.url.path

        start_time = time.perf_counter()

        response = await call_next(request)

        if endpoint in ("/metrics", "/docs", "/openapi.json"):
            return response

        elapsed_time = time.perf_counter() - start_time

        if response.status_code < 400:
            successful_requests.labels(method=method, endpoint=endpoint).inc()
        else:
            failed_requests.labels(method=method, endpoint=endpoint).inc()

        request_latency.labels(method=method, endpoint=endpoint).observe(elapsed_time)

        return response
