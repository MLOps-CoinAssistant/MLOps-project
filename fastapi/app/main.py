from fastapi import FastAPI, Response
from fastapi.middleware.cors import CORSMiddleware
from starlette_context.middleware import ContextMiddleware
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from app.core.config import config
from app.core.lifespan import lifespan
from app.core.container import Container
from app.core.middlewares.sqlalchemy import SQLAlchemyMiddleware
from app.core.middlewares.metric_middleware import MetricMiddleware

from app.core.errors.error import BaseAPIException, BaseAuthException
from app.core.errors.handler import api_error_handler, api_auth_error_handler
from app.routers import router


def create_app(container=Container()) -> FastAPI:
    app = FastAPI(lifesapn=lifespan, **config.fastapi_kwargs)

    container.config.from_dict(config.model_dump())

    app.include_router(router)
    app.add_exception_handler(BaseAPIException, api_error_handler)
    app.add_exception_handler(BaseAuthException, api_auth_error_handler)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.add_middleware(SQLAlchemyMiddleware)
    app.add_middleware(ContextMiddleware)
    app.add_middleware(MetricMiddleware)

    return app


app = create_app()


@app.get("/")
def read_root():
    return {"message": "Hello World"}


@app.get("/metrics")
async def get_metrics():
    """
    프로메테우스에서 데이터 수집을 위한 엔드포인트
    """
    headers = {"Content-Type": CONTENT_TYPE_LATEST}
    content = generate_latest().decode("utf-8")
    return Response(content=content, media_type="text/plain", headers=headers)
