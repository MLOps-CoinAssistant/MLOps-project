from fastapi import FastAPI, status
from fastapi.middleware.cors import CORSMiddleware
from starlette_context.middleware import ContextMiddleware
import logging
import aiohttp

from app.core.config import config
from app.core.lifespan import lifespan
from app.core.container import Container
from app.core.middlewares.sqlalchemy import SQLAlchemyMiddleware
from app.core.errors.error import BaseAPIException, BaseAuthException
from app.core.errors.handler import api_error_handler, api_auth_error_handler
from app.routers import router
from app.core.db.session import ping_db


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

    return app


app = create_app()


@app.get("/")
def read_root():
    return {
        "message": f"It's BTC price prediction backend server. {config.MINIO_SERVER_URL}"
    }


@app.get("/healthcheck")
async def healthcheck():
    try:
        await ping_db()
        return {"status": "healthy"}
    except Exception as e:
        return {"status": "unhealthy", "details": str(e)}


@app.get("/health/minio", status_code=status.HTTP_200_OK)
async def healthcheck_minio():
    async with aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=10)
    ) as session:
        try:
            async with session.get(
                f"{config.MINIO_SERVER_URL}/minio/health/live"
            ) as response:
                if response.status == 200:
                    return {"status": "Minio is healthy"}
                else:
                    details = await response.text()
                    logging.error(f"Minio healthcheck failed: {details}")
                    return {
                        "status": "Minio is not healthy",
                        "details": await response.text(),
                    }
        except Exception as e:
            logging.error(f"Minio healthcheck failed: {str(e)}")
            return {"status": "Minio healthcheck failed", "error": str(e)}


@app.get("/health/mlflow", status_code=status.HTTP_200_OK)
async def healthcheck_mlflow():
    async with aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=10)
    ) as session:
        try:
            async with session.get(
                f"{config.MLFLOW_TRACKING_URI_LOCAL}:{config.MLFLOW_TRACKING_PORT}"
            ) as response:
                if response.status == 200:
                    return {"status": "MLflow is healthy"}
                else:
                    details = await response.text()
                    logging.error(f"MLflow healthcheck failed: {details}")
                    return {
                        "status": "MLflow is not healthy",
                        "details": await response.text(),
                    }
        except Exception as e:
            logging.error(f"MLflow healthcheck failed: {str(e)}")
            return {"status": "MLflow healthcheck failed", "error": str(e)}
