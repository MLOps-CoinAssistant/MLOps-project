from contextlib import asynccontextmanager
from fastapi import FastAPI
import logging

from app.core.redis import redis_cache
from app.core.db.session import ping_db, close_db


@asynccontextmanager
async def lifespan(app: FastAPI):
    logging.info(f"Starting up...")
    await ping_db()
    await redis_cache.ping()

    yield

    await close_db()
    await redis_cache.close()
    logging.info(f"Shutting down...")
