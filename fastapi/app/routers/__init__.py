from fastapi import APIRouter, Depends

from app.routers import ml_router


router = APIRouter(prefix="/v1")

router.include_router(ml_router.router, prefix="/model", tags=["model"])
