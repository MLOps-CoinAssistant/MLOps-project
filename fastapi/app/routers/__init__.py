from fastapi import APIRouter, Depends

from app.routers import data, predict, xai


router = APIRouter(prefix="/v1")

router.include_router(data.router, prefix="/data", tags=["data"])
router.include_router(predict.router, prefix="/predict", tags=["predict"])
router.include_router(xai.router, prefix="/xai", tags=["xai"])
