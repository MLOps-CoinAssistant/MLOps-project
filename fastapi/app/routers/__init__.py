from fastapi import APIRouter, Depends

from app.routers import data_test, predict, xai


router = APIRouter(prefix="/v1")

router.include_router(data_test.router, prefix="/data", tags=["data"])
router.include_router(predict.router, prefix="/predict", tags=["predict"])
router.include_router(xai.router, prefix="/xai", tags=["xai"])
