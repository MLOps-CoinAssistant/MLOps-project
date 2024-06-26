from app import repositories
from app.models.schemas.predict import BtcPredictionResp
from typing import List


class PredictService:
    def __init__(self, predict_repository: repositories.PredictRepository):
        self.predict_repository = predict_repository

    async def predict(self) -> List[BtcPredictionResp]:

        return await self.predict_repository.predict()

    async def predict_product(self) -> List[BtcPredictionResp]:

        return await self.predict_repository.predict_product()
