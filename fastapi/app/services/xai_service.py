from app import repositories
from app.models.schemas.xai import BTCFeatureImportancesResp
from typing import List


class XaiService:
    def __init__(self, xai_repository: repositories.XaiRepository):
        self.xai_repository = xai_repository

    async def get_importances(
        self, skip: int, limit: int
    ) -> List[BTCFeatureImportancesResp]:

        return await self.xai_repository.get_importances(skip=skip, limit=limit)
