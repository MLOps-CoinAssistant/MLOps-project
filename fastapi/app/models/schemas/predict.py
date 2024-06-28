from pydantic import BaseModel, Field
from typing import Optional


class BtcPredictionResp(BaseModel):
    prediction: str = Field(..., title="예측 결과", alias="예측 결과")
    confidence: Optional[float] = Field(
        None, title="확률", description="확률 (%)", alias="확률 (%)"
    )

    class Config:
        populate_by_name = True


class BtcPredictionNoConfidenceResp(BaseModel):
    prediction: str = Field(..., title="예측 결과", alias="예측 결과")

    class Config:
        populate_by_name = True
