from pydantic import BaseModel, Field


class BtcPredictionResp(BaseModel):
    prediction: str = Field(..., title="예측 결과", alias="예측 결과")
    confidence: float = Field(
        ..., title="확률", description="확률 (%)", alias="확률 (%)"
    )

    class Config:
        populate_by_name = True
