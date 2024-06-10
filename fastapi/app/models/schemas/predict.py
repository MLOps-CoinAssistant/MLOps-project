from pydantic import BaseModel, Field


class BtcPredictionResp(BaseModel):
    prediction: str = Field(..., title="예측 결과", alias="예측 결과")
    confidence: float = Field(
        ..., title="신뢰도", description="신뢰도 (%)", alias="신뢰도 (%)"
    )

    class Config:
        populate_by_name = True
