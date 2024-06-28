from datetime import datetime
from pydantic import Field
from pydantic.dataclasses import dataclass


@dataclass
class BTCFeatureImportancesResp:
    run_id: str = Field(..., title="run id")
    experiment_name: str = Field(..., title="experiment_name")
    time: datetime = Field(..., title="Time")
    open: float = Field(..., title="Open")
    high: float = Field(..., title="High")
    low: float = Field(..., title="Low")
    close: float = Field(..., title="Close")
    volume: float = Field(..., title="Volume")
    ma_7: float = Field(..., title="MA_7")
    ma_14: float = Field(..., title="MA_14")
    ma_30: float = Field(..., title="MA_30")
    rsi_14: float = Field(..., title="RSI")
    rsi_over: float = Field(..., title="RSI_over")
