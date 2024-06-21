from datetime import datetime
from pydantic import Field
from pydantic.dataclasses import dataclass


@dataclass
class BtcOhlcvResp:
    time: datetime = Field(..., title="Time")
    open: int = Field(..., title="Open")
    high: int = Field(..., title="High")
    low: int = Field(..., title="Low")
    close: int = Field(..., title="Close")
    volume: float = Field(..., title="Volume")


@dataclass
class BtcPreprocessedResp:
    time: datetime = Field(..., title="Time")
    open: int = Field(..., title="Open")
    high: int = Field(..., title="High")
    low: int = Field(..., title="Low")
    close: int = Field(..., title="Close")
    volume: float = Field(..., title="Volume")
    ma_7: int = Field(..., title="MA_7")
    ma_14: int = Field(..., title="MA_14")
    ma_30: int = Field(..., title="MA_30")
    rsi_14: float = Field(..., title="RSI")
    rsi_over: float = Field(..., title="RSI_over")
