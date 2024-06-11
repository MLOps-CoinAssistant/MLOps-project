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
    volume: int = Field(..., title="Volume")


@dataclass
class BtcPreprocessedResp:
    time: datetime = Field(..., title="Time")
    open: int = Field(..., title="Open")
    high: int = Field(..., title="High")
    low: int = Field(..., title="Low")
    close: int = Field(..., title="Close")
    volume: int = Field(..., title="Volume")
