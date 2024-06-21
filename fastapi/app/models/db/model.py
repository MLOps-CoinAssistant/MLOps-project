from datetime import datetime
from sqlalchemy import DateTime, Integer, Float
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from app.core.db.session import Base


class BtcOhlcv(Base):
    __tablename__ = "btc_ohlcv"

    time: Mapped[datetime] = mapped_column(DateTime, primary_key=True)
    open: Mapped[int] = mapped_column(Integer)
    high: Mapped[int] = mapped_column(Integer)
    low: Mapped[int] = mapped_column(Integer)
    close: Mapped[int] = mapped_column(Integer)
    volume: Mapped[float] = mapped_column(Float)


class BtcPreprocessed(Base):
    __tablename__ = "btc_preprocessed"

    time: Mapped[datetime] = mapped_column(DateTime, primary_key=True)
    open: Mapped[int] = mapped_column(Integer)
    high: Mapped[int] = mapped_column(Integer)
    low: Mapped[int] = mapped_column(Integer)
    close: Mapped[int] = mapped_column(Integer)
    volume: Mapped[float] = mapped_column(Float)
    label: Mapped[int] = mapped_column(Integer)
    ma_7: Mapped[int] = mapped_column(Integer)
    ma_14: Mapped[int] = mapped_column(Integer)
    ma_30: Mapped[int] = mapped_column(Integer)
    rsi_14: Mapped[float] = mapped_column(Float)
    rsi_over: Mapped[float] = mapped_column(Float)
