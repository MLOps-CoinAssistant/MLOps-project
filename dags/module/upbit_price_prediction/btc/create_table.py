from airflow.providers.postgres.hooks.postgres import PostgresHook
from info.connections import Connections
from sqlalchemy import (
    create_engine,
    Column,
    DateTime,
    Integer,
    inspect,
    Float,
    Sequence,
    TIMESTAMP,
    Numeric,
)
from sqlalchemy.orm import declarative_base
from airflow.exceptions import AirflowSkipException
import logging
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

Base = declarative_base()


class BtcOhlcv(Base):
    __tablename__ = "btc_ohlcv"
    time = Column(DateTime, primary_key=True)
    open = Column(Integer)
    high = Column(Integer)
    low = Column(Integer)
    close = Column(Integer)
    volume = Column(Numeric(precision=6, scale=3))


class BtcPreprocessed(Base):
    __tablename__ = "btc_preprocessed"
    time = Column(DateTime, primary_key=True)
    open = Column(Integer)
    high = Column(Integer)
    low = Column(Integer)
    close = Column(Integer)
    volume = Column(Float)
    label = Column(Integer)
    ma_7 = Column(Integer)
    ma_14 = Column(Integer)
    ma_30 = Column(Integer)
    rsi_14 = Column(Float)
    rsi_over = Column(Float)


class BtcRsiState_75(Base):
    __tablename__ = "rsi_75_state"
    id = Column(
        Integer, Sequence("rsi_75_state_id_seq"), primary_key=True, autoincrement=True
    )
    range_start = Column(TIMESTAMP)
    range_end = Column(TIMESTAMP)
    max_rsi = Column(Float)
    max_rsi_time = Column(TIMESTAMP)


class BtcRsiState_25(Base):
    __tablename__ = "rsi_25_state"
    id = Column(
        Integer, Sequence("rsi_25_state_id_seq"), primary_key=True, autoincrement=True
    )
    range_start = Column(TIMESTAMP)
    range_end = Column(TIMESTAMP)
    min_rsi = Column(Float)
    min_rsi_time = Column(TIMESTAMP)


def create_table_fn(
    hook: PostgresHook = PostgresHook(
        postgres_conn_id=Connections.POSTGRES_DEFAULT.value
    ),
    **context: dict,
) -> None:
    """
    데이터베이스에 필요한 테이블이 존재하지 않으면 생성
    """
    s = time.time()
    engine = create_engine(hook.get_uri())
    inspector = inspect(engine)
    ti = context["ti"]
    ti.xcom_push(key="db_uri", value=hook.get_uri())
    if (
        inspector.has_table("btc_ohlcv")
        and inspector.has_table("btc_preprocessed")
        and inspector.has_table("rsi_75_state")
        and inspector.has_table("rsi_25_state")
    ):
        logger.info("ALL table already exists.")
        ti.xcom_push(key="table_created", value=False)
        e = time.time()
        es = e - s
        logger.info(f"Total working time : {es:.4f} sec")
        raise AirflowSkipException("ALL table already exists.")
    else:
        Base.metadata.create_all(engine)
        logger.info("Checked and created tables if not existing.")
        ti.xcom_push(key="table_created", value=True)

    e = time.time()
    es = e - s
    logger.info(f"Total working time : {es:.4f} sec")
