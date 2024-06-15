from airflow.providers.postgres.hooks.postgres import PostgresHook
from info.connections import Connections
from sqlalchemy import create_engine, Column, DateTime, Integer, inspect
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
    volume = Column(Integer)


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
    if inspector.has_table("btc_ohlcv"):
        logger.info("Table 'btc_ohlcv' already exists.")
        ti.xcom_push(key="table_created", value=False)
        ti.xcom_push(key="db_uri", value=hook.get_uri())
        e = time.time()
        es = e - s
        logger.info(f"Total working time : {es:.4f} sec")
        raise AirflowSkipException("Table 'btc_ohlcv' already exists.")
    else:
        Base.metadata.create_all(engine)
        logger.info("Checked and created tables if not existing.")
        ti.xcom_push(key="table_created", value=True)
        ti.xcom_push(key="db_uri", value=hook.get_uri())

    e = time.time()
    es = e - s
    logger.info(f"Total working time : {es:.4f} sec")
