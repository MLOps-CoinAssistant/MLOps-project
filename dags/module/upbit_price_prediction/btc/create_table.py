from airflow.providers.postgres.hooks.postgres import PostgresHook
from info.connections import Connections
from sqlalchemy import create_engine, Column, DateTime, Integer, inspect
import logging
from sqlalchemy.orm import declarative_base
from airflow.exceptions import AirflowSkipException
import click

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


@click.command()
def create_table_fn(
    hook: PostgresHook = PostgresHook(
        postgres_conn_id=Connections.POSTGRES_DEFAULT.value
    ),
    **context: dict,
) -> None:
    """
    데이터베이스에 필요한 테이블이 존재하지 않으면 생성
    """

    engine = create_engine(hook.get_uri())
    inspector = inspect(engine)

    if inspector.has_table("btc_ohlcv"):
        logger.info("Table 'btc_ohlcv' already exists.")

        context["ti"].xcom_push(key="table_created", value=False)
        context["ti"].xcom_push(key="db_uri", value=hook.get_uri())
        raise AirflowSkipException("Table 'btc_ohlcv' already exists.")
    else:
        Base.metadata.create_all(engine)
        logger.info("Checked and created tables if not existing.")
        context["ti"].xcom_push(key="table_created", value=True)
        context["ti"].xcom_push(key="db_uri", value=hook.get_uri())
