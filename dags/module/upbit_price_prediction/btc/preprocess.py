import pandas as pd
import numpy as np
import click
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import create_engine, Column, DateTime, Integer, text
from sqlalchemy.orm import declarative_base, sessionmaker
from datetime import datetime
import logging
from info.connections import Connections

# 로깅 설정
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


class BtcPreprocessed(Base):
    __tablename__ = "btc_preprocessed"
    time = Column(DateTime, primary_key=True)
    open = Column(Integer)
    high = Column(Integer)
    low = Column(Integer)
    close = Column(Integer)
    volume = Column(Integer)
    label = Column(Integer)


@click.command()
def preprocess_data_fn(**context):
    hook = PostgresHook(postgres_conn_id=Connections.POSTGRES_DEFAULT.value)
    engine = create_engine(hook.get_uri())
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        connection = hook.get_conn()
        with connection.cursor() as cursor:
            logger.info("Creating btc_preprocessed table if not exists")
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS btc_preprocessed (
                    time TIMESTAMP PRIMARY KEY,
                    open INTEGER,
                    high INTEGER,
                    low INTEGER,
                    close INTEGER,
                    volume INTEGER,
                    label INTEGER
                );
            """
            )

            logger.info("Finding missing times from btc_ohlcv not in btc_preprocessed")
            cursor.execute(
                """
                SELECT MAX(time) FROM btc_ohlcv;
            """
            )
            max_time_ohlcv = cursor.fetchone()[0]
            logger.info(f"Max time in btc_ohlcv: {max_time_ohlcv}")

            cursor.execute(
                """
                SELECT MAX(time) FROM btc_preprocessed;
            """
            )
            max_time_preprocessed = cursor.fetchone()[0]
            logger.info(f"Max time in btc_preprocessed: {max_time_preprocessed}")

            if not max_time_preprocessed:
                max_time_preprocessed = datetime.min

            if max_time_ohlcv > max_time_preprocessed:
                logger.info(
                    f"Inserting missing times from {max_time_preprocessed} to {max_time_ohlcv}"
                )
                cursor.execute(
                    """
                    INSERT INTO btc_preprocessed (time, open, high, low, close, volume)
                    SELECT time, open, high, low, close, volume
                    FROM btc_ohlcv
                    WHERE time > %s
                    ORDER BY time;
                """,
                    (max_time_preprocessed,),
                )

                logger.info(
                    f"Inserted missing times from btc_ohlcv to btc_preprocessed"
                )

                logger.info(f"Updating labels for new entries in btc_preprocessed")
                cursor.execute(
                    """
                    WITH CTE AS (
                        SELECT time, close,
                               LAG(close) OVER (ORDER BY time) AS prev_close
                        FROM btc_preprocessed
                    )
                    UPDATE btc_preprocessed
                    SET label = CASE
                                    WHEN CTE.close > CTE.prev_close THEN 1
                                    ELSE 0
                                END
                    FROM CTE
                    WHERE btc_preprocessed.time = CTE.time
                    AND btc_preprocessed.time > %s;
                """,
                    (max_time_preprocessed,),
                )

                logger.info(f"Labels updated successfully for new entries")

                logger.info(
                    f"Sorting btc_preprocessed table by time in ascending order"
                )
                cursor.execute(
                    """
                    CREATE TABLE temp_btc_preprocessed AS
                    SELECT * FROM btc_preprocessed
                    ORDER BY time ASC;

                    DROP TABLE btc_preprocessed;

                    ALTER TABLE temp_btc_preprocessed RENAME TO btc_preprocessed;
                """
                )
                logger.info(f"btc_preprocessed table sorted successfully")

            connection.commit()

        logger.info("Label column added and updated successfully for missing times.")
    except Exception as e:
        session.rollback()
        logger.error(f"Data preprocessing failed: {e}")
        raise
    finally:
        session.close()

    context["ti"].xcom_push(key="preprocessed", value=True)
