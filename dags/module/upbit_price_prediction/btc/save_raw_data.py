import asyncio
import uvloop
import aiohttp
import time
import jwt
import uuid
from datetime import datetime, timedelta
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import create_engine, Column, DateTime, Integer, func, text
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.dialects.postgresql import insert as pg_insert
from typing import Optional, Tuple, Dict, List
import logging
import os
from info.api import APIInformation
from dags.module.upbit_price_prediction.btc.create_table import BtcOhlcv, Base

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# JWT 생성 함수
def generate_jwt_token(access_key: str, secret_key: str) -> str:
    payload = {"access_key": access_key, "nonce": str(uuid.uuid4())}
    jwt_token = jwt.encode(payload, secret_key, algorithm="HS256")
    authorization_token = f"Bearer {jwt_token}"
    return authorization_token


# 남아있는 요청 수를 확인하고 대기 시간을 조절하는 함수.
def check_remaining_requests(
    headers: Dict[str, str]
) -> Tuple[Optional[int], Optional[int]]:
    remaining_req = headers.get("Remaining-Req")
    if remaining_req:
        _, min_req, sec_req = remaining_req.split("; ")
        min_req = int(min_req.split("=")[1])
        sec_req = int(sec_req.split("=")[1])
        return min_req, sec_req
    return None, None


# 비동기적으로 Upbit API를 사용하여 비트코인 시세 데이터를 가져오는 함수
async def fetch_ohlcv_data(session, market: str, to: str, count: int, retry=3):
    """
    Upbit API를 호출하여 OHLCV 데이터를 가져오는 함수

    Args:
        session (aiohttp.ClientSession): aiohttp 클라이언트 세션
        market (str): 시장 정보 (예: "KRW-BTC")
        to (str): 종료 시간 (ISO 8601 형식)
        count (int): 데이터 포인트 수
        retry (int): 재시도 횟수 (기본값: 3)

    Returns:
        list: OHLCV 데이터 목록
    """
    url = f"https://api.upbit.com/v1/candles/minutes/60?market={market}&to={to}&count={count}"
    headers = {
        "Accept": "application/json",
        "Authorization": generate_jwt_token(
            APIInformation.UPBIT_ACCESS_KEY.value, APIInformation.UPBIT_SECRET_KEY.value
        ),
    }
    backoff = 1
    for attempt in range(retry):
        try:
            async with session.get(url, headers=headers) as response:
                response.raise_for_status()
                data = await response.json()
                if isinstance(data, list):
                    return data
                else:
                    logger.error(f"Unexpected response format: {data}")
        except aiohttp.ClientError as e:
            if response.status == 429:
                logger.warning(
                    f"API request failed: {e}, retrying in {backoff} seconds..."
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)  # Exponential backoff
            else:
                logger.error(f"API request failed: {e}")
                break
    return []


# 데이터베이스에 데이터를 삽입하는 함수
async def insert_data_into_db(data: list, db_uri: str) -> None:
    engine = create_engine(db_uri)
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        for record in data:
            logger.debug(f"Inserting record: {record}")  # 디버그용 로그 추가
            stmt = (
                pg_insert(BtcOhlcv)
                .values(
                    time=datetime.fromisoformat(record["candle_date_time_kst"]),
                    open=record["opening_price"],
                    high=record["high_price"],
                    low=record["low_price"],
                    close=record["trade_price"],
                    volume=record["candle_acc_trade_volume"],
                )
                .on_conflict_do_update(
                    index_elements=["time"],
                    set_={
                        "open": record["opening_price"],
                        "high": record["high_price"],
                        "low": record["low_price"],
                        "close": record["trade_price"],
                        "volume": record["candle_acc_trade_volume"],
                    },
                )
            )
            session.execute(stmt)
        session.commit()
        logger.info("Data inserted successfully")
    except Exception as e:
        session.rollback()
        logger.error(f"Failed to insert data into database: {e}")
        raise e
    finally:
        session.close()


# 현재 시간(UTC+9)으로부터 365일이 지난 데이터를 데이터베이스에서 삭제하는 함수
async def delete_old_data(db_uri: str):
    engine = create_engine(db_uri)
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        threshold_date = datetime.utcnow() + timedelta(hours=9) - timedelta(days=365)
        deleted_rows = (
            session.query(BtcOhlcv).filter(BtcOhlcv.time < threshold_date).delete()
        )
        session.commit()
        logger.info(f"Deleted {deleted_rows} old records from the database.")
    except Exception as e:
        session.rollback()
        logger.error(f"Failed to delete old data: {e}")
    finally:
        session.close()


# 데이터베이스에 기록된 가장 최근의 시간 데이터를 가져오는 함수
def get_most_recent_data_time(db_uri: str) -> datetime:
    engine = create_engine(db_uri)
    Session = sessionmaker(bind=engine)
    session = Session()

    most_recent_time = (
        session.query(BtcOhlcv.time).order_by(BtcOhlcv.time.desc()).first()
    )
    session.close()
    if most_recent_time:
        logger.info("most_recent_time: {}".format(most_recent_time[0]))
        return most_recent_time[0]
    else:
        return None


# 데이터를 수집하고 데이터베이스에 적재하며, 365일이 지난 데이터를 삭제하는 함수
async def collect_and_load_data(db_uri: str, context: dict) -> None:
    async with aiohttp.ClientSession() as session:
        most_recent_time = get_most_recent_data_time(db_uri)
        current_time = datetime.now() + timedelta(hours=9)

        if most_recent_time:
            logger.info(
                f"most_recent_time: {most_recent_time}, current_time: {current_time}"
            )
        else:
            logger.info(
                f"No recent data found, setting most_recent_time to one year ago from current_time"
            )
            most_recent_time = current_time - timedelta(days=365)

        time_diff = current_time - most_recent_time
        logger.info(f"time_diff: {time_diff}")

        if time_diff < timedelta(hours=1):
            logger.info("Data is already up to date.")
            return

        data = []
        to_time = current_time

        while time_diff > timedelta(0):
            logger.info(f"Fetching data up to {to_time}")
            new_data = await fetch_ohlcv_data(
                session, "KRW-BTC", to_time.strftime("%Y-%m-%dT%H:%M:%S"), 200
            )
            if not new_data:
                break
            data.extend(new_data)
            last_record_time = datetime.fromisoformat(
                new_data[-1]["candle_date_time_kst"]
            )
            to_time = last_record_time - timedelta(minutes=60)
            time_diff = to_time - most_recent_time
            logger.info(f"Collected {len(new_data)} records, time_diff: {time_diff}")

        logger.info(f"Total collected records: {len(data)}")

        await insert_data_into_db(data, db_uri)
        await delete_old_data(db_uri)

        # 새로운 데이터의 시간을 XCom에 푸시
        if data:
            last_record_time = datetime.fromisoformat(data[-1]["candle_date_time_kst"])
            context["ti"].xcom_push(key="new_time", value=last_record_time.isoformat())


# Airflow Task에서 호출될 함수
def save_raw_data_from_API_fn(**context) -> None:
    ti = context["ti"]
    table_created = ti.xcom_pull(key="table_created", task_ids="create_table_fn")
    db_uri = ti.xcom_pull(key="db_uri", task_ids="create_table_fn")

    logger.info(f"table_created: {table_created}, db_uri: {db_uri}")

    if table_created is not None:
        logger.info(
            "Table created or already exists, starting data collection and load process."
        )
        asyncio.run(collect_and_load_data(db_uri, context))
    else:
        logger.info("Table not created and data collection and load process skipped.")
