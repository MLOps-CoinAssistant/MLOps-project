import asyncio
import uvloop
import aiohttp
import time
import jwt
import uuid
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from airflow.providers.postgres.hooks.postgres import PostgresHook
from info.connections import Connections
from info.api import APIInformation
from sqlalchemy import (
    create_engine,
    Column,
    DateTime,
    Integer,
    MetaData,
    func,
    Index,
    text,
    delete,
)
from sqlalchemy.orm import Session, declarative_base, sessionmaker
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.inspection import inspect
from typing import Optional, Tuple, Dict, List
import pandas as pd
import numpy as np
import requests
import logging
import os
import functools

# SQLAlchemy의 기본 클래스를 선언합니다.
Base = declarative_base()


# 데이터베이스 테이블 모델을 정의합니다.
class BtcOhlcv(Base):
    __tablename__ = "btc_ohlcv"
    time = Column(DateTime, primary_key=True)
    open = Column(Integer)
    high = Column(Integer)
    low = Column(Integer)
    close = Column(Integer)
    volume = Column(Integer)


# PostgreSQL 연결 정보를 가져옵니다.
postgres_hook = PostgresHook(postgres_conn_id=Connections.POSTGRES_DEFAULT.value)
engine = create_engine(postgres_hook.get_uri())
session_local = sessionmaker(bind=engine)

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


# 동기적으로 Upbit API를 사용하여 비트코인 시세 데이터를 가져오는 함수를 정의합니다.
def fetch_ohlcv_data(market: str, to: str, count: int) -> Optional[List[Dict]]:
    url = f"https://api.upbit.com/v1/candles/minutes/60?market={market}&to={to}&count={count}"
    headers = {
        "Accept": "application/json",
        "Authorization": generate_jwt_token(
            APIInformation.UPBIT_ACCESS_KEY.value, APIInformation.UPBIT_SECRET_KEY.value
        ),
    }
    backoff_time = 0.5
    max_backoff_time = 32  # 최대 대기 시간 설정 (64초).
    max_retries = 5
    retry_count = 0
    while retry_count < max_retries:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            min_req, sec_req = check_remaining_requests(response.headers)
            if min_req is not None and sec_req is not None and sec_req < 5:
                time.sleep(0.5)
            return data
        elif response.status_code == 429:
            time.sleep(backoff_time)
            backoff_time = min(backoff_time * 2, max_backoff_time)
            retry_count += 1
        else:
            return None
    return None


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


# 동기적으로 1년치 데이터를 여러 번 요청하여 가져오는 함수
def fetch_data(
    start_date: datetime, end_date: datetime, existing_times: set
) -> List[Dict]:
    market = "KRW-BTC"
    data = []
    max_workers = 3
    batch_size = 3
    current_date = start_date
    while current_date < end_date:
        futures = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for _ in range(batch_size):
                if current_date >= end_date:
                    break
                if current_date in existing_times:
                    current_date += timedelta(minutes=60 * 200)
                    continue

                to_date = min(
                    current_date + timedelta(minutes=60 * 199), end_date
                ).strftime("%Y-%m-%dT%H:%M:%S")
                future = executor.submit(fetch_ohlcv_data, market, to_date, 200)
                futures.append((current_date, future))
                current_date += timedelta(minutes=60 * 200)
                time.sleep(0.2)

            for date, future in sorted(futures, key=lambda x: x[0]):
                try:
                    result = future.result()
                    if result:
                        data.extend(result)
                except Exception as e:
                    logger.error(f"Exception occurred while fetching data: {e}")

    return data


def check_and_remove_duplicates(session: Session) -> None:
    """
    중복이 있는지 체크하고, 있다면 중복을 제거합니다.
    """
    duplicate_check_query = """
        SELECT time
        FROM btc_ohlcv
        GROUP BY time
        HAVING COUNT(time) > 1
    """
    duplicates = session.execute(duplicate_check_query).fetchall()
    if duplicates:
        ranked_ohlc = select(
            BtcOhlcv.time,
            BtcOhlcv.ctid,
            func.row_number()
            .over(partition_by=BtcOhlcv.time, order_by=BtcOhlcv.time)
            .label("row_num"),
        ).subquery()

        delete_stmt = delete(BtcOhlcv).where(
            BtcOhlcv.ctid.in_(
                select([ranked_ohlc.c.ctid]).where(ranked_ohlc.c.row_num > 1)
            )
        )

        session.execute(delete_stmt)
        session.commit()
        remaining_duplicates = session.execute(duplicate_check_query).fetchall()
        if remaining_duplicates:
            logger.warning(
                f"Remaining duplicates after removal: {remaining_duplicates}"
            )
        else:
            logger.info("No duplicates found after removal")
    else:
        logger.info("No duplicates found")


def check_and_interpolate_missing_values(
    session: Session, start_date: datetime, end_date: datetime
) -> None:
    """
    결측치를 확인 후 존재하는 데이터에 대해 linear 보간법을 적용하여 삽입합니다.
    """
    missing_times_query = """
        WITH all_times AS (
            SELECT generate_series(
                :start_time,
                :end_time,
                interval '1 hour'
            ) AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Seoul' AS time
        )
        SELECT time FROM all_times
        WHERE date_trunc('hour', time) NOT IN (
            SELECT date_trunc('hour', time)
            FROM btc_ohlcv
        )
    """
    missing_times = session.execute(
        missing_times_query, {"start_time": start_date, "end_time": end_date}
    ).fetchall()
    missing_times = [mt[0] for mt in missing_times]

    if missing_times:
        existing_data = session.query(BtcOhlcv).order_by(BtcOhlcv.time).all()
        df_existing = pd.DataFrame(
            [(d.time, d.open, d.high, d.low, d.close, d.volume) for d in existing_data],
            columns=["time", "open", "high", "low", "close", "volume"],
        )
        df_existing.reset_index(drop=True, inplace=True)
        df_existing["time"] = pd.to_datetime(df_existing["time"])

        missing_times_corrected = [
            mt.replace(minute=0, second=0, microsecond=0) for mt in missing_times
        ]
        df_missing = pd.DataFrame(missing_times_corrected, columns=["time"])
        df_missing["open"] = np.nan
        df_missing["high"] = np.nan
        df_missing["low"] = np.nan
        df_missing["close"] = np.nan
        df_missing["volume"] = np.nan

        df_combined = pd.concat([df_existing, df_missing])
        df_combined.sort_values("time", inplace=True)
        df_combined.set_index("time", inplace=True)
        df_combined.interpolate(method="linear", inplace=True)

        df_combined["open"] = pd.to_numeric(
            df_combined["open"].round(0), downcast="integer"
        )
        df_combined["high"] = pd.to_numeric(
            df_combined["high"].round(0), downcast="integer"
        )
        df_combined["low"] = pd.to_numeric(
            df_combined["low"].round(0), downcast="integer"
        )
        df_combined["close"] = pd.to_numeric(
            df_combined["close"].round(0), downcast="integer"
        )
        df_combined["volume"] = pd.to_numeric(
            df_combined["volume"].round(0), downcast="integer"
        )

        df_combined.reset_index(inplace=True)

        interpolated_df = df_combined.loc[
            df_combined["time"].isin(missing_times_corrected)
        ]
        for index, row in interpolated_df.iterrows():
            stmt = pg_insert(BtcOhlcv).values(
                time=row["time"],
                open=int(row["open"]),
                high=int(row["high"]),
                low=int(row["low"]),
                close=int(row["close"]),
                volume=int(row["volume"]),
            )
            upsert_stmt = stmt.on_conflict_do_update(
                index_elements=["time"],
                set_={
                    "open": stmt.excluded.open,
                    "high": stmt.excluded.high,
                    "low": stmt.excluded.low,
                    "close": stmt.excluded.close,
                    "volume": stmt.excluded.volume,
                },
            )
            session.execute(upsert_stmt)
        session.commit()
        logger.info(
            f"{len(interpolated_df)} interpolated records inserted successfully."
        )
    else:
        logger.info("No missing values found")


def check_and_sort_data(session: Session) -> None:
    """
    데이터를 정렬하는 함수. 결측치 보간 후 정렬을 위해 사용.
    """
    sort_check_query = """
        SELECT time
        FROM btc_ohlcv
    """
    result = session.execute(sort_check_query).fetchall()

    if result:
        first = result[:10]
        last = result[-10:]
        logger.info(f"First 10 times: {first}")
        logger.info(f"Last 10 times: {last}")

    is_sorted = result == sorted(result)

    if not is_sorted:
        logger.info("Sorting required. Proceeding with sort.")
        sort_query = text(
            """
            CREATE TEMP TABLE sorted_btc_ohlcv AS
            SELECT * FROM btc_ohlcv
            ORDER BY time;

            DELETE FROM btc_ohlcv;

            INSERT INTO btc_ohlcv
            SELECT * FROM sorted_btc_ohlcv;

            DROP TABLE sorted_btc_ohlcv;
        """
        )
        session.execute(sort_query)
        session.commit()

        result_after_sort = session.execute(sort_check_query).fetchall()
        if result_after_sort:
            first_10_after_sort = result_after_sort[:10]
            last_10_after_sort = result_after_sort[-10:]
            logger.info(f"First 10 times after sort: {first_10_after_sort}")
            logger.info(f"Last 10 times after sort: {last_10_after_sort}")
        else:
            logger.info("No data found after sorting")

        logger.info("Data sorted")
    else:
        logger.info("Data already sorted")


def process_data(
    data: List[Dict],
    start_date: datetime,
    end_date: datetime,
    session: Session,
    initial_load: bool,
) -> None:
    records = []
    for item in data:
        try:
            time_value = datetime.strptime(
                item["candle_date_time_kst"], "%Y-%m-%dT%H:%M:%S"
            )
            new_record = BtcOhlcv(
                time=time_value,
                open=item["opening_price"],
                high=item["high_price"],
                low=item["low_price"],
                close=item["trade_price"],
                volume=item["candle_acc_trade_volume"],
            )
            records.append(new_record)
        except Exception as e:
            logger.error(f"Error preparing record: {e}, data: {item}")

    if records:
        try:
            if initial_load:
                session.bulk_insert_mappings(
                    BtcOhlcv, [record.__dict__ for record in records]
                )
                session.commit()
                check_and_interpolate_missing_values(session, start_date, end_date)
                check_and_sort_data(session)
                result = session.query(BtcOhlcv.time).order_by(BtcOhlcv.time).all()
                if result:
                    first = result[:10]
                    last = result[-10:]
                    logger.info(f"First 10 times: {first}")
                    logger.info(f"Last 10 times: {last}")
                else:
                    logger.info("No data found in the table.")
            else:
                batch_size = 10
                for i in range(0, len(records), batch_size):
                    batch_records = records[i : i + batch_size]
                    retries = 0
                    max_retries = 3
                    while retries < max_retries:
                        try:
                            for record in batch_records:
                                stmt = pg_insert(BtcOhlcv).values(
                                    time=record.time,
                                    open=record.open,
                                    high=record.high,
                                    low=record.low,
                                    close=record.close,
                                    volume=record.volume,
                                )
                                upsert_stmt = stmt.on_conflict_do_update(
                                    index_elements=["time"],
                                    set_={
                                        "open": stmt.excluded.open,
                                        "high": stmt.excluded.high,
                                        "low": stmt.excluded.low,
                                        "close": stmt.excluded.close,
                                        "volume": stmt.excluded.volume,
                                    },
                                )
                                session.execute(upsert_stmt)
                            session.commit()
                            break
                        except OperationalError as e:
                            logger.error(f"OperationalError occurred: {e}")
                            session.rollback()
                            retries += 1
                            if retries < max_retries:
                                wait_time = 2**retries
                                time.sleep(wait_time)
                            else:
                                raise
                        except Exception as e:
                            logger.error(f"An error occurred while inserting data: {e}")
                            session.rollback()
                            raise
        except OperationalError as e:
            logger.error(f"OperationalError occurred: {e}")
            session.rollback()
        except Exception as e:
            logger.error(f"An error occurred while inserting data: {e}")
            session.rollback()
        finally:
            session.close()


async def fetch_upbit_data_fill_gaps(session, last_recorded_time):
    """
    데이터베이스에 기록된 마지막 시간 이후의 데이터를 Upbit API로부터 가져오는 함수

    Args:
        session (aiohttp.ClientSession): aiohttp 클라이언트 세션
        last_recorded_time (datetime): 데이터베이스에 기록된 마지막 시간

    Returns:
        list: 가져온 데이터 목록
    """
    end_date = datetime.utcnow() + timedelta(hours=9)
    all_data = []

    logger.info("last_recorded_time: {}".format(last_recorded_time))
    logger.info("end_date (KST): {}".format(end_date))

    while last_recorded_time < end_date:
        to_date_str = last_recorded_time.strftime("%Y-%m-%dT%H:%M:%S")
        data = await fetch_ohlcv_data(session, "KRW-BTC", to_date_str, 24)
        if not data:
            logger.warning(f"No data available for date: {to_date_str}")
            break
        all_data.extend(data)
        last_recorded_time += timedelta(hours=24)
        await asyncio.sleep(0.5)

    all_data.sort(key=lambda x: x["candle_date_time_kst"])
    return all_data


async def delete_old_data():
    """
    현재 시간(UTC+9)으로부터 365일이 지난 데이터를 데이터베이스에서 삭제하는 함수
    """
    postgres_hook = PostgresHook(postgres_conn_id=Connections.POSTGRES_DEFAULT.value)
    engine = create_engine(postgres_hook.get_uri())
    SessionLocal = sessionmaker(bind=engine)

    session = SessionLocal()
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


async def insert_data_into_db(fetch_data):
    """
    가져온 데이터를 데이터베이스에 삽입하는 함수

    Args:
        fetch_data (list): 가져온 데이터 목록
    """
    postgres_hook = PostgresHook(postgres_conn_id=Connections.POSTGRES_DEFAULT.value)
    engine = create_engine(postgres_hook.get_uri())
    SessionLocal = sessionmaker(bind=engine)

    def is_continuous_timestamps(data):
        """
        데이터의 타임스탬프가 1시간 간격으로 연속적인지 확인하는 함수

        Args:
            data (list): 데이터 목록

        Returns:
            tuple: (연속적인지 여부, 마지막 연속된 타임스탬프, 연속되지 않은 첫 타임스탬프)
        """
        timestamps = [
            datetime.strptime(entry["candle_date_time_kst"], "%Y-%m-%dT%H:%M:%S")
            for entry in data
        ]
        for i in range(1, len(timestamps)):
            if (timestamps[i] - timestamps[i - 1]) != timedelta(hours=1):
                return False, timestamps[i - 1], timestamps[i]
        return True, None, None

    async def fetch_missing_data(start_time, end_time):
        """
        누락된 데이터를 Upbit API로부터 가져오는 함수

        Args:
            start_time (datetime): 시작 시간
            end_time (datetime): 종료 시간

        Returns:
            list: 누락된 데이터 목록
        """
        missing_data = []
        async with aiohttp.ClientSession() as session:
            while start_time < end_time:
                to_date_str = start_time.strftime("%Y-%m-%dT%H:%M:%S")
                data = await fetch_ohlcv_data(session, "KRW-BTC", to_date_str, 1)
                if not data:
                    logger.warning(
                        f"No data available for date: {to_date_str}. Filling with default values."
                    )
                    missing_data.append(
                        {
                            "candle_date_time_kst": to_date_str,
                            "opening_price": 0,
                            "trade_price": 0,
                            "high_price": 0,
                            "low_price": 0,
                            "candle_acc_trade_volume": 0,
                        }
                    )
                else:
                    missing_data.extend(data)
                start_time += timedelta(hours=1)
                await asyncio.sleep(0.5)
        return missing_data

    def fill_missing_timestamps(data):
        """
        누락된 타임스탬프를 기본 값으로 채우는 함수

        Args:
            data (list): 데이터 목록

        Returns:
            list: 누락된 타임스탬프가 채워진 데이터 목록
        """
        filled_data = []
        timestamps = [
            datetime.strptime(entry["candle_date_time_kst"], "%Y-%m-%dT%H:%M:%S")
            for entry in data
        ]

        for i in range(1, len(timestamps)):
            current_time = timestamps[i - 1]
            next_time = timestamps[i]

            filled_data.append(data[i - 1])
            while (next_time - current_time) > timedelta(hours=1):
                current_time += timedelta(hours=1)
                filled_data.append(
                    {
                        "candle_date_time_kst": current_time.strftime(
                            "%Y-%m-%dT%H:%M:%S"
                        ),
                        "opening_price": 0,
                        "trade_price": 0,
                        "high_price": 0,
                        "low_price": 0,
                        "candle_acc_trade_volume": 0,
                    }
                )

        filled_data.append(data[-1])
        return filled_data

    continuous, last_time, missing_start_time = is_continuous_timestamps(fetch_data)
    if not continuous:
        logger.warning(
            "Data timestamps are not continuous in 1-hour intervals. Fetching missing data."
        )
        missing_data = await fetch_missing_data(last_time, missing_start_time)
        fetch_data.extend(missing_data)
        fetch_data.sort(key=lambda x: x["candle_date_time_kst"])
        fetch_data = fill_missing_timestamps(fetch_data)
        continuous, _, _ = is_continuous_timestamps(fetch_data)
        if not continuous:
            logger.info(
                "Even after fetching, data timestamps are not continuous in 1-hour intervals."
            )

    fetch_data.sort(key=lambda x: x["candle_date_time_kst"])

    session = SessionLocal()
    try:
        for entry in fetch_data:
            timestamp = datetime.strptime(
                entry["candle_date_time_kst"], "%Y-%m-%dT%H:%M:%S"
            )
            open_price = entry["opening_price"]
            close_price = entry["trade_price"]
            high_price = entry["high_price"]
            low_price = entry["low_price"]
            volume = entry["candle_acc_trade_volume"]

            existing_record = (
                session.query(BtcOhlcv).filter(BtcOhlcv.time == timestamp).first()
            )
            if existing_record:
                logger.info(
                    f"Duplicate entry found for timestamp {timestamp}. Updating existing record."
                )
                existing_record.open = open_price
                existing_record.high = high_price
                existing_record.low = low_price
                existing_record.close = close_price
                existing_record.volume = volume
            else:
                new_record = BtcOhlcv(
                    time=timestamp,
                    open=open_price,
                    high=high_price,
                    low=low_price,
                    close=close_price,
                    volume=volume,
                )
                session.add(new_record)
                logger.info(
                    f"Inserted new record for timestamp {timestamp}: open={open_price}, high={high_price}, low={low_price}, close={close_price}, volume={volume}"
                )

        session.commit()

        # 데이터 정렬
        sort_query = text(
            """
            CREATE TEMP TABLE sorted_btc_ohlcv AS
            SELECT * FROM btc_ohlcv
            ORDER BY time;

            DELETE FROM btc_ohlcv;

            INSERT INTO btc_ohlcv
            SELECT * FROM sorted_btc_ohlcv;

            DROP TABLE sorted_btc_ohlcv;
        """
        )
        session.execute(sort_query)
        session.commit()

        logger.info("Data sorted after insertion.")
    except Exception as e:
        session.rollback()
        logger.error(f"Failed to insert data into db: {e}")
    finally:
        session.close()


def get_most_recent_data_time():
    """
    데이터베이스에 기록된 가장 최근의 시간 데이터를 가져오는 함수

    Returns:
        datetime: 가장 최근의 시간 데이터
    """
    postgres_hook = PostgresHook(postgres_conn_id=Connections.POSTGRES_DEFAULT.value)
    engine = create_engine(postgres_hook.get_uri())
    session = Session(bind=engine)

    most_recent_time = (
        session.query(BtcOhlcv.time).order_by(BtcOhlcv.time.desc()).first()
    )
    session.close()
    if most_recent_time:
        logger.info("most_recent_time: {}".format(most_recent_time[0]))
        return most_recent_time[0]
    else:
        return None


def collect_and_load_data_sync():
    asyncio.run(collect_and_load_data())


async def run_upbit_api_call_event_loop_policy():
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    await collect_and_load_data()


def run_upbit_api_call_event_loop_policy_sync():
    uvloop.install()
    collect_and_load_data_sync()


def create_table_if_not_exists():
    """
    데이터베이스에 필요한 테이블이 존재하지 않으면 생성하는 함수
    """
    postgres_hook = PostgresHook(postgres_conn_id=Connections.POSTGRES_DEFAULT.value)
    engine = create_engine(postgres_hook.get_uri())
    Base.metadata.create_all(engine)
    logger.info("Checked and created tables if not existing.")


async def collect_and_load_data():
    """
    데이터를 수집하고 데이터베이스에 적재하며, 365일이 지난 데이터를 삭제하는 함수
    """
    create_table_if_not_exists()

    async with aiohttp.ClientSession() as session:
        most_recent_time = get_most_recent_data_time()
        if most_recent_time:
            data = await fetch_upbit_data_fill_gaps(session, most_recent_time)
        else:
            start_date = datetime.now() - timedelta(days=365)
            end_date = datetime.now()
            num_cores = os.cpu_count() - 1

            date_ranges = [
                (
                    start_date + timedelta(days=i * 365 // num_cores),
                    start_date + timedelta(days=(i + 1) * 365 // num_cores),
                )
                for i in range(num_cores)
            ]

            loop = asyncio.get_event_loop()
            with ThreadPoolExecutor(max_workers=num_cores) as executor:
                tasks = [
                    loop.run_in_executor(
                        executor,
                        functools.partial(
                            fetch_data_for_period_sync,
                            date_range[0].strftime("%Y-%m-%dT%H:%M:%S"),
                            date_range[1].strftime("%Y-%m-%dT%H:%M:%S"),
                        ),
                    )
                    for date_range in date_ranges
                ]
                results = await asyncio.gather(*tasks)
                data = [item for sublist in results for item in sublist]

        await insert_data_into_db(data)
        await delete_old_data()


def collect_and_load_data_sync():
    asyncio.run(collect_and_load_data())


def run_upbit_api_call_event_loop_policy_sync():
    uvloop.install()
    asyncio.run(collect_and_load_data_sync())
    import xgboost as xgb
    import catboost as cb

    logger.info("xgb version: {}".format(xgb.__version__))
    logger.info("catboost version: {}".format(cb.__version__))
