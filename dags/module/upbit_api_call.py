import asyncio
import uvloop
import aiohttp
from datetime import datetime, timedelta
from sqlalchemy import create_engine, Column, DateTime, Integer, MetaData, Table, Index
from sqlalchemy.orm import Session, declarative_base, sessionmaker
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging
import jwt
import uuid
import time
from dags.module.info.connections import Connections
from module.info.api import APIInformation
import module.info.api as api
from concurrent.futures import ThreadPoolExecutor
import functools
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

metadata = MetaData()

news_table = Table(
    "btc_ohlcv",
    metadata,
    Column("time", DateTime, primary_key=True),
    Column("open", Integer, nullable=True),
    Column("high", Integer, nullable=True),
    Column("low", Integer, nullable=True),
    Column("close", Integer, nullable=True),
    Column("volume", Integer, nullable=True),
)

Base = declarative_base()


class BtcOhlcv(Base):
    __tablename__ = "btc_ohlcv"
    time = Column(DateTime, primary_key=True)
    open = Column(Integer, nullable=True)
    high = Column(Integer, nullable=True)
    low = Column(Integer, nullable=True)
    close = Column(Integer, nullable=True)
    volume = Column(Integer, nullable=True)
    __table_args__ = (Index("idx_btc_ohlcv_time", "time"),)


total_execution_time = 0


def create_table_if_not_exists():
    """
    데이터베이스에 필요한 테이블이 존재하지 않으면 생성하는 함수
    """

    postgres_hook = PostgresHook(postgres_conn_id=Connections.POSTGRES_DEFAULT.value)
    engine = create_engine(postgres_hook.get_uri())
    Base.metadata.create_all(engine)
    # create_table_if_not_exists 설명
    # 데이터베이스에 필요한 테이블이 존재하지 않으면 생성하는 함수
    # Base.metadata.create_all(engine): 데이터베이스에 필요한 테이블이 존재하지 않으면 생성하는 함수
    # Base: declarative_base()로 생성된 객체로서, 데이터베이스 테이블을 정의하는 클래스의 부모 클래스이다.
    # metadata: 데이터베이스 테이블을 정의하는 클래스의 메타데이터를 저장하는 객체
    # metadata.create_all(engine): 데이터베이스에 필요한 테이블이 존재하지 않으면 생성
    # engine: 데이터베이스 연결을 관리하는 객체
    logger.info("Checked and created tables if not existing.")


def generate_jwt_token(access_key: str, secret_key: str) -> str:
    """
    Upbit API 호출을 위한 JWT 토큰을 생성하는 함수

    Args:
        access_key (str): Upbit API의 액세스 키
        secret_key (str): Upbit API의 시크릿 키

    Returns:
        str: 생성된 JWT 토큰
    """
    payload = {"access_key": access_key, "nonce": str(uuid.uuid4())}
    jwt_token = jwt.encode(payload, secret_key, algorithm="HS256")
    authorization_token = f"Bearer {jwt_token}"
    return authorization_token


# generate_jwt_token 설명
# Upbit API 호출을 위한 JWT 토큰을 생성하는 함수
# JWT 토큰이 필요한 이유: Upbit API를 호출할 때, 인증을 위해 JWT 토큰을 사용한다.
# JWT란 JSON Web Token의 약자로, JSON 객체를 URL 안전 문자열로 변환한 토큰이다.
# JWT 토큰은 헤더, 페이로드, 서명 세 부분으로 구성되어 있다.
# 헤더: JWT 토큰의 유형과 해싱 알고리즘을 지정한다.
# 페이로드: JWT 토큰에 포함되는 클레임(claim) 정보를 담고 있다.
# 서명: 헤더와 페이로드를 인코딩한 후, 비밀 키를 이용하여 서명을 생성한다.
# payload: JWT 토큰의 페이로드 부분으로, 액세스 키와 랜덤한 UUID를 포함한다.
# jwt_token: jwt.encode(payload, secret_key, algorithm="HS256") 함수를 이용하여 JWT 토큰을 생성한다.


def time_execution(func):
    """
    함수의 실행 시간을 측정하고 누적 실행 시간을 기록하는 데코레이터 함수

    Args:
        func (function): 실행 시간을 측정할 함수

    Returns:
        function: 데코레이터된 함수
    """

    async def wrapper(*args, **kwargs):
        global total_execution_time
        start_time = time.time()
        result = await func(*args, **kwargs)
        end_time = time.time()
        elapsed_time = end_time - start_time
        total_execution_time += elapsed_time
        return result

    return wrapper


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


# fetch_upbit_data_fill_gaps 설명
# 데이터베이스에 기록된 마지막 시간 이후의 데이터를 Upbit API로부터 가져오는 함수


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


@time_execution
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
    url = api.get_upbit_api_url(market, to, count)
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


async def fetch_data_for_period(start_date_str, end_date_str):
    """
    주어진 기간 동안의 데이터를 Upbit API로부터 가져오는 함수

    Args:
        start_date_str (str): 시작 시간
        end_date_str (str): 종료 시간

    Returns:
        list: 기간 동안의 데이터 목록
    """
    async with aiohttp.ClientSession() as session:
        end_date = datetime.strptime(end_date_str, "%Y-%m-%dT%H:%M:%S")
        start_date = datetime.strptime(start_date_str, "%Y-%m-%dT%H:%M:%S")
        all_data = []

        while start_date < end_date:
            to_date_str = end_date.strftime("%Y-%m-%dT%H:%M:%S")
            data = await fetch_ohlcv_data(session, "KRW-BTC", to_date_str, 24)
            if not data:
                logger.warning(f"No data available for date: {to_date_str}")
                break
            all_data.extend(data)
            end_date -= timedelta(hours=24)
            await asyncio.sleep(0.5)

        all_data.sort(key=lambda x: x["candle_date_time_kst"])
        return all_data


async def fetch_upbit_data_incremental(session):
    """
    최신 데이터를 Upbit API로부터 가져오는 함수

    Args:
        session (aiohttp.ClientSession): aiohttp 클라이언트 세션

    Returns:
        list: 최신 데이터 목록
    """
    end_date = datetime.now()
    to_date_str = end_date.strftime("%Y-%m-%dT%H:%M:%S")
    data = await fetch_ohlcv_data(session, "KRW-BTC", to_date_str, 1)

    if data:
        data.sort(key=lambda x: x["candle_date_time_kst"])

    return data


def check_data_exists():
    """
    데이터베이스에 데이터가 존재하는지 확인하는 함수

    Returns:
        bool: 데이터 존재 여부
    """
    postgres_hook = PostgresHook(postgres_conn_id=Connections.POSTGRES_DEFAULT.value)
    engine = create_engine(postgres_hook.get_uri())
    session = Session(bind=engine)

    exists = session.query(BtcOhlcv).first() is not None
    session.close()
    return exists


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

    # 데이터베이스에 삽입하기 전에 fetch_data를 정렬한다.
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
    except Exception as e:
        session.rollback()
        logger.error(f"Failed to insert data into db: {e}")
    finally:
        session.close()


def fetch_data_for_period_sync(start_date_str, end_date_str):
    """
    동기적으로 주어진 기간 동안의 데이터를 가져오는 함수

    Args:
        start_date_str (str): 시작 시간 (ISO 8601 형식)
        end_date_str (str): 종료 시간 (ISO 8601 형식)

    Returns:
        list: 기간 동안의 데이터 목록
    """
    return asyncio.run(fetch_data_for_period(start_date_str, end_date_str))


async def collect_and_load_data():
    """
    데이터를 수집하고 데이터베이스에 적재하며, 365일이 지난 데이터를 삭제하는 함수
    """
    global total_execution_time
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

    logger.info(
        f"Total execution time for fetch_ohlcv_data: {total_execution_time:.2f} seconds"
    )


async def run_upbit_api_call_event_loop_policy():
    """
    uvloop 이벤트 루프 정책을 사용하여 collect_and_load_data 함수를 실행하는 함수
    """
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    await collect_and_load_data()


def run_upbit_api_call_event_loop_policy_sync():
    """
    동기적으로 run_upbit_api_call_event_loop_policy 함수를 실행하는 함수
    """
    asyncio.run(run_upbit_api_call_event_loop_policy())
