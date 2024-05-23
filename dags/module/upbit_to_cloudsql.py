from concurrent.futures import ThreadPoolExecutor
from airflow.providers.postgres.hooks.postgres import PostgresHook
from dags.module.info.connections import Connections
from dags.module.info.api import APIInformation
from sqlalchemy import (
    create_engine,
    Column,
    DateTime,
    Float,
    MetaData,
    func,
    inspect,
    Index,
    text,
)
from sqlalchemy.orm import Session, declarative_base
from sqlalchemy.exc import IntegrityError
from sqlalchemy.dialects.postgresql import insert as pg_insert
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple, Dict, List
import numpy as np
import pandas as pd
from scipy.interpolate import interp1d
from scipy.ndimage import gaussian_filter1d

import requests
import time
import jwt
import uuid

# SQLAlchemy의 기본 클래스를 선언합니다.
Base = declarative_base()


# 데이터베이스 테이블 모델을 정의합니다.
class BtcOhlc(Base):
    __tablename__ = "btc_ohlc"
    time = Column(DateTime, primary_key=True)
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    volume = Column(Float)
    __table_args__ = (Index("idx_btc_ohlc_time", "time"),)


# PostgreSQL 연결 정보를 환경 변수에서 가져옵니다.
postgres_hook = PostgresHook(postgres_conn_id=Connections.POSTGRES_DEFAULT.value)
upbit_access_key = APIInformation.UPBIT_ACCESS_KEY.value
upbit_secret_key = APIInformation.UPBIT_ACCESS_KEY.value
if not postgres_hook:
    raise ValueError(
        "Postgres connection string is not set in the environment variables."
    )
if not upbit_access_key or not upbit_secret_key:
    raise ValueError("Upbit API keys are not set in the environment variables.")

# 데이터베이스 연결 엔진을 생성합니다.
engine = create_engine(postgres_hook.get_uri())

session = Session(bind=engine)


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
        group, min_req, sec_req = remaining_req.split("; ")
        min_req = int(min_req.split("=")[1])
        sec_req = int(sec_req.split("=")[1])
        return min_req, sec_req
    return None, None


# 동기적으로 Upbit API를 사용하여 비트코인 시세 데이터를 가져오는 함수를 정의합니다.
def fetch_ohlcv_data(market: str, to: str, count: int) -> Optional[List[Dict]]:
    url = f"https://api.upbit.com/v1/candles/minutes/60?market={market}&to={to}&count={count}"
    headers = {
        "Accept": "application/json",
        "Authorization": generate_jwt_token(upbit_access_key, upbit_secret_key),
    }
    print(f"Calling API with URL: {url}")  # API 호출 URL 로그 출력
    backoff_time = 1
    max_backoff_time = 64  # 최대 대기 시간 설정 (64초). Ecponential Backoff방식 : 429에러 발생시 처음1초 대기 후 재시도하고, 이후에는 대기시간을 2배씩 늘려가면서 최대 64초까지 대기시킴.
    max_retries = 5
    retry_count = 0
    while retry_count < max_retries:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            min_req, sec_req = check_remaining_requests(response.headers)
            if min_req is not None and sec_req is not None and sec_req < 5:
                print(f"Rate limit close to being exceeded. Waiting for 1 second...")
                time.sleep(1)
            return data
        elif response.status_code == 429:
            print(
                f"Rate limit exceeded. Waiting for {backoff_time} seconds before retrying..."
            )
            time.sleep(backoff_time)
            backoff_time = min(
                backoff_time * 2, max_backoff_time
            )  # Exponential backoff
            retry_count += 1
        else:
            print(f"Error fetching data: {response.status_code}, {response.text}")
            return None
    print(f"Failed to fetch data after {max_retries} retries.")
    return None


# 동기적으로 1년치 데이터를 여러 번 요청하여 가져오는 함수
def fetch_data(
    start_date: datetime, end_date: datetime, existing_times: set
) -> List[Dict]:
    market = "KRW-BTC"
    data = []
    max_workers = 3  # 적절한 스레드 수 설정 . 일반적으로 I/O 바운드 작업에서는 5~10개의 스레드가 적절한 성능을 제공한다고 함. 실험을 통해 적절히 조절가능
    batch_size = 3  # 초당 5회 제한때문에 설정했는데 속도를 올리기위해 추후 조정
    print(
        f"Fetching data from {start_date} to {end_date}"
    )  # 데이터 가져오는 범위 로그 추가
    current_date = start_date
    while current_date < end_date:
        futures = []
        with ThreadPoolExecutor(
            max_workers=max_workers
        ) as executor:  # max_workers 로 설정된 고정된 스레풀을 사용할 수 있도록 ThreadPoolExecutor 사용(과부하방지)
            for _ in range(batch_size):
                if current_date >= end_date:
                    break
                if current_date in existing_times:
                    current_date += timedelta(minutes=60 * 200)
                    continue

                to_date = (current_date + timedelta(minutes=60 * 199)).strftime(
                    "%Y-%m-%dT%H:%M:%S"
                )
                future = executor.submit(fetch_ohlcv_data, market, to_date, 200)
                print(f"Scheduled fetch for date: {current_date}")
                futures.append((current_date, future))
                current_date += timedelta(minutes=60 * 200)
                time.sleep(0.2)
                # 현재 작업중인 스레드 개수 및 작업 대기열 상태 출력
            print(f"Current active threads: {len(executor._threads)}")
            print(f"Tasks in queue: {executor._work_queue.qsize()}")

            for date, future in sorted(futures, key=lambda x: x[0]):
                try:
                    result = future.result()
                    if result:
                        print(
                            f"Data fetched for date {current_date}: {len(result)} records"
                        )
                        data.extend(result)
                    else:
                        print("No result from future")  # 결과가 없는 경우 로그
                except Exception as e:
                    print(f"Exception occurred while fetching data: {e}")

    return data


def process_data(
    data: List[Dict], start_date: datetime, end_date: datetime, session: Session
) -> None:

    print(f"Total records fetched before removing duplicates: {len(data)}")

    # 중복 제거 전 데이터 확인
    if len(data) >= 200:
        print("Records from index 191 to 200 before processing:")
        for record in data[190:200]:  # 인덱스가 0부터 시작하므로 190부터 199까지
            print(record)

    # 최초 1년치 데이터 요청에서 40개의 데이터를 제거
    if len(data) > 8760:
        data = data[:8760]
        print(f"Trimmed data to 8760 records.")

    # 중복 제거
    unique_data = {entry["candle_date_time_kst"]: entry for entry in data}
    data = list(unique_data.values())
    print(f"Total records after removing duplicates: {len(data)}")

    # 데이터를 시간 순서로 정렬
    data.sort(key=lambda x: x["candle_date_time_kst"])
    print(f"Data sorted by time in ascending order.")

    # 기존 데이터 확인
    existing_times = set(time[0] for time in session.query(BtcOhlc.time).all())
    new_records = [
        item
        for item in data
        if datetime.strptime(item["candle_date_time_kst"], "%Y-%m-%dT%H:%M:%S")
        not in existing_times
    ]

    records = []
    for item in new_records:
        try:
            time_value = datetime.strptime(
                item["candle_date_time_kst"], "%Y-%m-%dT%H:%M:%S"
            )
            new_record = BtcOhlc(
                time=time_value,
                open=item["opening_price"],
                high=item["high_price"],
                low=item["low_price"],
                close=item["trade_price"],
                volume=item["candle_acc_trade_volume"],
            )
            records.append(new_record)
        except Exception as e:
            print(f"Error preparing record: {e}, data: {item}")

    # 데이터를 먼저 삽입
    if session.query(BtcOhlc).count() == 0:
        session.bulk_insert_mappings(BtcOhlc, [record.__dict__ for record in records])
        session.commit()
        print(
            f"{len(records)} records inserted successfully using bulk_insert_mappings."
        )
    else:
        for record in records:
            stmt = pg_insert(BtcOhlc).values(
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
        print(f"{len(records)} records inserted successfully using session.add().")

    # 누락된 시간 확인 및 채우기(데이터 자체 시간이 KST로 되어있음)
    # btc_ohlc 에 존재하지 않는 시간을 찾는 쿼리
    print("Checking for missing times...")
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
            FROM btc_ohlc
        )
    """
    #  fetchall() 사용해서 CursorResult를 리스트로 변환
    missing_times = session.execute(
        missing_times_query, {"start_time": start_date, "end_time": end_date}
    ).fetchall()

    # 누락된시간
    missing_times = [mt[0] for mt in missing_times]

    print(f"Missing times (KST): {missing_times}")

    # 누락된 시간에 대한 로그 추가
    print(f"Total missing times detected: {len(missing_times)}")

    if missing_times:

        sample_data = session.query(BtcOhlc).first()
        print(f"Sample data from database: {sample_data}")
        print(
            f"Sample data attributes: time={sample_data.time}, type={type(sample_data.time)}, open={sample_data.open}, high={sample_data.high}, low={sample_data.low}, close={sample_data.close}, volume={sample_data.volume}"
        )
        if sample_data:
            print(
                f"Sample data attributes: time={sample_data.time}, open={sample_data.open}, high={sample_data.high}, low={sample_data.low}, close={sample_data.close}, volume={sample_data.volume}"
            )

        # 보간을 위한 데이터프레임 생성
        existing_data = session.query(BtcOhlc).order_by(BtcOhlc.time).all()
        # 디버그: 기존 데이터 출력
        print(f"Existing data sample: {existing_data[:10]}")

        df_existing = pd.DataFrame(
            [(d.time, d.open, d.high, d.low, d.close, d.volume) for d in existing_data],
            columns=["time", "open", "high", "low", "close", "volume"],
        )
        # 인덱스로 설정하지 않음
        df_existing.reset_index(drop=True, inplace=True)
        df_existing["time"] = pd.to_datetime(df_existing["time"])

        # 누락된 시간에 대한 데이터프레임 생성
        missing_times_corrected = [
            mt.replace(minute=0, second=0, microsecond=0) for mt in missing_times
        ]
        df_missing = pd.DataFrame(missing_times_corrected, columns=["time"])
        df_missing["open"] = np.nan
        df_missing["high"] = np.nan
        df_missing["low"] = np.nan
        df_missing["close"] = np.nan
        df_missing["volume"] = np.nan

        # 데이터 타입 일치시키기
        df_missing = df_missing.astype(
            {
                "open": "float64",
                "high": "float64",
                "low": "float64",
                "close": "float64",
                "volume": "float64",
            }
        )

        # df_missing의 각 행을 하나씩 출력
        print("Missing data rows:")
        for index, row in df_missing.iterrows():
            print(f"Index: {index}, Row: {row.to_dict()}")

        # 인덱스 타입 확인을 위한 로그 추가
        print(f"df_existing index type: {type(df_existing.index[0])}")
        print(f"df_missing index type: {type(df_missing.index[0])}")

        # 기존 데이터프레임과 누락된 시간 데이터프레임 결합
        df_combined = pd.concat([df_existing, df_missing])
        df_combined.sort_values("time", inplace=True)

        # 디버그: 결합된 데이터프레임 출력
        print(f"Combined DataFrame head:\n{df_combined.head(10)}")
        print(f"Combined DataFrame tail:\n{df_combined.tail(10)}")
        print(
            f"Data before interpolation:\n{df_combined.head(10)}"
        )  # 보간 전 데이터 일부 출력

        # 데이터프레임을 다시 인덱스로 설정하여 보간
        df_combined.set_index("time", inplace=True)

        # 결측치 채우기 (선형 보간법 적용)
        df_combined.interpolate(method="linear", inplace=True)

        df_combined["open"] = df_combined["open"].round(0).astype(float)
        df_combined["high"] = df_combined["high"].round(0).astype(float)
        df_combined["low"] = df_combined["low"].round(0).astype(float)
        df_combined["close"] = df_combined["close"].round(0).astype(float)

        print(
            f"Data after interpolation:\n{df_combined.head(10)}"
        )  # 보간 후 데이터 일부 출력
        print(
            f"Data after interpolation (tail):\n{df_combined.tail(10)}"
        )  # 보간 후 데이터 일부 출력

        # 보간된 데이터 타입 확인
        print(f"Interpolated DataFrame dtypes:\n{df_combined.dtypes}")
        # 인덱스를 컬럼으로 다시 변환
        df_combined.reset_index(inplace=True)

        # 보간된 데이터를 데이터베이스에 삽입 (UPSERT) (UPSERT 란 INSERT ON CONFLICT DO UPDATE).
        # 중복된 기본 키나 고유 제약 조건이 있는 레코드를 삽입할 때 충돌이 발생하면 기존 레코드를 업데이트하고,
        # 그렇지 않으면 새로운 레코드를 삽입할 수 있도록 하기 위함입니다. 이를 통해 중복 키 오류를 방지할 수 있습니다.

        # 보간된 데이터만 필터링
        interpolated_df = df_combined.loc[
            df_combined["time"].isin(missing_times_corrected)
        ]
        print(f"Interpolated DataFrame head:\n{interpolated_df.head(10)}")
        print(f"Interpolated DataFrame tail:\n{interpolated_df.tail(10)}")

        # 누락된 시간의 데이터만 데이터베이스에 삽입
        for index, row in interpolated_df.iterrows():
            # index 값을 문자열로 변환
            time_value = row[
                "time"
            ].to_pydatetime()  # to_pydatetime()으로 datetime.datetime 객체로 변환
            print(f"type time_value:{type(time_value)}")
            query = f"""
                INSERT INTO btc_ohlc (time, open, high, low, close, volume)
                VALUES ('{time_value}', {row['open']}, {row['high']}, {row['low']}, {row['close']}, {row['volume']});
            """
            print(f"Executing query: {query}")
            session.execute(query)
        session.commit()
        print(
            f"{len(interpolated_df)} interpolated records inserted successfully using session.add()."
        )

        # 총 누락된 시간과 보간된 시간 개수 비교
        total_missing_times = len(missing_times)
        print(f"Total missing times: {total_missing_times}")
        print(f"Interpolated records: {len(interpolated_df)}")

        # 데이터 정렬 쿼리
        sort_query = text(
            """
            CREATE TEMP TABLE sorted_btc_ohlc AS
            SELECT * FROM btc_ohlc
            ORDER BY time;

            DELETE FROM btc_ohlc;

            INSERT INTO btc_ohlc
            SELECT * FROM sorted_btc_ohlc;

            DROP TABLE sorted_btc_ohlc;
        """
        )

        # 정렬 쿼리 실행
        session.execute(sort_query)

        # 최종 커밋
        session.commit()

    # 최종 데이터 정렬 및 조회
    start_sort_time = time.time()
    sorted_data = session.query(BtcOhlc).order_by(BtcOhlc.time.asc()).all()
    end_sort_time = time.time()
    sort_duration = end_sort_time - start_sort_time
    print(f"Data sorting took {sort_duration} seconds")
    print(f"Total sorted records after interpolation: {len(sorted_data)}")


# 데이터를 수집하고 데이터베이스에 적재하는 함수
def collect_and_load_data() -> None:
    start_time = time.time()  # 시작 시간 기록
    metadata = MetaData()
    inspector = inspect(
        engine
    )  # sqlalchemy에서 데이터베이스 메타데이터를 추출하는데 사용하는 방식. 테이블 존재여부의 작업을 할때 권장된다.
    # 테이블 존재 여부 확인 및 생성
    try:
        if not inspector.has_table("btc_ohlc"):
            Base.metadata.create_all(bind=engine)
            print("Table 'btc_ohlc' created.")  # 테이블 생성 로그 추가

    except IntegrityError:
        # 테이블이 이미 존재하는 경우 무시
        print("Table 'btc_ohlc' already exists.")
        session.rollback()

    try:
        end_date = datetime.now()  # 현재 시간을 종료 시간으로 설정
        result = None  # 초기화

        try:
            # 데이터베이스에서 가장 최근 데이터의 시간을 가져옵니다.
            result = session.query(func.max(BtcOhlc.time)).scalar()
            print(f"Query result: {result}")  # 쿼리 결과를 로그에 남김
        except Exception as query_exception:
            print(f"Query exception occurred: {query_exception}")
            # result가 None이 되도록 함으로써 이후 로직에서 올바르게 처리되도록 함
            result = None
        last_loaded_time = result

        # 만약 최근 로드된 데이터 시간이 존재하면, 그 시간 이후로 데이터를 요청합니다.
        # 그렇지 않으면, 기본적으로 1년 전부터 데이터를 요청합니다.
        # KST = timezone(timedelta(hours=9))
        end_date = datetime.now()
        if last_loaded_time and last_loaded_time < end_date:
            start_date = last_loaded_time + timedelta(
                hours=1
            )  # 최근 데이터 이후 1시간부터 시작
        else:
            start_date = end_date - timedelta(
                days=365
            )  # 데이터가 없거나 가장 최근 데이터가 현재 시간 이후인 경우
            print(
                "No recent data found or invalid last loaded time. Fetching data from one year ago."
            )

        print(f"Fetching data from {start_date} to {end_date}")
        existing_times = set(time[0] for time in session.query(BtcOhlc.time).all())
        data = fetch_data(start_date, end_date, existing_times)

        if data:
            print(
                f"Inserting {len(data)} records into the database."
            )  # 삽입할 데이터 수를 로그에 남김
            process_data(data, start_date, end_date, session)

    except Exception as e:
        print(f"An error occurred: {e}")
        session.rollback()
    finally:
        session.close()
        end_time = time.time()  # 종료 시간 기록
        elapsed_time = end_time - start_time
        print(f"Elapsed time for data load: {elapsed_time} seconds")
