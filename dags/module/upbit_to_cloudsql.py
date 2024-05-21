from queue import Queue
from concurrent.futures import ThreadPoolExecutor
from sqlalchemy import create_engine, Column, DateTime, Float, MetaData, func, inspect
from sqlalchemy.orm import Session, declarative_base
from sqlalchemy.exc import IntegrityError
from datetime import datetime, timedelta
import os
import requests
import time
import jwt
import uuid
import threading

# UPbit의 access key와 secret key를 환경 변수에서 가져옵니다.
# UPBIT_ACCESS_KEY = os.environ.get("UPBIT_ACCESS_KEY")
# UPBIT_SECRET_KEY = os.environ.get("UPBIT_SECRET_KEY")
# print("UPBIT_ACCESS_KEY:", os.environ.get("UPBIT_ACCESS_KEY"))
# print("UPBIT_SECRET_KEY:", os.environ.get("UPBIT_SECRET_KEY"))
# print(os.environ.get("AIRFLOW__CORE__SQL_ALCHEMY_CONN"))
# 환경변수로 불러오는게  안돼서 하드코딩해서 테스트중(키는 이메일로 보내놓음)
UPBIT_ACCESS_KEY = "6zfu7mYxCEkUW6mrUXVw5PTe8WobvBK6yFm45Jty"
UPBIT_SECRET_KEY = "K3xyuFgVLbwYcu3UZTCPBjjFXbZzjW4vd3F5oMeT"

if not UPBIT_ACCESS_KEY or not UPBIT_SECRET_KEY:
    raise ValueError("Upbit API keys are not set in the environment variables.")

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


# PostgreSQL 연결 정보를 환경 변수에서 가져옵니다.
postgres_conn_str = os.environ.get("AIRFLOW__CORE__SQL_ALCHEMY_CONN")
# print(postgres_conn_str)
if not postgres_conn_str:
    raise ValueError(
        "Postgres connection string is not set in the environment variables."
    )

# 데이터베이스 연결 엔진을 생성합니다.
engine = create_engine(postgres_conn_str)

# 데이터베이스 세션을 생성합니다.

session = Session(bind=engine)


# JWT 생성 함수
def generate_jwt_token(access_key, secret_key):
    payload = {"access_key": access_key, "nonce": str(uuid.uuid4())}
    jwt_token = jwt.encode(payload, secret_key, algorithm="HS256")
    # print(f'jwt_token: {jwt_token}')
    authorization_token = f"Bearer {jwt_token}"
    return authorization_token


# 남아있는 요청 수를 확인하고 대기 시간을 조절하는 함수.
def check_remaining_requests(headers):
    remaining_req = headers.get("Remaining-Req")
    if remaining_req:
        group, min_req, sec_req = remaining_req.split("; ")
        min_req = int(min_req.split("=")[1])
        sec_req = int(sec_req.split("=")[1])
        # if sec_req < 5:
        #     print(f"Rate limit close to being exceeded. Waiting for 1 second...")
        #     time.sleep(1)
        return min_req, sec_req
    return None, None


# 동기적으로 Upbit API를 사용하여 비트코인 시세 데이터를 가져오는 함수를 정의합니다.
def fetch_ohlcv_data(market, to, count):
    url = f"https://api.upbit.com/v1/candles/minutes/60?market={market}&to={to}&count={count}"
    headers = {
        "Accept": "application/json",
        "Authorization": generate_jwt_token(UPBIT_ACCESS_KEY, UPBIT_SECRET_KEY),
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
def fetch_data(start_date, end_date):
    market = "KRW-BTC"
    data = []
    max_workers = 5  # 적절한 스레드 수 설정 . 일반적으로 I/O 바운드 작업에서는 5~10개의 스레드가 적절한 성능을 제공한다고 함. 실험을 통해 적절히 조절가능
    batch_size = 5
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
                to_date = (current_date + timedelta(minutes=60 * 199)).strftime(
                    "%Y-%m-%dT%H:%M:%S"
                )
                future = executor.submit(fetch_ohlcv_data, market, to_date, 200)
                print(f"Scheduled fetch for date: {current_date}")
                futures.append((current_date, future))
                current_date += timedelta(minutes=60 * 200)
                time.sleep(0.2)
                # 스레드 개수 및 작업 대기열 상태 출력
            print(f"Current active threads: {len(executor._threads)}")
            print(f"Tasks in queue: {executor._work_queue.qsize()}")

            results = []
            for date, future in sorted(futures, key=lambda x: x[0]):
                try:
                    result = future.result()
                    if result:
                        # print(
                        #     f"Data fetched for future with result: {result}"
                        # )  # URL과 함께 결과 로그 추가
                        print(
                            f"Data fetched for date {current_date}: {len(result)} records"
                        )
                        results.extend(result)
                    else:
                        print("No result from future")  # 결과가 없는 경우 로그
                except Exception as e:
                    print(f"Exception occurred while fetching data: {e}")
            data.extend(results)

    # 중복제거 전 개수
    print(f"Total records fetched before removing duplicates: {len(data)}")
    # 데이터 정렬 및 중복 제거 시간 측정
    # start_sort_time = time.time()
    unique_data = {entry["candle_date_time_kst"]: entry for entry in data}
    data = list(unique_data.values())
    # sorted_data = sorted(unique_data.values(), key=lambda x: x["candle_date_time_kst"])

    # end_sort_time = time.time()
    # sort_duration = end_sort_time - start_sort_time
    # print(f"Data sorting took {sort_duration} seconds")
    print(f"Total records after removing duplicates: {len(data)}")

    return data


# 데이터를 수집하고 데이터베이스에 적재하는 함수
def collect_and_load_data():
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
        if last_loaded_time and last_loaded_time < end_date:
            start_date = last_loaded_time + timedelta(
                minutes=60
            )  # 최근 데이터 이후 1시간부터 시작
        else:
            start_date = end_date - timedelta(
                days=365
            )  # 데이터가 없거나 가장 최근 데이터가 현재 시간 이후인 경우
            print(
                "No recent data found or invalid last loaded time. Fetching data from one year ago."
            )

        print("Fetching data from Upbit")
        data = fetch_data(start_date, end_date)

        if data:
            print(
                f"Inserting {len(data)} records into the database."
            )  # 삽입할 데이터 수를 로그에 남김
            # 중복 제거를 위한 데이터 삽입 쿼리
            # insert_query = """
            # INSERT INTO btc_ohlc (time, open, high, low, close, volume)
            # SELECT * FROM (SELECT :time AS time, :open AS open, :high AS high, :low AS low, :close AS close, :volume AS volume) AS temp
            # WHERE NOT EXISTS (
            #     SELECT 1 FROM btc_ohlc WHERE time = temp.time
            # );
            # """
            records = []
            for item in data:
                try:
                    time_value = datetime.strptime(
                        item["candle_date_time_kst"], "%Y-%m-%dT%H:%M:%S"
                    )
                    existing_record = (
                        session.query(BtcOhlc)
                        .filter(BtcOhlc.time == time_value)
                        .first()
                    )
                    # print(f"Checking for existing record at time: {time_value}")
                    if existing_record:
                        print(f"Existing record found: {existing_record}")
                        continue

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
                    print(f"Error inserting record: {e}, data: {item}")
            # 일정 데이터 개수 이상일 때만 bulk insert 사용
            if len(records) >= 100:
                session.bulk_insert_mappings(
                    BtcOhlc, [record.__dict__ for record in records]
                )
                session.commit()
                print(
                    f"{len(records)} records inserted successfully using bulk_insert_mappings."
                )
            else:
                for record in records:
                    session.add(record)
                session.commit()
                print(
                    f"{len(records)} records inserted successfully using session.add()."
                )

            new_data_count = session.query(BtcOhlc).count()
            print(f"Total records in the database after insertion: {new_data_count}")
        #  31312
        else:
            print("No data retrieved.")

        # 누락된 시간 확인
        print("Checking for missing times...")
        result = session.execute(
            """
            SELECT time::timestamp
            FROM generate_series(:start_time, :end_time, interval '1 hour') AS time
            WHERE date_trunc('hour', time::timestamp) NOT IN (SELECT date_trunc('hour', time) FROM btc_ohlc)
            """,
            {"start_time": start_date, "end_time": end_date},
        )

        missing_times = []
        for row in result:
            missing_time = row[0]
            missing_times.append(missing_time)
            print(f"Missing time detected: {missing_time}")

            # 누락된 데이터에 대해서는 앞,뒤의 데이터 합의 평균을 사용해서 처리함.
            # 가장 가까운 이전 데이터
            prev_data = (
                session.query(BtcOhlc)
                .filter(BtcOhlc.time < missing_time)
                .order_by(BtcOhlc.time.desc())
                .first()
            )
            # 가장 가까운 이후 데이터
            next_data = (
                session.query(BtcOhlc)
                .filter(BtcOhlc.time > missing_time)
                .order_by(BtcOhlc.time.asc())
                .first()
            )

            if prev_data and next_data:
                new_record = BtcOhlc(
                    time=missing_time,
                    open=(prev_data.open + next_data.open) / 2,
                    high=(prev_data.high + next_data.high) / 2,
                    low=(prev_data.low + next_data.low) / 2,
                    close=(prev_data.close + next_data.close) / 2,
                    volume=(prev_data.volume + next_data.volume) / 2,
                )
                session.add(new_record)
                print(
                    f"Interpolated data for {missing_time}: open={new_record.open}, high={new_record.high}, low={new_record.low}, close={new_record.close}, volume={new_record.volume}"
                )

        if missing_times:
            session.commit()
            print(f"Missing times: {len(missing_times)} entries")
            print(f"Inserted interpolated records for missing times.")
        else:
            print("No missing times detected.")

        # 정렬된 데이터 다시 조회 및 시간측정
        start_sort_time = time.time()
        sorted_data = session.query(BtcOhlc).order_by(BtcOhlc.time).all()
        end_sort_time = time.time()
        sort_duration = end_sort_time - start_sort_time
        print(f"Data sorting took {sort_duration} seconds")
        print(f"Total sorted records after interpolation: {len(sorted_data)}")

    except Exception as e:
        print(f"An error occurred: {e}")
        session.rollback()
    finally:
        session.close()
        end_time = time.time()  # 종료 시간 기록
        elapsed_time = end_time - start_time
        print(f"Elapsed time for data load: {elapsed_time} seconds")


# collect_and_load_data()
