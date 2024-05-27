from concurrent.futures import ThreadPoolExecutor
from airflow.providers.postgres.hooks.postgres import PostgresHook
from dags.module.info.connections import Connections
from dags.module.info.api import APIInformation
from sqlalchemy import (
    create_engine,
    Column,
    DateTime,
    Integer,
    MetaData,
    func,
    inspect,
    Index,
    text,
    delete,
    select,
)
from sqlalchemy.orm import Session, declarative_base
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.dialects.postgresql import insert as pg_insert
from datetime import datetime, timedelta
from typing import Optional, Tuple, Dict, List
import numpy as np
import pandas as pd

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
    open = Column(Integer)
    high = Column(Integer)
    low = Column(Integer)
    close = Column(Integer)
    volume = Column(Integer)
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
        "Authorization": generate_jwt_token(upbit_access_key, upbit_secret_key),
    }
    print(f"Calling API with URL: {url}")  # API 호출 URL 로그 출력
    backoff_time = 0.5
    max_backoff_time = 32  # 최대 대기 시간 설정 (64초). Ecponential Backoff방식 : 429에러 발생시 처음1초 대기 후 재시도하고, 이후에는 대기시간을 2배씩 늘려가면서 최대 64초까지 대기시킴.
    max_retries = 5
    retry_count = 0
    if count > 1:
        while retry_count < max_retries:
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                data = response.json()
                min_req, sec_req = check_remaining_requests(response.headers)
                if min_req is not None and sec_req is not None and sec_req < 5:
                    print(
                        f"Rate limit close to being exceeded. Waiting for 1 second..."
                    )
                    time.sleep(0.5)
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

    else:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            return data
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

                to_date = min(
                    current_date + timedelta(minutes=60 * 199), end_date
                ).strftime("%Y-%m-%dT%H:%M:%S")
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


def check_and_remove_duplicates(session: Session) -> None:
    """
    중복이 있는지 체크하고, 있다면 중복을 제거합니다.

    """
    duplicate_check_query = """
        SELECT time
        FROM btc_ohlc
        GROUP BY time
        HAVING COUNT(time) > 1
    """
    duplicates = session.execute(duplicate_check_query).fetchall()
    print("1111")
    if duplicates:
        print(f"Found duplicates: {duplicates}")
        # 중복된 시간에 대해 첫 번째 행만 남기고 나머지를 제거
        # ranked_ohlc : row_number 윈도우 함수를 통해 각 time값에 대해 행 번호를 매김
        # delete_stmt : ctid(postgresql에서 제공하는 시스템 컬럼) 를 사용하여 row_num > 1 보다 큰 행들을 삭제한다. 결론적으로 time 값에 대해 첫 번째 행만 남기고 삭제
        ranked_ohlc = select(
            BtcOhlc.time,
            BtcOhlc.ctid,
            func.row_number()
            .over(partition_by=BtcOhlc.time, order_by=BtcOhlc.time)
            .label("row_num"),
        ).subquery()

        delete_stmt = delete(BtcOhlc).where(
            BtcOhlc.ctid.in_(
                select([ranked_ohlc.c.ctid]).where(ranked_ohlc.c.row_num > 1)
            )
        )

        session.execute(delete_stmt)
        session.commit()
        print("Duplicates removed")
        # 중복이 제거된 후에 중복이 여전히 존재하는지 확인하는 로그 추가
        remaining_duplicates = session.execute(duplicate_check_query).fetchall()
        if remaining_duplicates:
            print(f"Remaining duplicates after removal: {remaining_duplicates}")
        else:
            print("No duplicates found after removal")
    else:
        print("No duplicates found")


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
            FROM btc_ohlc
        )
    """
    missing_times = session.execute(
        missing_times_query, {"start_time": start_date, "end_time": end_date}
    ).fetchall()
    missing_times = [mt[0] for mt in missing_times]

    if missing_times:
        existing_data = session.query(BtcOhlc).order_by(BtcOhlc.time).all()
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

        # df_missing = df_missing.astype(
        #     {
        #         "open": "float64",
        #         "high": "float64",
        #         "low": "float64",
        #         "close": "float64",
        #         "volume": "float64",
        #     }
        # )

        df_combined = pd.concat([df_existing, df_missing])
        df_combined.sort_values("time", inplace=True)

        df_combined.set_index("time", inplace=True)
        df_combined.interpolate(method="linear", inplace=True)

        # 모든 컬럼을 정수형으로 변환하고 메모리 최적화 (downcast)
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
            stmt = pg_insert(BtcOhlc).values(
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
        print(f"{len(interpolated_df)} interpolated records inserted successfully.")
    else:
        print("No missing values found")


def check_and_sort_data(session: Session) -> None:
    """
    데이터를 정렬하는 함수. 결측치 보간 후 정렬을 위해 사용.
    """

    sort_check_query = """
        SELECT time
        FROM btc_ohlc
    """
    result = session.execute(sort_check_query).fetchall()

    if result:
        # 첫 10개 행과 마지막 10개 행을 출력
        first = result[:10]
        last = result[-10:]
        print(f"First 10 times: {first}")
        print(f"Last 10 times: {last}")

    # 정렬이 필요한지 확인
    is_sorted = result == sorted(result)

    if not is_sorted:
        print("Sorting required. Proceeding with sort.")
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
        session.execute(sort_query)
        session.commit()

        # 정렬 후 데이터 다시 확인
        result_after_sort = session.execute(sort_check_query).fetchall()
        if result_after_sort:
            first_10_after_sort = result_after_sort[:10]
            last_10_after_sort = result_after_sort[-10:]
            print(f"First 10 times after sort: {first_10_after_sort}")
            print(f"Last 10 times after sort: {last_10_after_sort}")
        else:
            print("No data found after sorting")

        print("Data sorted")
    else:
        print("Data already sorted")


def process_data(
    data: List[Dict],
    start_date: datetime,
    end_date: datetime,
    session: Session,
    initial_load: bool,
) -> None:
    """ """
    s = time.time()
    records = []
    for item in data:
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
    e = time.time()
    se = e - s
    print(f"records time : {se}")
    # 데이터가 있는지 확인
    if records:
        try:

            # 1년치 데이터를 먼저 삽입
            if initial_load:
                session.bulk_insert_mappings(
                    BtcOhlc, [record.__dict__ for record in records]
                )
                session.commit()
                print(
                    f"{len(records)} records inserted successfully using bulk_insert_mappings."
                )
                # 결측치 보간
                s = time.time()
                check_and_interpolate_missing_values(session, start_date, end_date)
                e = time.time()
                es = e - s
                print(f"interpolate time: {es} seconds")

                # 데이터 정렬
                s = time.time()
                check_and_sort_data(session)
                e = time.time()
                es = e - s
                print(f"sorting time: {es} seconds")
                result = session.query(BtcOhlc.time).order_by(BtcOhlc.time).all()
                if result:
                    first = result[:10]
                    last = result[-10:]
                    print(f"First 10 times: {first}")
                    print(f"Last 10 times: {last}")
                else:
                    print("No data found in the table.")
            else:
                s = time.time()
                batch_size = 10  # 적절한 배치 크기로 설정
                print(1)
                print(records)
                for i in range(0, len(records), batch_size):
                    batch_records = records[i : i + batch_size]
                    retries = 0
                    max_retries = 3
                    while retries < max_retries:
                        try:
                            for record in batch_records:
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
                                print(2)
                                session.execute(upsert_stmt)
                            session.commit()
                            break
                        except OperationalError as e:
                            print(f"OperationalError occurred: {e}")
                            session.rollback()
                            retries += 1
                            if retries < max_retries:
                                wait_time = (
                                    2**retries
                                )  # 지수 백오프 방식으로 대기 시간 설정
                                print(f"Retrying in {wait_time} seconds...")
                                time.sleep(wait_time)
                            else:
                                print(
                                    "Max retries reached. Could not complete the transaction."
                                )
                                raise
                        except Exception as e:
                            print(f"An error occurred while inserting data: {e}")
                            session.rollback()
                            raise

                e = time.time()
                se = e - s
                print(f"records time : {se}")

        except OperationalError as e:
            print(f"OperationalError occurred: {e}")
            session.rollback()
        except Exception as e:
            print(f"An error occurred while inserting data: {e}")
            session.rollback()
        finally:
            session.close()

    # # 최종 데이터 정렬 및 조회
    # start_sort_time = time.time()
    # sorted_data = session.query(BtcOhlc).order_by(BtcOhlc.time.asc()).all()
    # end_sort_time = time.time()
    # sort_duration = end_sort_time - start_sort_time
    # print(f"Data sorting took {sort_duration} seconds")
    # print(f"Total sorted records after interpolation: {len(sorted_data)}")


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
        end_date = datetime.now()
        result = session.query(func.max(BtcOhlc.time)).scalar()
        last_loaded_time = result

        # 디버그: last_loaded_time과 end_date 출력
        print(f"last loaded time : {last_loaded_time} (type: {type(last_loaded_time)})")
        print(f"end date : {end_date} (type: {type(end_date)})")

        # KST 시간을 UTC 시간으로 변환
        if last_loaded_time:
            last_loaded_time = last_loaded_time - timedelta(hours=9)

        # 디버그: last_loaded_time과 end_date 출력
        print(f"last loaded time: {last_loaded_time}")
        print(f"end date: {end_date}")

        # 아직 1시간이 지나지 않았을 때 요청이 들어오면 작업 종료.
        # ex) 1시30분경에 요청이 들어온다면, 이미 1시에 데이터가 들어오는 작업이 진행됐으므로 2시가 되기 전까지는 이 태스크 실행이 의미가 없음

        if last_loaded_time:
            # if last_loaded_time.replace(
            #     minute=0, second=0, microsecond=0
            # ) == end_date.replace(minute=0, second=0, microsecond=0):
            #     return
            print("load not first. fetch 1 hour data")
            # timedelta(hours=1)로 하니까 정각에 호출하게 돼서 그 시간의 데이터를 가져오지못함. -> 1분 뒤의 시간으로 호출
            start_date = last_loaded_time + timedelta(hours=1, minutes=1)
            to_date = start_date.strftime("%Y-%m-%dT%H:%M:%S")
            print(f"to date: {to_date}")
            data = fetch_ohlcv_data("KRW-BTC", to_date, 1)
            initial_load = False

        # 그 외에는 최초삽입에 해당하므로 1년치 데이터 호출
        else:
            print("load first. fetch 365days data")
            start_date = end_date - timedelta(days=365)
            existing_times = set(time[0] for time in session.query(BtcOhlc.time).all())
            data = fetch_data(start_date, end_date, existing_times)
            # 중복 제거(check_and_remove_duplicates 함수는 sqlalchemy orm으로 작성되어서 테이블에 데이터가 존재하지 않을 시 중복제거 작업이 불가능)
            # 해쉬테이블을 이용해 제거
            unique_data = {entry["candle_date_time_kst"]: entry for entry in data}
            data = list(unique_data.values())
            initial_load = True

        # 세션 내 데이터 확인
        session_times = session.query(BtcOhlc.time).all()
        print(f"Data in session before processing: {session_times}")
        s_time = time.time()
        print(f"1234134: {data[0]}")
        if data:
            process_data(data, start_date, end_date, session, initial_load)
            print("data processing finished")
        else:
            print("No data fetched")
        check_and_sort_data(session)
        e_time = time.time()
        etime = e_time - s_time
        print(f"processing time: {etime} seconds")
    except Exception as e:
        print(f"An error occurred: {e}")
        session.rollback()

    finally:
        session.close()
        elapsed_time = time.time() - start_time
        print(f"Elapsed time for data load: {elapsed_time} seconds")
