from sqlalchemy import Column, DateTime, Integer, select, func, text
from sqlalchemy.ext.asyncio import (
    create_async_engine,
    AsyncSession,
    async_scoped_session,
)
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import declarative_base, sessionmaker
from contextvars import ContextVar
from datetime import datetime, timedelta

import numpy as np
import logging
import asyncio
import uvloop
import time

# uvloop를 기본 이벤트 루프로 설정
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

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


# ContextVar를 사용하여 세션을 관리(비동기 함수간에 컨텍스트를 안전하게 전달하도록 해줌. 세션을 여러 코루틴간에 공유 가능)
session_context = ContextVar("session_context", default=None)


async def fill_missing_and_null_data(
    session: AsyncSession,
    conn: AsyncSession,
    past_new_time: str,
    new_time: str,
    current_time: str,
    minutes: int,
) -> None:
    """
    - XCom으로 받아온 변수 설명
    initial_insert : save_raw_data_API_fn 태스크에서 데이터를 적재할 때 최초 삽입시 True, 아닐시 False
    new_time : save_raw_data_API_fn 태스크에서 적재 후 db에 적재된 데이터 중 가장 최근시간
    past_new_time : save_raw_data_API_fn 태스크에서 적재 되기 전에 db에 존재하는 데이터 중 가장 최근시간 (없으면 None)
    current_time : save_raw_data_API_fn 태스크에서 업비트에 데이터를 호출했을 당시의 시간


    - 함수에 대한 설명
    이 함수는 이전 태스크에서 적재된 데이터 중 누락된 데이터 or null 값이 있는 데이터를 확인한 후
    만약 누락된 데이터나 null값이 존재하지 않을시 즉시 함수를 종료시킵니다
    처리해야 할 데이터가 존재한다면 선형보간법을 적용하고 upsert방식으로 btc_preprocessed 테이블에 삽입합니다
    """
    start_time = time.time()
    # 만약 존재하면 datetime으로 바꿔주고, 없을 시 None
    if past_new_time:
        past_new_time_plus_1_hour = (
            datetime.fromisoformat(past_new_time) + timedelta(hours=1)
        ).isoformat()
    else:
        past_new_time = None

    # current time : 정각의 시간을 필요로 하기 때문에 시간 밑으로는 버림
    # one_year_ago : generate_series로 current_time을 기준으로 과거 1년치의 데이터를 가져오기 위한 기간설정
    current_time = (
        datetime.fromisoformat(current_time).replace(minute=0, second=0, microsecond=0)
    ).isoformat()
    one_year_ago = (
        datetime.fromisoformat(current_time) - timedelta(days=365, hours=-1)
    ).isoformat()

    logger.info(f"type of new_time : {type(new_time)}")
    logger.info(f"new_time : {new_time}")
    logger.info(f"type of past_new_time : {type(past_new_time)}")
    logger.info(f"past_new_time : {past_new_time}")
    logger.info(f"type of current_time : {type(current_time)}")
    logger.info(f"current_time : {current_time}")

    # 최초 삽입 시 generate_series 범위를 1년으로 잡는다
    if past_new_time is None:
        query = text(
            f"""
            SELECT gs AS time, b.open, b.high, b.low, b.close, b.volume
            FROM generate_series(
                '{one_year_ago}'::timestamp,
                '{current_time}'::timestamp,
                interval '1 hour'
            ) AS gs
            LEFT JOIN btc_ohlcv b ON gs = b.time
            WHERE (b.time IS NULL OR b.open IS NULL OR b.high IS NULL OR b.low IS NULL OR b.close IS NULL OR b.volume IS NULL)
            AND gs <= '{new_time}'::timestamp
            """
        )

    # 최초 삽입이 아닐 시에는 삽입 전 가장 최신데이터의 시간(past_new_time) 이후부터 ~ 이전 태스크에서 적재된 후
    # 최신데이터의 시간(new_time)까지 generate_series 범위로 선택하여 이 기간동안의 데이터 중 null값이 있거나 누락된 경우를 불러온다 (save_raw_data task에서 적재된 데이터 기간에 해당)
    else:
        logger.info("-----------------")
        query = text(
            f"""
            SELECT gs AS time, b.open, b.high, b.low, b.close, b.volume
            FROM generate_series(
                '{past_new_time_plus_1_hour}'::timestamp,
                '{new_time}'::timestamp,
                interval '1 hour'
            ) AS gs
            LEFT JOIN btc_ohlcv b ON gs = b.time
            WHERE (b.time IS NULL OR b.open IS NULL OR b.high IS NULL OR b.low IS NULL OR b.close IS NULL OR b.volume IS NULL)
            AND gs > '{past_new_time}'::timestamp
            """
        )
        logger.info("-----------------")
        logger.info(query)

    result = await conn.execute(query)
    missing_or_null_data = result.fetchall()
    logger.info(f"Missing or null data counts : {len(missing_or_null_data)}")
    logger.info(f"Missing or null data records: {missing_or_null_data}")

    # 누락된 데이터 or null값인 데이터가 존재하지 않을 시 이 함수 탈출
    if not missing_or_null_data:
        logger.info(
            "No missing or null data found. Skipping fill_missing_and_null_data."
        )
        return

    """
    이동평균으로 누락된 데이터 및 null 값 채우기


    period : 얼마만큼의 기간으로 이동평균을 설정할지. 기준은 5분봉 일 때 1일치 이동평균을 기준으로 잡도록 설정하고
    -> 간격이 줄어들수록 적은 기간으로, 늘어날수록 긴 기간으로 동적으로 설정되도록 적절하게 선정
    num_data_points : 그 기간에 맞게 불러올 데이터 수
    """
    period = (minutes / 5) ** 0.5
    num_data_points = int(period * 24 * 60) // minutes

    current_time_dt = datetime.fromisoformat(current_time).replace(
        minute=0, second=0, microsecond=0
    )
    # null 값이 없는 데이터만 가져오기
    all_data_query = (
        select(BtcOhlcv)
        .filter(BtcOhlcv.time <= current_time_dt)
        .filter(BtcOhlcv.open.isnot(None))
        .filter(BtcOhlcv.high.isnot(None))
        .filter(BtcOhlcv.low.isnot(None))
        .filter(BtcOhlcv.close.isnot(None))
        .filter(BtcOhlcv.volume.isnot(None))
        .order_by(BtcOhlcv.time)
    )
    all_data_result = await session.execute(all_data_query)
    all_data = all_data_result.scalars().all()

    # 메모리에서 처리하기 위해 딕셔너리 형태로 데이터를 변환
    data_dict = {d.time: d for d in all_data}
    noise_factor = 0.3  # 1시간봉 기준 0.3이 적당하다고 판단. 저 작은 분봉 데이터를 사용하게되면 좀 더 낮춰도 될듯하다
    # 낮췄을 때는 좀 더 변동성이 클 것이고 이동평균에 사용되는 데이터갯수도 많아지므로 낮춰서 변동성을 좀 더 줄이고 원래패턴과 유사하도록 설정

    for record in missing_or_null_data:
        missing_time = record[0]

        # 이동평균 계산을 위한 데이터 가져오기
        prev_data = [
            data_dict[time] for time in sorted(data_dict) if time < missing_time
        ][-num_data_points:]

        open_avg = np.mean([d.open for d in prev_data])
        high_avg = np.mean([d.high for d in prev_data])
        low_avg = np.mean([d.low for d in prev_data])
        close_avg = np.mean([d.close for d in prev_data])
        volume_avg = np.mean([d.volume for d in prev_data])

        open_std = np.std([d.open for d in prev_data])
        high_std = np.std([d.high for d in prev_data])
        low_std = np.std([d.low for d in prev_data])
        close_std = np.std([d.close for d in prev_data])
        volume_std = np.std([d.volume for d in prev_data])

        # 표준편차의 noise_factor만큼의 비율만큼 랜덤으로 노이즈가 끼게하여 연속으로 존재하는 결측치가 같은값으로 대체되지 않도록 함
        open_avg += np.random.normal(0, open_std * noise_factor)
        high_avg += np.random.normal(0, high_std * noise_factor)
        low_avg += np.random.normal(0, low_std * noise_factor)
        close_avg += np.random.normal(0, close_std * noise_factor)
        volume_avg += np.random.normal(0, volume_std * noise_factor)

        logger.info(
            f"moving average interpolated open value for {missing_time}: {open_avg}"
        )
        logger.info(
            f"moving average interpolated high value for {missing_time}: {high_avg}"
        )
        logger.info(
            f"moving average interpolated low value for {missing_time}: {low_avg}"
        )
        logger.info(
            f"moving average interpolated close value for {missing_time}: {close_avg}"
        )
        logger.info(
            f"moving average interpolated volume value for {missing_time}: {volume_avg}"
        )

        new_record = BtcPreprocessed(
            time=missing_time,
            open=int(open_avg),
            high=int(high_avg),
            low=int(low_avg),
            close=int(close_avg),
            volume=int(volume_avg),
        )
        session.add(new_record)

    end_time = time.time()
    interpol_time = end_time - start_time
    logger.info(f"interpolate time : {interpol_time:.4f} sec")
    await session.commit()


# fill_missing_and_null_data 테스트를 위한 함수
async def insert_null_data(session: AsyncSession):
    null_data = [
        BtcOhlcv(
            time=datetime(2024, 6, 15, 19, 0, 0) + timedelta(hours=i),
            open=None,
            high=None,
            low=None,
            close=None,
            volume=None,
        )
        for i in range(10)
    ]
    session.add_all(null_data)
    await session.commit()
    logger.info("Inserted null data into btc_ohlcv")


async def preprocess_data(context: dict) -> None:
    """
    XCom으로 받아온 변수 설명
    db_uri : db주소
    minutes : save_raw_data_API_fn 태스크에서 호출한 데이터가 몇분봉 데이터인지
    initial_insert : save_raw_data_API_fn 태스크에서 데이터를 적재할 때 최초 삽입시 True, 아닐시 False
    new_time : save_raw_data_API_fn 태스크에서 적재 후 db에 적재된 데이터 중 가장 최근시간
    past_new_time : save_raw_data_API_fn 태스크에서 적재 되기 전에 db에 존재하는 데이터 중 가장 최근시간 (없으면 None)
    current_time : save_raw_data_API_fn 태스크에서 업비트에 데이터를 호출했을 당시의 시간
    """
    db_uri = context["db_uri"]
    engine = create_async_engine(
        db_uri.replace("postgresql", "postgresql+asyncpg"), future=True
    )
    session_factory = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
    # 비동기 함수들간에 세션을 안전하게 공유, 세션 자동생성/해제 하기 위해 사용. 세션 관리하기 좋고 코드의일관성 유지가능.
    AsyncScopedSession = async_scoped_session(
        session_factory, scopefunc=session_context.get
    )

    token = session_context.set(session_factory)

    # 이전 태스크에서 데이터가 최초삽입인지 아닌지를 구분해서 효율적으로 작업하기 위해 initial_insert를 xcom으로 받아옴
    initial_insert = context["initial_insert"]
    new_time = context["new_time"]
    past_new_time = context["past_new_time"]
    current_time = context["current_time"]
    minutes = context["minutes"]

    logger.info(f"initial_insert : {initial_insert}")
    logger.info(f"new_time : {new_time}")
    logger.info(f"past_new_time : {past_new_time}")

    # 테스트용 new_time 설계
    # new_time = (datetime.fromisoformat(past_new_time) + timedelta(hours=10)).isoformat()

    # 데이터가 추가되지 않았을시에는 이 작업을 하지 않음
    if new_time is None:
        logger.info("No new data to process. Exiting preprocess_data_fn.")
        return

    try:
        async with engine.connect() as conn:
            # Base.metadata.create_all 이 구문을 사용 시 SQLAlchemy의 비동기 지원을 활용해서 데이터베이스의 테이블을 동기방식으로 생성
            # 비동기 코드에서 동기 메소드를 호출할 수 있게 해줌. 테이블이 존재하지 않는 경우에만 테이블을 생성시킨다.
            await conn.run_sync(Base.metadata.create_all)
            await conn.commit()
            logger.info("Creating btc_preprocessed table if not exists")

            async with AsyncScopedSession() as session:
                # await insert_null_data(session)
                await fill_missing_and_null_data(
                    session, conn, past_new_time, new_time, current_time, minutes
                )
                start_time_in = time.time()
                if initial_insert:
                    # 초기 데이터 처리
                    logger.info("Initial data processing")
                    # SQLAlchemy Core API를 사용한 방식
                    # Core API는 내부적으로 최적화된 배치 처리 메커니즘을 사용할 수 있게 해주고 메모리 사용량도 줄어든다.
                    # insert().from_select() 를 사용하여 단일 sql쿼리를 생성해서 db에 대한 네트워크 왕복을 줄여서 좀 더 효율적이다.(db내에 테이블에 있는 데이터를 이동할 때 유용)
                    logger.info(
                        "Inserting ALL DATA from btc_ohlcv to btc_preprocessed using Core API"
                    )

                    stmt = pg_insert(BtcPreprocessed).from_select(
                        ["time", "open", "high", "low", "close", "volume"],
                        select(
                            BtcOhlcv.time,
                            BtcOhlcv.open,
                            BtcOhlcv.high,
                            BtcOhlcv.low,
                            BtcOhlcv.close,
                            BtcOhlcv.volume,
                        ).order_by(BtcOhlcv.time),
                    )
                    await conn.execute(stmt)
                    await conn.commit()
                    end_time_in = time.time()
                    initial_insert_time = end_time_in - start_time_in
                    logger.info(
                        f"initial inserting time : {initial_insert_time:.4f} sec"
                    )
                    logger.info("Data insertion completed.")

                    # 삽입된 데이터의 개수 로깅
                    count_stmt = select([func.count()]).select_from(BtcPreprocessed)
                    result = await conn.execute(count_stmt)
                    inserted_row_count = result.scalar()
                    logger.info(f"Number of rows inserted: {inserted_row_count}")

                else:
                    # 최초 삽입이 아니라면 airflow 스케줄링에 의한 작은 크기의 데이터들에 대한 처리작업 진행
                    logger.info("Not initial data. Try insert btc_preprocessed")
                    new_time = datetime.fromisoformat(new_time)
                    logger.info(f"new_time type : {type(new_time)}")

                    # 이전 태스크에 적재된 범위 내의 모든 데이터를 삽입
                    new_data = await session.execute(
                        select(BtcOhlcv)
                        .filter(
                            BtcOhlcv.time > text(f"'{past_new_time}'::timestamp"),
                            BtcOhlcv.time <= text(f"'{new_time}'::timestamp"),
                        )
                        .order_by(BtcOhlcv.time)
                    )
                    new_data = new_data.scalars().all()

                    """
                    pg_insert : PostgreSQL의 INSERT 명령어를 SQLAlchemy에서 사용할 수 있도록 해줌. 삽입 작업을 수행하기 위한 객체
                    values : 삽입할 데이터를 지정 (ohlcv)
                    coalesce : null이 아닌 첫번째 값을 반환.
                               여기서는 pg_insert(BtcPreprocessed).excluded 와 이미 존재하는 값(BtcPreprocessed) 중 NULL이 아닌 값을 선택
                    excluded : ON CONFLICT 절에서 사용되는 키워드로 , 충돌이 발생한 경우 새로운 값을 나타냄.
                               여기서는 on_conflict_do_update로 새로운값으로 업데이트(upsert)
                    """
                    for data in new_data:
                        stmt = (
                            pg_insert(BtcPreprocessed)
                            .values(
                                time=data.time,
                                open=data.open,
                                high=data.high,
                                low=data.low,
                                close=data.close,
                                volume=data.volume,
                            )
                            .on_conflict_do_update(
                                index_elements=["time"],
                                set_={
                                    "open": func.coalesce(
                                        pg_insert(BtcPreprocessed).excluded.open,
                                        BtcPreprocessed.open,
                                    ),
                                    "high": func.coalesce(
                                        pg_insert(BtcPreprocessed).excluded.high,
                                        BtcPreprocessed.high,
                                    ),
                                    "low": func.coalesce(
                                        pg_insert(BtcPreprocessed).excluded.low,
                                        BtcPreprocessed.low,
                                    ),
                                    "close": func.coalesce(
                                        pg_insert(BtcPreprocessed).excluded.close,
                                        BtcPreprocessed.close,
                                    ),
                                    "volume": func.coalesce(
                                        pg_insert(BtcPreprocessed).excluded.volume,
                                        BtcPreprocessed.volume,
                                    ),
                                },
                            )
                        )
                        await session.execute(stmt)
                    await session.commit()
                    logger.info(f"Data insertion completed. counts:{len(new_data)}")

                logger.info(
                    "Updating labels 1 or 0 for all entries in btc_preprocessed"
                )

                await conn.execute(
                    # 상승:1, 하락:0 으로 라벨링
                    # LAG(close) OVER (ORDER BY time) : time정렬된 데이터에서 현재 데이터의 이전close값을 가져옴
                    # 이것을 현재 close 값이랑 비교해서 1, 0 으로 라벨링
                    text(
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
                        WHERE btc_preprocessed.time = CTE.time;
                        """
                    )
                )
                await conn.commit()
                logger.info("Labels updated successfully for all entries")

                # 삽입된 데이터를 시간순으로 정렬
                await session.execute(
                    select(BtcPreprocessed).order_by(BtcPreprocessed.time)
                )

                logger.info("Sorting btc_preprocessed table by time")
                count_final = await session.scalar(
                    select(func.count()).select_from(BtcPreprocessed)
                )
                logger.info(f"Final Count of rows in btc_preprocessed: {count_final}")

            await conn.commit()

        logger.info("Label column added and updated successfully for missing times.")
    except Exception as e:
        session.rollback()
        logger.error(f"Data preprocessing failed: {e}")
        raise
    finally:
        session_context.reset(token)
        await engine.dispose()


def preprocess_data_fn(**context) -> None:
    s = time.time()
    ti = context["ti"]
    db_uri = ti.xcom_pull(key="db_uri", task_ids="create_table_fn")
    minutes = ti.xcom_pull(key="minutes", task_ids="save_raw_data_from_API_fn")
    initial_insert = ti.xcom_pull(
        key="initial_insert", task_ids="save_raw_data_from_API_fn"
    )
    new_time = ti.xcom_pull(key="new_time", task_ids="save_raw_data_from_API_fn")
    past_new_time = ti.xcom_pull(
        key="past_new_time", task_ids="save_raw_data_from_API_fn"
    )
    current_time = ti.xcom_pull(
        key="current_time", task_ids="save_raw_data_from_API_fn"
    )
    # 비동기 함수 호출 시 전달할 context 생성(XCom은 JSON직렬화를 요구해서 그냥 쓸려고하면 비동기함수와는 호환이 안됨)
    async_context = {
        "db_uri": db_uri,
        "initial_insert": initial_insert,
        "new_time": new_time,
        "past_new_time": past_new_time,
        "current_time": current_time,
        "minutes": minutes,
    }

    asyncio.run(preprocess_data(async_context))
    e = time.time()
    es = e - s
    logger.info(f"Total working time : {es:.4f} sec")
