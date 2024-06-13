from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import Column, DateTime, Integer, select, func, text, bindparam
from sqlalchemy.ext.asyncio import (
    create_async_engine,
    AsyncSession,
    async_scoped_session,
)
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import declarative_base, sessionmaker
from info.connections import Connections
from contextvars import ContextVar
from datetime import datetime, timedelta
import logging
import asyncio
import uvloop

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


async def fill_missing_and_null_data(session, conn, past_new_time_str, new_time_str):
    # 이번 파이프라인에서 적재된 데이터 중 누락된 데이터 or null 값이 있는 데이터를 확인해서 선형보간법으로 채워줌
    new_time = datetime.fromisoformat(new_time_str)
    new_time_iso = new_time.isoformat()
    # 만약 존재하면 datetime으로 바꿔주고, 없을 시 0으로 할당
    if past_new_time_str:
        past_new_time = datetime.fromisoformat(past_new_time_str)
        past_new_time_iso = past_new_time.isoformat()
        past_new_time_plus_1_hour = (past_new_time + timedelta(hours=1)).isoformat()
    else:
        past_new_time = None
    logger.info(f"type of new_time : {type(new_time)}")
    logger.info(f"new_time : {new_time}")
    logger.info(f"type of past_new_time : {type(past_new_time)}")
    logger.info(f"past_new_time : {past_new_time}")

    # 최초 삽입 시 generate_series 범위를 1년으로 잡는다
    if past_new_time is None:
        query = text(
            f"""
            SELECT gs AS time, b.open, b.high, b.low, b.close, b.volume
            FROM generate_series(
                (SELECT MIN(time) FROM btc_ohlcv),
                (SELECT MIN(time) + interval '1 year' - interval '1 hour' FROM btc_ohlcv),
                interval '1 hour'
            ) AS gs
            LEFT JOIN btc_ohlcv b ON gs = b.time
            WHERE (b.time IS NULL OR b.open IS NULL OR b.high IS NULL OR b.low IS NULL OR b.close IS NULL OR b.volume IS NULL)
            AND gs <= '{new_time_iso}'::timestamp
            """
        )
        # .bindparams(bindparam("new_time", value=new_time))
        # bindparam을 이용하면 쿼리에 직접 값을 입력하지 않고 파라미터를 바인딩할 수 있게 해줌.
        # 타입도 알아서 적절하게 변환시켜줌

    # 최초 삽입이 아닐 시에는 삽입 전 가장 최신데이터의 시간(past_new_time) 이후부터 ~ 이전 태스크에서 적재된 후 최신데이터의 시간(new_time)까지 generate_series 범위로 선택
    else:
        logger.info("-----------------")
        query = text(
            f"""
            SELECT gs AS time, b.open, b.high, b.low, b.close, b.volume
            FROM generate_series(
                '{past_new_time_plus_1_hour}'::timestamp,
                '{new_time_iso}'::timestamp,
                interval '1 hour'
            ) AS gs
            LEFT JOIN btc_ohlcv b ON gs = b.time
            WHERE (b.time IS NULL OR b.open IS NULL OR b.high IS NULL OR b.low IS NULL OR b.close IS NULL OR b.volume IS NULL)
            AND gs > '{past_new_time_iso}'::timestamp
            """
        )

        # .bindparams(
        #     bindparam("past_new_time", value=past_new_time),
        #     bindparam("new_time", value=new_time)
        # )
        logger.info("-----------------")
        logger.info(query)

    result = await conn.execute(query)
    missing_or_null_data = result.fetchall()
    logger.info(f"Missing or null data records: {missing_or_null_data}")

    # 선형보간법으로 누락된 데이터 및 null 값 채우기
    for record in missing_or_null_data:
        missing_time = record[0]
        prev_data = await session.execute(
            select(BtcOhlcv)
            .filter(BtcOhlcv.time < missing_time)
            .order_by(BtcOhlcv.time.desc())
            .limit(1)
        )
        prev_data = prev_data.scalars().first()

        next_data = await session.execute(
            select(BtcOhlcv)
            .filter(BtcOhlcv.time > missing_time)
            .order_by(BtcOhlcv.time.asc())
            .limit(1)
        )
        next_data = next_data.scalars().first()

        if prev_data and next_data:
            delta = (missing_time - prev_data.time) / (next_data.time - prev_data.time)
            interpolated_open = prev_data.open + delta * (
                next_data.open - prev_data.open
            )
            interpolated_high = prev_data.high + delta * (
                next_data.high - prev_data.high
            )
            interpolated_low = prev_data.low + delta * (next_data.low - prev_data.low)
            interpolated_close = prev_data.close + delta * (
                next_data.close - prev_data.close
            )
            interpolated_volume = prev_data.volume + delta * (
                next_data.volume - prev_data.volume
            )

            new_record = BtcPreprocessed(
                time=missing_time,
                open=interpolated_open,
                high=interpolated_high,
                low=interpolated_low,
                close=interpolated_close,
                volume=interpolated_volume,
            )
            session.add(new_record)
    await session.commit()


async def preprocess_data(context):
    """
    XCom으로 받아온 변수 설명
    initial_insert : save_raw_data_API_fn 태스크에서 데이터를 적재할 때 최초 삽입시 True, 아닐시 False
    new_time_str : save_raw_data_API_fn 태스크에서 적재 후 db에 적재된 데이터 중 가장 최근시간
    past_new_time_str : save_raw_data_API_fn 태스크에서 적재 되기 전에 db에 존재하는 데이터 중 가장 최근시간 (없으면 None)


    """
    hook = PostgresHook(postgres_conn_id=Connections.POSTGRES_DEFAULT.value)
    engine = create_async_engine(
        hook.get_uri().replace("postgresql", "postgresql+asyncpg"), future=True
    )
    session_factory = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
    # 비동기 함수들간에 세션을 안전하게 공유, 세션 자동생성/해제 하기 위해 사용. 세션 관리하기 좋고 코드의일관성 유지가능.
    AsyncScopedSession = async_scoped_session(
        session_factory, scopefunc=session_context.get
    )

    token = session_context.set(session_factory)

    # 이전 태스크에서 데이터가 최초삽입인지 아닌지를 구분해서 효율적으로 작업하기 위해 initial_insert를 xcom으로 받아옴
    initial_insert = context["initial_insert"]
    new_time_str = context["new_time_str"]
    past_new_time_str = context["past_new_time_str"]

    logger.info(f"initial_insert : {initial_insert}")
    logger.info(f"new_time_str : {new_time_str}")
    logger.info(f"past_new_time_str : {past_new_time_str}")

    # 데이터가 추가되지 않았을시에는 이 작업을 하지 않음
    if new_time_str is None:
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
                await fill_missing_and_null_data(
                    session, conn, past_new_time_str, new_time_str
                )

                if initial_insert:
                    # 초기 데이터 처리
                    logger.info("Initial data processing")
                    # SQLAlchemy Core API를 사용한 방식
                    # Core API는 내부적으로 최적화된 배치 처리 메커니즘을 사용할 수 있게 해주고 메모리 사용량도 줄어든다.
                    # insert().from_select() 를 사용하여 단일 sql쿼리를 생성해서 db에 대한 네트워크 왕복을 줄여서 좀 더 효율적이다.
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
                        ),
                    )
                    await conn.execute(stmt)
                    await conn.commit()
                    logger.info("Data insertion completed.")

                else:
                    # 최초 삽입이 아니라면 airflow 스케줄링에 의한 작은 크기의 데이터들에 대한 처리작업 진행
                    logger.info("Not initial data. Try insert btc_preprocessed")
                    new_time = datetime.fromisoformat(new_time_str)
                    logger.info(f"new_time type : {type(new_time_str)}")

                    new_data = await session.execute(
                        select(BtcOhlcv).filter(BtcOhlcv.time == new_time).limit(1)
                    )
                    new_data = new_data.scalars().first()

                    if new_data:
                        stmt = (
                            pg_insert(BtcPreprocessed)
                            .values(
                                time=new_data.time,
                                open=new_data.open,
                                high=new_data.high,
                                low=new_data.low,
                                close=new_data.close,
                                volume=new_data.volume,
                            )
                            .on_conflict_do_update(  # excluded : 충돌시 제외된 값을 업데이트
                                index_elements=["time"],
                                set_={
                                    "open": pg_insert(BtcPreprocessed).excluded.open,
                                    "high": pg_insert(BtcPreprocessed).excluded.high,
                                    "low": pg_insert(BtcPreprocessed).excluded.low,
                                    "close": pg_insert(BtcPreprocessed).excluded.close,
                                    "volume": pg_insert(
                                        BtcPreprocessed
                                    ).excluded.volume,
                                },
                            )
                        )
                        await session.execute(stmt)
                    await session.commit()

                    logger.info("Data insertion completed.")

                logger.info(
                    "Updating labels 1 or 0 for all entries in btc_preprocessed"
                )
                # 상승:1, 하락:0 으로 라벨링
                await conn.execute(
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
                logger.info("Sorting btc_preprocessed table by time in ascending order")
                # CLUSTER명령어로 primary key를 기준으로 정렬.

                await session.execute(
                    text(
                        """
                        ALTER TABLE btc_preprocessed
                        CLUSTER ON btc_preprocessed_pkey;
                        """
                    )
                )
                await session.commit()
                logger.info("btc_preprocessed table sorted successfully using CLUSTER")

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


# context["ti"].xcom_push(key="preprocessed", value=True)


def preprocess_data_fn(**context):

    initial_insert = context["ti"].xcom_pull(
        key="initial_insert", task_ids="save_raw_data_from_API_fn"
    )
    new_time_str = context["ti"].xcom_pull(
        key="new_time", task_ids="save_raw_data_from_API_fn"
    )
    past_new_time_str = context["ti"].xcom_pull(
        key="past_new_time", task_ids="save_raw_data_from_API_fn"
    )

    # 비동기 함수 호출 시 전달할 context 생성(XCom은 JSON직렬화를 요구해서 그냥 쓸려고하면 비동기함수와는 호환이 안됨)
    async_context = {
        "initial_insert": initial_insert,
        "new_time_str": new_time_str,
        "past_new_time_str": past_new_time_str,
    }

    asyncio.run(preprocess_data(async_context))
