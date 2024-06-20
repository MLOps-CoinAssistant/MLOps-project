from dags.module.upbit_price_prediction.btc.create_table import (
    BtcOhlcv,
    BtcPreprocessed,
    BtcRsiState_75,
    BtcRsiState_25,
)
from sqlalchemy import select, func, text, and_, update, bindparam
from sqlalchemy.ext.asyncio import (
    create_async_engine,
    AsyncSession,
    async_scoped_session,
)
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import declarative_base, sessionmaker
from contextvars import ContextVar

from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

import psutil
import pandas as pd
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

# ContextVar를 사용하여 세션을 관리(비동기 함수간에 컨텍스트를 안전하게 전달하도록 해줌. 세션을 여러 코루틴간에 공유 가능)
session_context = ContextVar("session_context", default=None)


# 현재 시간(UTC+9)으로부터 365일이 지난 데이터를 데이터베이스에서 삭제하는 함수
async def delete_old_data(session: AsyncSession) -> None:
    threshold_date = datetime.now() - relativedelta(days=365)
    threshold_kst = threshold_date + timedelta(hours=9)
    now = datetime.now() + timedelta(hours=9)
    logger.info(f"now_kst : {now}")
    logger.info(f"Threshold date for deletion: {threshold_kst}")

    # 디버그를 위해 삭제할 데이터의 개수를 먼저 확인
    count_query = select(func.count()).where(BtcPreprocessed.time < threshold_kst)
    result = await session.execute(count_query)
    delete_count = result.scalar()
    logger.info(f"Number of records to be deleted: {delete_count}")
    if delete_count == 0:
        logger.info("There is No data to DELETE")
        return

    delete_query = BtcPreprocessed.__table__.delete().where(
        BtcPreprocessed.time < threshold_kst
    )
    await session.execute(delete_query)
    await session.commit()
    logger.info(f"Deleted {delete_count} old records from the database.")


async def fill_missing_and_null_data(
    session: AsyncSession,
    conn: AsyncSession,
    past_new_time: str,
    new_time: str,
    current_time: datetime,
    minutes: int,
    all_data,
) -> None:
    """
    - XCom으로 받아온 변수 설명
    initial_insert : save_raw_data_API_fn 태스크에서 데이터를 적재할 때 최초 삽입시 True, 아닐시 False
    new_time : save_raw_data_API_fn 태스크에서 적재 후 db에 적재된 데이터 중 가장 최근시간
    past_new_time : save_raw_data_API_fn 태스크에서 적재 되기 전에 db에 존재하는 데이터 중 가장 최근시간 (없으면 None)
    current_time : save_raw_data_API_fn 태스크에서 업비트에 데이터를 호출했을 당시의 시간을 second=0, microsecond=0 으로 잘라놓은 시간


    - 함수에 대한 설명
    이 함수는 이전 태스크에서 적재된 데이터 중 누락된 데이터 or null 값이 있는 데이터를 확인한 후
    만약 누락된 데이터나 null값이 존재하지 않을시 즉시 함수를 종료시킵니다
    처리해야 할 데이터가 존재한다면 선형보간법을 적용하고 upsert방식으로 btc_preprocessed 테이블에 삽입합니다
    """
    start_time = time.time()
    # 만약 존재하면 datetime으로 바꿔주고, 없을 시 None
    if past_new_time is not None:
        past_new_time_plus = (
            datetime.fromisoformat(past_new_time) - relativedelta(days=1, minutes=5)
        ).isoformat()
    else:
        past_new_time = None

    # current time : 정각의 시간을 필요로 하기 때문에 시간 밑으로는 버림
    # one_year_ago : generate_series로 current_time을 기준으로 과거 1년치의 데이터를 가져오기 위한 기간설정
    one_year_ago = (
        datetime.fromisoformat(new_time) - relativedelta(days=(365 + 1), minutes=-5)
    ).isoformat()

    logger.info(f"one_year_ago : {one_year_ago}")
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
                interval '5 minutes'
            ) AS gs
            LEFT JOIN btc_ohlcv b ON gs = b.time
            WHERE (b.time IS NULL OR b.open IS NULL OR b.high IS NULL OR b.low IS NULL OR b.close IS NULL OR b.volume IS NULL OR b.volume = 0)
            AND gs <= '{current_time}'::timestamp
            """
        )

    # 최초 삽입이 아닐 시에는 삽입 전 가장 최신데이터의 시간(past_new_time) 이후부터 ~ 이전 태스크에서 적재된 후
    # 최신데이터의 시간(new_time)까지 generate_series 범위로 선택하여 이 기간동안의 데이터 중 null값이 있거나 누락된 경우를 불러온다 (save_raw_data task에서 적재된 데이터 기간에 해당)
    else:
        query = text(
            f"""
            SELECT gs AS time, b.open, b.high, b.low, b.close, b.volume
            FROM generate_series(
                '{past_new_time_plus}'::timestamp,
                '{new_time}'::timestamp,
                interval '5 minutes'
            ) AS gs
            LEFT JOIN btc_ohlcv b ON gs = b.time
            WHERE (b.time IS NULL OR b.open IS NULL OR b.high IS NULL OR b.low IS NULL OR b.close IS NULL OR b.volume IS NULL OR b.volume = 0)
            AND gs > '{past_new_time}'::timestamp
            """
        )

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

        # prev_data = [d for d in prev_data if not any(np.isnan([d.open, d.high, d.low, d.close, d.volume]))]

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
        new_record = BtcPreprocessed(
            time=missing_time,
            open=open_avg,
            high=high_avg,
            low=low_avg,
            close=close_avg,
            volume=volume_avg,
        )
        session.add(new_record)

    end_time = time.time()
    interpol_time = end_time - start_time
    logger.info(f"interpolate time : {interpol_time:.4f} sec")
    await session.commit()
    logger.info(f"moving average interpolate completed")


# fill_missing_and_null_data 테스트를 위한 함수
async def insert_null_data(session: AsyncSession) -> None:
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


async def standard_scale_data(
    session: AsyncSession, first_time_: str, last_time: str, BATCH_SIZE: int = 100
) -> None:
    """
    지정된 시간 범위 내의 데이터를 표준화(스케일링)하는 함수."""

    logger.info(
        f"Standard scaling data in btc_preprocessed between {first_time_} and {last_time}"
    )
    logger.info(f"type of first_time = {type(first_time_)}")
    logger.info(f"type of last_time = {type(last_time)}")

    first_time = datetime.fromisoformat(first_time_)
    last_time = datetime.fromisoformat(last_time)
    logger.info(f"type of first_time after = {type(first_time)}")
    logger.info(f"first_time : {first_time}")
    logger.info(f"type of last_time after = {type(last_time)}")
    logger.info(f"last_time : {last_time}")
    # 데이터 선택
    stmt = (
        select(BtcPreprocessed)
        .where(BtcPreprocessed.time >= first_time)
        .where(BtcPreprocessed.time <= last_time)
        .order_by(BtcPreprocessed.time)
    )

    result = await session.execute(stmt)
    data = result.scalars().all()

    # 데이터 분할
    split_point = int(len(data) * 0.9)
    split_time = data[split_point - 1].time

    # 훈련 데이터 선택
    train_stmt = (
        select(BtcPreprocessed)
        .where(BtcPreprocessed.time >= first_time)
        .where(BtcPreprocessed.time <= split_time)
        .order_by(BtcPreprocessed.time)
    )

    train_result = await session.execute(train_stmt)
    train_data = train_result.scalars().all()

    # 테스트 데이터 선택
    test_stmt = (
        select(BtcPreprocessed)
        .where(BtcPreprocessed.time > split_time)
        .where(BtcPreprocessed.time <= last_time)
        .order_by(BtcPreprocessed.time)
    )

    test_result = await session.execute(test_stmt)
    test_data = test_result.scalars().all()

    # 훈련 데이터 배열 생성
    train_array = np.array(
        [
            [
                row.open,
                row.high,
                row.low,
                row.close,
                row.volume,
                row.ma_7,
                row.ma_14,
                row.ma_30,
                row.rsi_14,
            ]
            for row in train_data
        ]
    )

    # 훈련 데이터의 평균 및 표준편차 계산
    train_mean = np.mean(train_array, axis=0)
    train_std = np.std(train_array, axis=0)

    # 훈련 데이터 스케일링 및 업데이트
    logger.info("inserting scaled train data in btc_preprocessed...")
    await scale_and_update_data(session, train_data, train_mean, train_std, BATCH_SIZE)
    logger.info("train data update completed")
    # 테스트 데이터 스케일링 및 업데이트
    logger.info("inserting scaled test data in btc_preprocessed...")
    await scale_and_update_data(session, test_data, train_mean, train_std, BATCH_SIZE)
    logger.info("test data update completed")
    logger.info("ALL Standard scaling and update completed.")


async def scale_and_update_data(session: AsyncSession, data, mean, std, batch_size):
    """
    btc_preprocessed에 스케일링한 데이터를 업데이트하는 함수
    """

    async def update_batch(batch_data, batch_scaled):
        update_values = [
            {
                "b_time": row.time,
                "open": batch_scaled[j][0],
                "high": batch_scaled[j][1],
                "low": batch_scaled[j][2],
                "close": batch_scaled[j][3],
                "volume": batch_scaled[j][4],
                "ma_7": batch_scaled[j][5],
                "ma_14": batch_scaled[j][6],
                "ma_30": batch_scaled[j][7],
                "rsi_14": batch_scaled[j][8],
            }
            for j, row in enumerate(batch_data)
        ]

        await session.execute(
            update(BtcPreprocessed)
            .where(BtcPreprocessed.time == bindparam("b_time"))
            .values(
                open=bindparam("open"),
                high=bindparam("high"),
                low=bindparam("low"),
                close=bindparam("close"),
                volume=bindparam("volume"),
                ma_7=bindparam("ma_7"),
                ma_14=bindparam("ma_14"),
                ma_30=bindparam("ma_30"),
                rsi_14=bindparam("rsi_14"),
            ),
            update_values,
        )

    tasks = []
    for i in range(0, len(data), batch_size):
        batch_data = data[i : i + batch_size]

        batch_array = np.array(
            [
                [
                    row.open,
                    row.high,
                    row.low,
                    row.close,
                    row.volume,
                    row.ma_7,
                    row.ma_14,
                    row.ma_30,
                    row.rsi_14,
                ]
                for row in batch_data
            ]
        )

        batch_scaled = (batch_array - mean) / std
        tasks.append(update_batch(batch_data, batch_scaled))

    await asyncio.gather(*tasks)
    await session.commit()  # Commit the session after all batches


async def load_all_data(session: AsyncSession):
    # null 값이 없는 BtcOhlcv데이터만 가져오기
    all_data_query = (
        select(BtcOhlcv)
        .where(
            and_(
                BtcOhlcv.open.isnot(None),
                BtcOhlcv.high.isnot(None),
                BtcOhlcv.low.isnot(None),
                BtcOhlcv.close.isnot(None),
                BtcOhlcv.volume.isnot(None),
                BtcOhlcv.open != float("nan"),
                BtcOhlcv.high != float("nan"),
                BtcOhlcv.low != float("nan"),
                BtcOhlcv.close != float("nan"),
                BtcOhlcv.volume != float("nan"),
            )
        )
        .order_by(BtcOhlcv.time)
    )
    all_data_result = await session.execute(all_data_query)
    all_data = all_data_result.scalars().all()
    return all_data


async def add_moving_average(
    session: AsyncSession, first_time: str, last_time: str
) -> None:
    """
    이동평균선(MA)를 만드는 함수( 7일, 14일, 30일 )
    """
    logger.info(
        f"Add MA_7, MA_14, MA_30 in btc_preprocessed  between {first_time} and {last_time}"
    )
    await session.execute(
        text(
            f"""
            WITH subquery AS (
                SELECT
                    time,
                    AVG(close) OVER (ORDER BY time ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS avg_close_7,
                    AVG(close) OVER (ORDER BY time ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS avg_close_14,
                    AVG(close) OVER (ORDER BY time ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS avg_close_30
                FROM btc_preprocessed
                WHERE time BETWEEN '{first_time}' AND '{last_time}'
            )
            UPDATE btc_preprocessed
            SET
                ma_7 = subquery.avg_close_7,
                ma_14 = subquery.avg_close_14,
                ma_30 = subquery.avg_close_30
            FROM subquery
            WHERE btc_preprocessed.time = subquery.time
            AND btc_preprocessed.time BETWEEN '{first_time}' AND '{last_time}';
        """
        )
    )

    await session.commit()
    logger.info("MA add success")


async def add_ema(session: AsyncSession, first_time: str, last_time: str) -> None:
    """
    지수이동평균선(EMA)을 만드는 함수( 7일, 14일, 30일 )
    """
    logger.info(
        f"Add EMA_7, EMA_14, EMA_30 in btc_preprocessed between {first_time} and {last_time}"
    )
    await session.execute(
        text(
            """
            ALTER TABLE btc_preprocessed
            ALTER COLUMN ema_7 TYPE DOUBLE PRECISION,
            ALTER COLUMN ema_14 TYPE DOUBLE PRECISION,
            ALTER COLUMN ema_30 TYPE DOUBLE PRECISION
            """
        )
    )
    await session.execute(
        text(
            f"""
            WITH subquery AS (
                SELECT
                    time,
                    EXP(SUM(LOG(close)) OVER (ORDER BY time ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)) AS ema_close_7,
                    EXP(SUM(LOG(close)) OVER (ORDER BY time ROWS BETWEEN 13 PRECEDING AND CURRENT ROW)) AS ema_close_14,
                    EXP(SUM(LOG(close)) OVER (ORDER BY time ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)) AS ema_close_30
                FROM btc_preprocessed
                WHERE time BETWEEN '{first_time}' AND '{last_time}'
            )
            UPDATE btc_preprocessed
            SET
                ema_7 = subquery.ema_close_7,
                ema_14 = subquery.ema_close_14,
                ema_30 = subquery.ema_close_30
            FROM subquery
            WHERE btc_preprocessed.time = subquery.time
            AND btc_preprocessed.time BETWEEN '{first_time}' AND '{last_time}';
        """
        )
    )
    await session.commit()
    logger.info("EMA add success")


async def add_rsi(session: AsyncSession, first_time: str, last_time: str) -> None:
    """
    RSI(14일 기준의 상대강도지수) 를 만드는 함수.
    """
    logger.info(f"Add RSI_14 in btc_preprocessed between {first_time} and {last_time}")
    await session.execute(
        text(
            f"""
            WITH gains_and_losses AS (
                SELECT
                    time,
                    CASE WHEN close - LAG(close) OVER (ORDER BY time) > 0
                        THEN close - LAG(close) OVER (ORDER BY time)
                        ELSE 0
                    END AS gain,
                    CASE WHEN close - LAG(close) OVER (ORDER BY time) < 0
                        THEN LAG(close) OVER (ORDER BY time) - close
                        ELSE 0
                    END AS loss
                FROM btc_preprocessed
                WHERE time BETWEEN '{first_time}' AND '{last_time}'
            ),
            avg_gains_losses AS (
                SELECT
                    time,
                    AVG(gain) OVER (ORDER BY time ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS avg_gain,
                    AVG(loss) OVER (ORDER BY time ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS avg_loss
                FROM gains_and_losses
            )
            UPDATE btc_preprocessed
            SET rsi_14 = CASE
                            WHEN avg_loss = 0 THEN 100
                            WHEN avg_gain = 0 THEN 0
                            ELSE 100 - (100 / (1 + avg_gain / avg_loss))
                        END
            FROM avg_gains_losses
            WHERE btc_preprocessed.time = avg_gains_losses.time
            AND btc_preprocessed.time BETWEEN '{first_time}' AND '{last_time}';
            """
        )
    )
    await session.commit()
    logger.info("RSI add success")


async def add_rsi_over(session: AsyncSession, first_time: str, last_time: str) -> None:
    """
    RSI_14 값을 기반으로 rsi_over 피쳐를 추가하는 함수. (최초 삽입 시에만 호출)
    """

    logger.info(
        f"Add RSI_OVER in btc_preprocessed between {first_time} and {last_time}"
    )

    # 25 <= rsi <= 75 인 구간
    await session.execute(
        text(
            f"""
            UPDATE btc_preprocessed
            SET rsi_over = 2
            WHERE time BETWEEN '{first_time}' AND '{last_time}';
            """
        )
    )
    await session.commit()

    """
    - 서브쿼리 설명

    rsi_75_intervals : rsi가 75이상인 구간을 찾습니다. 75이상이면 start, 이하면 end로 interval_marker 설정

    rsi_75_ranges : SUM을 사용하여 구간의 시작과 끝을 식별합니다.
    75이상인 구간부터 start, end가 각각 등장할 때 마다 1씩 증가시켜 두 합을 비교해서 75이상인 구간을 찾습니다.
    구간이 끝날 때 마다 reset_count를 증가시킵니다.

    reset_ranges_75 : 이전 구간 종료 여부(이전 행의 reset_count보다 클때)를 확인해서 누적합계(range_start, range_end)를 초기화합니다. (75이상인 구간이 여러번 나올 것이므로)
    이 때 reset = 1 로 설정하여 구간이 종료됐음을 나타냅니다.

    max_rsi_75 : 각 구간 내에서 최고값과 그 시점을 찾습니다. reset_count를 group by 해서 구간을 분리합니다.

    UPDATE : 구간 내 최고값을 기준으로 과거에는 1, 미래에는 0 을 부여합니다. reset_count를 통해 구간을 구분합니다.
    """

    await session.execute(
        text(
            f"""
            WITH rsi_75_intervals AS (
                SELECT
                    time,
                    rsi_14,
                    CASE
                        WHEN rsi_14 >= 75 THEN 'start'
                        WHEN rsi_14 < 75 THEN 'end'
                    END AS interval_marker
                FROM btc_preprocessed
                WHERE time BETWEEN '{first_time}' AND '{last_time}'
            ),
            rsi_75_ranges AS (
                SELECT
                    time,
                    rsi_14,
                    interval_marker,
                    CASE
                        WHEN interval_marker = 'start' THEN
                            SUM(CASE WHEN interval_marker = 'start' THEN 1 ELSE 0 END) OVER (ORDER BY time)
                        ELSE 0
                    END AS range_start,
                    CASE
                        WHEN interval_marker = 'end' THEN
                            SUM(CASE WHEN interval_marker = 'end' THEN 1 ELSE 0 END) OVER (ORDER BY time)
                        ELSE 0
                    END AS range_end,
                    COUNT(CASE WHEN interval_marker = 'end' THEN 1 ELSE NULL END) OVER (ORDER BY time) AS reset_count
                FROM rsi_75_intervals
            ),
            reset_ranges_75 AS (
                SELECT
                    time,
                    rsi_14,
                    interval_marker,
                    CASE
                        WHEN LAG(reset_count, 1, 0) OVER (ORDER BY time) < reset_count THEN 0
                        ELSE range_start
                    END AS range_start,
                    CASE
                        WHEN LAG(reset_count, 1, 0) OVER (ORDER BY time) < reset_count THEN 0
                        ELSE range_end
                    END AS range_end,
                    reset_count
                FROM rsi_75_ranges
            ),
            max_rsi_75 AS (
                SELECT
                    reset_count,
                    MAX(rsi_14) AS max_rsi
                FROM reset_ranges_75
                WHERE range_start > range_end
                GROUP BY reset_count
            ),
            max_time_75 AS (
                SELECT
                    reset_count,
                    MIN(time) AS max_time
                FROM reset_ranges_75
                WHERE rsi_14 = (SELECT max_rsi FROM max_rsi_75 WHERE max_rsi_75.reset_count = reset_ranges_75.reset_count)
                GROUP BY reset_count
            )
            UPDATE btc_preprocessed
            SET rsi_over = CASE
                            WHEN btc_preprocessed.time <= max_time_75.max_time THEN 1
                            WHEN btc_preprocessed.time > max_time_75.max_time THEN 0
                            ELSE btc_preprocessed.rsi_over
                        END
            FROM max_rsi_75, reset_ranges_75, max_time_75
            WHERE btc_preprocessed.time BETWEEN '{first_time}' AND '{last_time}'
            AND btc_preprocessed.rsi_14 >= 75
            AND reset_ranges_75.reset_count = max_rsi_75.reset_count
            AND btc_preprocessed.time = reset_ranges_75.time
            AND max_time_75.reset_count = reset_ranges_75.reset_count;
            """
        )
    )
    await session.commit()

    # rsi_75_state 테이블에 상태 저장
    await session.execute(
        text(
            f"""
            INSERT INTO rsi_75_state (range_start, range_end, max_rsi, max_rsi_time)
            SELECT
                COALESCE(MIN(time), '1970-01-01') AS range_start,
                COALESCE(MAX(time), '1970-01-01') AS range_end,
                COALESCE(MAX(rsi_14), 0) AS max_rsi,
                COALESCE(MIN(time), '1970-01-01') AS max_rsi_time
            FROM btc_preprocessed
            WHERE rsi_14 >= 75
            GROUP BY time
            """
        )
    )

    # rsi < 25인 구간
    await session.execute(
        text(
            f"""
            WITH rsi_25_intervals AS (
                SELECT
                    time,
                    rsi_14,
                    CASE
                        WHEN rsi_14 <= 25 THEN 'start'
                        WHEN rsi_14 > 25 THEN 'end'
                    END AS interval_marker
                FROM btc_preprocessed
                WHERE time BETWEEN '{first_time}' AND '{last_time}'
            ),
            rsi_25_ranges AS (
                SELECT
                    time,
                    rsi_14,
                    interval_marker,
                    CASE
                        WHEN interval_marker = 'start' THEN
                            SUM(CASE WHEN interval_marker = 'start' THEN 1 ELSE 0 END) OVER (ORDER BY time)
                        ELSE 0
                    END AS range_start,
                    CASE
                        WHEN interval_marker = 'end' THEN
                            SUM(CASE WHEN interval_marker = 'end' THEN 1 ELSE 0 END) OVER (ORDER BY time)
                        ELSE 0
                    END AS range_end,
                    COUNT(CASE WHEN interval_marker = 'end' THEN 1 ELSE NULL END) OVER (ORDER BY time) AS reset_count
                FROM rsi_25_intervals
            ),
            reset_ranges_25 AS (
                SELECT
                    time,
                    rsi_14,
                    interval_marker,
                    CASE
                        WHEN LAG(reset_count, 1, 0) OVER (ORDER BY time) < reset_count THEN 0
                        ELSE range_start
                    END AS range_start,
                    CASE
                        WHEN LAG(reset_count, 1, 0) OVER (ORDER BY time) < reset_count THEN 0
                        ELSE range_end
                    END AS range_end,
                    reset_count
                FROM rsi_25_ranges
            ),
            min_rsi_25 AS (
                SELECT
                    reset_count,
                    MIN(rsi_14) AS min_rsi
                FROM reset_ranges_25
                WHERE range_start > range_end
                GROUP BY reset_count
            ),
            min_time_25 AS (
                SELECT
                    reset_count,
                    MIN(time) AS min_time
                FROM reset_ranges_25
                WHERE rsi_14 = (SELECT min_rsi FROM min_rsi_25 WHERE min_rsi_25.reset_count = reset_ranges_25.reset_count)
                GROUP BY reset_count
            )
            UPDATE btc_preprocessed
            SET rsi_over = CASE
                            WHEN btc_preprocessed.time <= min_time_25.min_time THEN 0
                            WHEN btc_preprocessed.time > min_time_25.min_time THEN 1
                            ELSE btc_preprocessed.rsi_over
                        END
            FROM min_rsi_25, reset_ranges_25, min_time_25
            WHERE btc_preprocessed.time BETWEEN '{first_time}' AND '{last_time}'
            AND btc_preprocessed.rsi_14 <= 25
            AND reset_ranges_25.reset_count = min_rsi_25.reset_count
            AND btc_preprocessed.time = reset_ranges_25.time
            AND min_time_25.reset_count = reset_ranges_25.reset_count;
            """
        )
    )

    await session.commit()

    # rsi_25_state 테이블에 상태 저장
    await session.execute(
        text(
            f"""
            INSERT INTO rsi_25_state (range_start, range_end, min_rsi, min_rsi_time)
            SELECT
                COALESCE(MIN(time), '1970-01-01') AS range_start,
                COALESCE(MAX(time), '1970-01-01') AS range_end,
                COALESCE(MAX(rsi_14), 0) AS min_rsi,
                COALESCE(MIN(time), '1970-01-01') AS min_rsi_time
            FROM btc_preprocessed
            WHERE rsi_14 <= 25
            GROUP BY time
            """
        )
    )
    await session.commit()

    logger.info("RSI_OVER add success")


async def update_rsi_state_and_data(
    session: AsyncSession, first_time: str, last_time: str
) -> None:
    """
    rsi_over 피쳐의 계산을 위해 미리 rsi_state 테이블에 저장하고,
    저장되어있는 정보를 이용해서 필요 시 rsi_over를 업데이트하고 정보를 저장합니다

    range_start : rsi_over 계산 시 특정구간(75 혹은 25) 에 들어갈 때의 시간 (timestamp)
    range_end :  rsi_over 계산 시 특정구간에서 나올 때의 시간 (timestamp)
    max_rsi : 마지막에 저장된 75이상 구간의 rsi의 최대값
    min_rsi : 마지막에 저장된 25이하 구간의 rsi의 최소값
    """

    logger.info(f"Updating RSI state and data between {first_time} and {last_time}")

    # DB에서 새로운 데이터 구간 가져오기
    query = f"""
    SELECT time, rsi_14
    FROM btc_preprocessed
    WHERE time BETWEEN '{first_time}' AND '{last_time}'
    ORDER BY time
    """
    result = await session.execute(text(query))

    # 새로 들어온 데이터를 데이터프레임으로 변환
    new_data_df = pd.DataFrame(result.fetchall(), columns=["time", "rsi_14"])
    logger.info(f"Fetched {len(new_data_df)} rows from btc_preprocessed")

    # DB에서 상태 저장 테이블의 최신 상태 가져오기
    rsi_75_state_query = "SELECT * FROM rsi_75_state ORDER BY range_end ASC LIMIT 1;"
    result_75 = await session.execute(text(rsi_75_state_query))
    state_75 = result_75.fetchone()

    if state_75:
        range_start_75 = state_75["range_start"]
        range_end_75 = state_75["range_end"]
        max_rsi_75 = state_75["max_rsi"]
        max_rsi_time_75 = state_75["max_rsi_time"]
    else:
        range_start_75 = range_end_75 = max_rsi_time_75 = "1970-01-01 00:00:00"
        max_rsi_75 = 0

    rsi_25_state_query = "SELECT * FROM rsi_25_state ORDER BY range_end ASC LIMIT 1;"
    result_25 = await session.execute(text(rsi_25_state_query))
    state_25 = result_25.fetchone()

    if state_25:
        range_start_25 = state_25["range_start"]
        range_end_25 = state_25["range_end"]
        min_rsi_25 = state_25["min_rsi"]
        min_rsi_time_25 = state_25["min_rsi_time"]
    else:
        range_start_25 = range_end_25 = min_rsi_time_25 = "1970-01-01 00:00:00"
        min_rsi_25 = 100

    # 새로운 데이터를 기존 상태와 비교 및 업데이트

    for index, row in new_data_df.iterrows():
        time = row["time"]
        rsi_14 = row["rsi_14"]

        # 처리 rsi >= 75
        if rsi_14 >= 75:
            if range_start_75 == "1970-01-01 00:00:00":
                range_start_75 = time
            if max_rsi_75 == 0 or rsi_14 > max_rsi_75:
                max_rsi_75 = rsi_14
                max_rsi_time_75 = time
        elif rsi_14 < 75 and range_start_75 != "1970-01-01 00:00:00":
            # 구간을 벗어났으므로 현재 시간을 종료시간으로 업데이트
            range_end_75 = time

            # 상태 저장 테이블에 상태 갱신
            insert_query = f"""
            INSERT INTO rsi_75_state (range_start, range_end, max_rsi, max_rsi_time)
            VALUES ('{range_start_75}', '{range_end_75}', {max_rsi_75}, '{max_rsi_time_75}')
            """
            await session.execute(text(insert_query))
            await session.commit()

            logger.info(
                f"Updated RSI > 75 state: range_start={range_start_75}, range_end={range_end_75}, max_rsi={max_rsi_75}, max_rsi_time={max_rsi_time_75}"
            )
            if (
                range_start_75 != "1970-01-01 00:00:00"
                and range_end_75 != "1970-01-01 00:00:00"
            ):
                # 구간을 벗어났으므로 rsi_over업데이트
                update_query = f"""
                UPDATE btc_preprocessed
                SET rsi_over = CASE
                                WHEN time <= '{max_rsi_time_75}' THEN 1
                                WHEN time > '{max_rsi_time_75}' THEN 0
                                END
                WHERE time BETWEEN '{range_start_75}' AND '{range_end_75}'
                AND rsi_14 >= 75
                """
                await session.execute(text(update_query))
                await session.commit()
                logger.info(
                    f"Updated btc_preprocessed rsi_over based on max_rsi_time_75"
                )

            # 상태 초기화
            range_start_75 = range_end_75 = max_rsi_time_75 = "1970-01-01 00:00:00"
            max_rsi_75 = 0

        # 처리 rsi <= 25
        if rsi_14 <= 25:
            if range_start_25 == "1970-01-01 00:00:00":
                range_start_25 = time
            if min_rsi_25 == 100 or rsi_14 < min_rsi_25:
                min_rsi_25 = rsi_14
                min_rsi_time_25 = time
        elif rsi_14 > 25 and range_start_25 != "1970-01-01 00:00:00":
            range_end_25 = time

            # 상태 저장 테이블에 상태 갱신
            insert_query = f"""
            INSERT INTO rsi_25_state (range_start, range_end, min_rsi, min_rsi_time)
            VALUES ('{range_start_25}', '{range_end_25}', {min_rsi_25}, '{min_rsi_time_25}')
            """
            await session.execute(text(insert_query))
            await session.commit()

            logger.info(
                f"Updated RSI < 25 state: range_start={range_start_25}, range_end={range_end_25}, min_rsi={min_rsi_25}, min_rsi_time={min_rsi_time_25}"
            )
            if (
                range_start_25 != "1970-01-01 00:00:00"
                and range_end_25 != "1970-01-01 00:00:00"
            ):
                # 구간을 벗어났으므로 rsi_over업데이트
                update_query = f"""
                UPDATE btc_preprocessed
                SET rsi_over = CASE
                                WHEN time <= '{min_rsi_time_25}' THEN 0
                                WHEN time > '{min_rsi_time_25}' THEN 1
                            END
                WHERE time BETWEEN '{range_start_25}' AND '{range_end_25}'
                AND rsi_14 <= 25
                """
                await session.execute(text(update_query))
                await session.commit()
                logger.info(
                    f"Updated btc_preprocessed rsi_over based on min_rsi_time_25"
                )

            # 상태 초기화
            range_start_25 = range_end_25 = min_rsi_time_25 = "1970-01-01 00:00:00"
            min_rsi_25 = 100

        # 마지막 정보 테이블에 저장
        if range_start_75 != "1970-01-01 00:00:00":
            range_end_75 = time  # or use the last known time
            insert_query = f"""
            INSERT INTO rsi_75_state (range_start, range_end, max_rsi, max_rsi_time)
            VALUES ('{range_start_75}', '{range_end_75}', {max_rsi_75}, '{max_rsi_time_75}')
            """
            await session.execute(text(insert_query))
            await session.commit()

            logger.info(
                f"Final Updated RSI > 75 state: range_start={range_start_75}, range_end={range_end_75}, max_rsi={max_rsi_75}, max_rsi_time={max_rsi_time_75}"
            )

        if range_start_25 != "1970-01-01 00:00:00":
            range_end_25 = time  # or use the last known time
            insert_query = f"""
            INSERT INTO rsi_25_state (range_start, range_end, min_rsi, min_rsi_time)
            VALUES ('{range_start_25}', '{range_end_25}', {min_rsi_25}, '{min_rsi_time_25}')
            """
            await session.execute(text(insert_query))
            await session.commit()

            logger.info(
                f"Final Updated RSI < 25 state: range_start={range_start_25}, range_end={range_end_25}, min_rsi={min_rsi_25}, min_rsi_time={min_rsi_time_25}"
            )

        # 25~75사이구간 2로 업데이트
        update_query = f"""
        UPDATE btc_preprocessed
        SET rsi_over = 2
        WHERE time BETWEEN '{first_time}' AND '{last_time}'
        AND rsi_14 > 25 AND rsi_14 < 75
        """
        await session.execute(text(update_query))
        await session.commit()
        logger.info(f"Set rsi_over to 2 for RSI between 25 and 75")

    result = await session.execute(
        text(
            f"SELECT time, rsi_14, rsi_over FROM btc_preprocessed WHERE time BETWEEN '{first_time}' AND '{last_time}'"
        )
    )
    final_data = pd.DataFrame(result.fetchall(), columns=["time", "rsi_14", "rsi_over"])
    logger.info(f"Final btc_preprocessed data: {final_data}")

    logger.info(
        f"Completed updating RSI state and data between {first_time} and {last_time}"
    )


async def update_labels(session: AsyncSession, first_time: str, last_time: str) -> None:
    """
    close 값의 전, 후 비교를 통해 상승:1, 하락:0 으로 라벨링하는 함수

    LAG(close) OVER (ORDER BY time) : time정렬된 데이터에서 현재 데이터의 이전close값을 가져옴
    이것을 현재 close 값이랑 비교해서 1, 0 으로 라벨링
    """
    logger.info("Updating labels 1 or 0 for all entries in btc_preprocessed")
    await session.execute(
        text(
            f"""
            WITH CTE AS (
                SELECT time, close,
                    LAG(close) OVER (ORDER BY time) AS prev_close
                FROM btc_preprocessed
                WHERE time BETWEEN '{first_time}' AND '{last_time}'
            )
            UPDATE btc_preprocessed
            SET label = CASE
                            WHEN CTE.close > CTE.prev_close THEN 1
                            ELSE 0
                        END
            FROM CTE
            WHERE btc_preprocessed.time = CTE.time
            AND btc_preprocessed.time BETWEEN '{first_time}' AND '{last_time}';
            """
        )
    )
    await session.commit()
    logger.info("Labels updated successfully for all entries")


async def insert_data(
    session: AsyncSession,
    conn: AsyncSession,
    initial_insert: bool,
    past_new_time: str,
    new_time: str,
) -> None:
    """ """
    if initial_insert:
        start_time_in = time.time()
        # 초기 데이터 처리
        logger.info("Initial data processing")
        # SQLAlchemy Core API를 사용한 방식
        # Core API는 내부적으로 최적화된 배치 처리 메커니즘을 사용할 수 있게 해주고 메모리 사용량도 줄어든다.
        # insert().from_select() 를 사용하여 단일 sql쿼리를 생성해서 db에 대한 네트워크 왕복을 줄여서 좀 더 효율적이다.(db내에 테이블에 있는 데이터를 이동할 때 유용)
        # .on_conflict_do_nothing() : 충돌이 생길 경우 업데이트 하지 않고 기존값을 유지. (fill_missing_and_null_data함수에서 결측치에 대한 처리 후 미리 넣었기 때문)
        logger.info(
            "Inserting ALL DATA from btc_ohlcv to btc_preprocessed using Core API"
        )

        stmt = (
            pg_insert(BtcPreprocessed)
            .from_select(
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
            .on_conflict_do_nothing(index_elements=["time"])
        )  # fill_missing_and_null_data함수에서 처리된 데이터를 btc_prerpocessed에 미리 넣고, btc_ohlcv를 가져오는 형태이므로 충돌시 업데이트를 하지 않음.
        await conn.execute(stmt)
        await conn.commit()
        end_time_in = time.time()
        initial_insert_time = end_time_in - start_time_in
        logger.info(f"initial inserting time : {initial_insert_time:.4f} sec")
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
            .where(
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
                .on_conflict_do_nothing()
            )
            await session.execute(stmt)

        await session.commit()
        logger.info(f"Data insertion completed. counts:{len(new_data)}")


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
        db_uri.replace("postgresql", "postgresql+asyncpg"),
        pool_size=10,
        max_overflow=20,
        future=True,
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

    current_time_dt = (
        datetime.fromisoformat(current_time).replace(second=0, microsecond=0)
    ).isoformat()

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
                # await insert_null_data(session) : 테스트용

                # null 값이 없는 raw_data 가져오기
                all_data = await load_all_data(session)

                # 누락되었거나, null값인 데이터에 대한 처리작업을 한 후 그 데이터만 먼저 btc_preprocessed에 넣는다.
                await fill_missing_and_null_data(
                    session,
                    conn,
                    past_new_time,
                    new_time,
                    current_time_dt,
                    minutes,
                    all_data,
                )
                # btc_ohlcv에서 1년1개월치에 해당하는 데이터만 btc_preprocessed에 넣는다. 단, 이 때 충돌이 나는 데이터는 업데이트 하지 않는다.(미리 전처리했기 때문)
                await insert_data(
                    session, conn, initial_insert, past_new_time, new_time
                )

                # 최초삽입시에는 month_past_time 1년1개월 전으로 설정. 그 이후에는 적재 전 db의 최신시간 에서 1달전으로 설정(30일 이동평균선의 계산을 위해)
                # minutes + 5 를 하는 이유는 작업 직전 db의 최신시간 이후부터 작업해야하기 때문(이 부분부터 포함해서 current_time_dt 까지 조회하기위해)
                if initial_insert:
                    month_past_time = (
                        datetime.fromisoformat(new_time)
                        - relativedelta(days=(365 + 30), minutes=-5)
                    ).isoformat()
                else:
                    month_past_time = (
                        datetime.fromisoformat(past_new_time)
                        - relativedelta(days=30, minutes=-5)
                    ).isoformat()

                # 이동평균선, 지수이동평균선, RSI, label feature 추가
                await add_moving_average(session, month_past_time, current_time_dt)
                # await add_ema(conn, past_time, current_time_dt)
                await add_rsi(session, month_past_time, current_time_dt)

                # rsi_state 테이블들 시퀀스 설정
                await session.execute(
                    text(
                        "ALTER TABLE rsi_75_state ALTER COLUMN id SET DEFAULT nextval('rsi_75_state_id_seq')"
                    )
                )
                await session.execute(
                    text(
                        "ALTER TABLE rsi_25_state ALTER COLUMN id SET DEFAULT nextval('rsi_25_state_id_seq')"
                    )
                )
                await session.commit()

                if initial_insert:
                    await add_rsi_over(session, month_past_time, current_time_dt)
                else:
                    await update_rsi_state_and_data(
                        session, past_new_time, current_time_dt
                    )
                await update_labels(session, month_past_time, current_time_dt)

                # 1년이 지난 데이터 삭제
                await delete_old_data(session)

                # 스케일링 작업(필요시 사용)
                # 최초삽입시 past_time을 다시 1년전으로 재조정. 이 시점에서는 btc_preprocessed는 현재기준 딱 1년치의 데이터를 보유중이기 때문
                # past_time = (
                #     datetime.fromisoformat(new_time)
                #     - relativedelta(days=365, minutes=-5)
                # ).isoformat()
                # if initial_insert:
                #     await standard_scale_data(
                #         session, past_time, current_time_dt
                #     )
                # else:
                #     await standard_scale_data(session, past_new_time, current_time_dt)

                # 삽입된 데이터를 시간순으로 정렬
                await session.execute(
                    select(BtcPreprocessed).order_by(BtcPreprocessed.time)
                )
                await session.commit()
                logger.info("Sorting btc_preprocessed table by time")
                count_final = await session.scalar(
                    select(func.count()).select_from(BtcPreprocessed)
                )
                logger.info(f"Final Count of rows in btc_preprocessed: {count_final}")
            await conn.commit()
            logger.info("Features added and updated successfully for missing times.")
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
    # preprocess task의 속도, 메모리, cpu, 테스트를 위한코드
    process = psutil.Process()
    initial_memory = process.memory_info().rss
    initial_cpu = process.cpu_percent(interval=None)

    asyncio.run(preprocess_data(async_context))

    final_memory = process.memory_info().rss
    final_cpu = process.cpu_percent(interval=None)
    memory_usage = final_memory - initial_memory
    cpu_usage = final_cpu - initial_cpu
    logger.info(
        f"Memory usage: {memory_usage / (1024 * 1024):.2f} MB, CPU usage: {cpu_usage:.2f}%"
    )
    e = time.time()
    es = e - s
    logger.info(f"Total working time : {es:.4f} sec")
