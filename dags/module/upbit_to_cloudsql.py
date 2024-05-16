import asyncio
import aiohttp
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, DateTime, Float, MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.future import select
from sqlalchemy.sql import func
from datetime import datetime, timedelta
import os

# UPbit의 access key와 secret key를 환경 변수에서 가져옵니다.
UPBIT_ACCESS_KEY = os.getenv("UPBIT_ACCESS_KEY")
UPBIT_SECRET_KEY = os.getenv("UPBIT_SECRET_KEY")
# print(f"UPBIT_ACCESS_KEY: {UPBIT_ACCESS_KEY}")
# print(f"UPBIT_SECRET_KEY: {UPBIT_SECRET_KEY}")

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

if not postgres_conn_str:
    raise ValueError(
        "Postgres connection string is not set in the environment variables."
    )

# 데이터베이스 연결 엔진을 생성합니다.
engine = create_async_engine(postgres_conn_str, echo=True)

# 데이터베이스 세션을 생성합니다.
async_session = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)


# 비동기적으로 Upbit API를 사용하여 비트코인 시세 데이터를 가져오는 함수를 정의합니다.
async def fetch_ohlcv_data(session, market, to, count):
    url = f"https://api.upbit.com/v1/candles/minutes/60?market={market}&to={to}&count={count}"
    headers = {"Accept": "application/json"}
    async with session.get(url, headers=headers) as response:
        return await response.json()


# 비동기적으로 1년치 데이터를 여러 번 요청하여 가져오는 함수
async def fetch_data(start_date, end_date):
    market = "KRW-BTC"  # 비트코인 시세 데이터를 가져올 시장 (KRW-BTC)
    data = []
    async with aiohttp.ClientSession() as session:
        while start_date < end_date:
            to_date = end_date.strftime("%Y-%m-%dT%H:%M:%S")
            new_data = await fetch_ohlcv_data(session, market, to_date, 200)
            if not new_data:
                break
            data.extend(new_data)
            end_date = datetime.strptime(
                new_data[-1]["candle_date_time_utc"], "%Y-%m-%dT%H:%M:%S"
            ) - timedelta(minutes=60)
    return data


# 비동기적으로 데이터를 수집하고 데이터베이스에 적재하는 함수
async def collect_and_load_data():
    async with async_session() as session:
        async with session.begin():
            metadata = MetaData()
            if not engine.dialect.has_table(engine, "btc_ohlc"):
                Base.metadata.create_all(bind=engine)

            try:
                end_date = datetime.now()  # 현재 시간을 종료일로 설정합니다.
                start_date = end_date - timedelta(
                    days=365
                )  # 최초 실행 시에는 1년치 데이터를 가져옵니다.

                # 중복된 데이터를 방지하기 위해 마지막으로 적재된 데이터의 시간을 확인합니다.
                result = await session.execute(select([func.max(BtcOhlc.time)]))
                last_loaded_time = result.scalar()

                if last_loaded_time:
                    start_date = max(
                        start_date, last_loaded_time + timedelta(minutes=60)
                    )

                # 데이터를 가져옵니다.
                data = await fetch_data(start_date, end_date)

                if data:
                    # 데이터베이스에 데이터를 대량 삽입합니다.
                    values = [
                        BtcOhlc(
                            time=datetime.strptime(
                                item["candle_date_time_kst"], "%Y-%m-%dT%H:%M:%S"
                            ),
                            open=item["opening_price"],
                            high=item["high_price"],
                            low=item["low_price"],
                            close=item["trade_price"],
                            volume=item["candle_acc_trade_volume"],
                        )
                        for item in data
                    ]
                    session.add_all(values)
                    await session.commit()
                    print("Data successfully inserted into database.")
                else:
                    print("No data retrieved.")
            except Exception as e:
                print(f"An error occurred: {e}")


# 직접 실행 시 데이터를 수집하고 데이터베이스에 적재합니다.

asyncio.run(collect_and_load_data())
