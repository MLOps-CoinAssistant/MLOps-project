from sqlalchemy import create_engine, Column, DateTime, Float, MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func
from datetime import datetime, timedelta
import pyupbit
import os

# UPbit의 access key와 secret key를 환경 변수에서 가져옵니다.
UPBIT_ACCESS_KEY = os.environ.get("UPBIT_ACCESS_KEY")
UPBIT_SECRET_KEY = os.environ.get("UPBIT_SECRET_KEY")

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

# 데이터베이스 연결 엔진을 생성합니다.
engine = create_engine(postgres_conn_str)

# 기존 테이블이 존재하는지 확인하기 위해 MetaData 객체를 생성합니다.
metadata = MetaData(engine)

# 데이터베이스에 테이블이 없는 경우에만 테이블을 생성합니다.
if not engine.dialect.has_table(engine, "btc_ohlc"):
    # 데이터베이스 테이블을 생성합니다.
    Base.metadata.create_all(engine)

# 데이터베이스 세션을 생성합니다.
Session = sessionmaker(bind=engine)


# PyUpbit을 사용하여 비트코인 시세 데이터를 가져오는 함수를 정의합니다.
def get_hourly_data(start_date, end_date):
    # PyUpbit 클라이언트를 생성합니다.
    upbit = pyupbit.Upbit(UPBIT_ACCESS_KEY, UPBIT_SECRET_KEY)
    # 비트코인(KRW-BTC)의 시간별로 OHLCV(Open, High, Low, Close, Volume) 데이터를 가져옵니다.
    df = upbit.get_ohlcv(
        "KRW-BTC",
        interval="minute60",
        to=end_date.strftime("%Y-%m-%d %H:%M:%S"),
        count=168,  # 일주일 = 168시간
    )
    return df


# 데이터를 수집하고 데이터베이스에 적재하는 함수를 정의합니다.
def collect_and_load_data():
    try:
        end_date = datetime.now()  # 현재 시간을 종료일로 설정합니다.
        start_date = end_date - timedelta(
            days=7
        )  # 최초 실행 시에는 일주일치 데이터를 가져옵니다.

        # 중복된 데이터를 방지하기 위해 마지막으로 적재된 데이터의 시간을 확인합니다.
        session = Session()
        last_loaded_time = session.query(func.max(BtcOhlc.time)).scalar()

        # 이미 적재된 데이터의 다음 시간부터 데이터를 가져옵니다.
        if last_loaded_time:
            start_date = max(start_date, last_loaded_time)

        # 데이터를 가져옵니다.
        df = get_hourly_data(start_date, end_date)

        if not df.empty:
            # 데이터베이스에 데이터를 삽입합니다.
            for index, row in df.iterrows():
                btc_ohlc_data = BtcOhlc(
                    time=index,
                    open=row["open"],
                    high=row["high"],
                    low=row["low"],
                    close=row["close"],
                    volume=row["volume"],
                )
                session.add(btc_ohlc_data)
            session.commit()
            print("Data successfully inserted into database.")
        else:
            print("No data retrieved.")
    except Exception as e:
        # 예외가 발생한 경우 로그를 출력합니다.
        print(f"An error occurred: {e}")
    finally:
        session.close()


# 데이터를 수집하고 데이터베이스에 적재합니다.
collect_and_load_data()
