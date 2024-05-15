from sqlalchemy import create_engine, Column, DateTime
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


# PostgreSQL 연결 정보를 환경 변수에서 가져옵니다.
postgres_conn_str = os.environ.get("AIRFLOW__CORE__SQL_ALCHEMY_CONN")

# 데이터베이스 연결 엔진을 생성합니다.
engine = create_engine(postgres_conn_str)

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
        count=168,
    )
    return df


# 데이터를 수집하고 데이터베이스에 적재하는 함수를 정의합니다.
def collect_and_load_data():
    end_date = datetime.now()  # 현재 시간을 종료일로 설정합니다.
    start_date = end_date - timedelta(
        days=7
    )  # 최초 실행 시에는 일주일치 데이터를 가져옵니다.

    # 데이터베이스 세션을 생성합니다.
    session = Session()

    # 최초 적재 여부를 확인합니다.
    if not session.query(BtcOhlc).first():
        # 최초 적재인 경우, 일주일치 데이터를 가져오기 위해 아무 작업도 하지 않습니다.
        pass
    else:
        # 이후 적재인 경우, 마지막으로 적재된 데이터의 시간을 기준으로 현재 시간까지의 데이터를 가져옵니다.
        last_loaded_time = session.query(func.max(BtcOhlc.time)).scalar()
        start_date = max(start_date, last_loaded_time)

    # 세션을 닫습니다.
    session.close()

    # 중복을 제외한 데이터를 수집합니다.
    df = get_hourly_data(start_date, end_date)

    if not df.empty:
        # 데이터프레임을 리스트 형태로 변환합니다.
        data_list = df.values.tolist()
        # 데이터베이스에 데이터를 직접 삽입합니다.
        conn = engine.connect()
        conn.execute(BtcOhlc.__table__.insert(), data_list)
        conn.close()
    else:
        print("No data retrieved.")
