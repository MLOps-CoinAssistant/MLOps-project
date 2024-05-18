from airflow.providers.postgres.hooks.postgres import PostgresHook
from dags.module.info.connections import AirflowConnections
from dags.module.info.api import APIInformation
from dags.module.info.coins import Coins
from sqlalchemy import create_engine, MetaData, Table, Column, String, Integer, Text
from sqlalchemy.orm import sessionmaker
from datetime import datetime, timedelta
import requests
import hashlib

# 뉴스 테이블 정의
metadata = MetaData()

news_table = Table(
    "btc_news",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("title", String, nullable=True),
    Column("source", String, nullable=False),
    Column("content", Text, nullable=False),
    Column("coin_type", String, nullable=True),
    Column("pos_neg", String, nullable=True),
    Column("published_on", Integer, nullable=False),  # 뉴스가 발행된 타임스탬프
    Column("hash", String, nullable=False),  # 중복 여부를 판단할 해시 값
)


def get_postgres_engine(**context):
    postgres_hook = PostgresHook(postgres_conn_id=AirflowConnections.DEVELOPMENT.value)
    engine = create_engine(postgres_hook.get_uri())
    return engine


def create_database_if_not_exists(**context):
    engine = get_postgres_engine()
    with engine.connect() as conn:
        if not engine.dialect.has_table(conn, "btc_news"):
            metadata.create_all(engine)


def hash_news(title, source, content):
    hash_object = hashlib.sha256()
    hash_object.update(f"{title}{source}{content}".encode("utf-8"))
    return hash_object.hexdigest()


def fetch_btc_news(min_news_count=100, **context):
    current_time = datetime.now()
    news_data = []
    hours_ago = 1

    while (
        len([news for news in news_data if news["coin_type"] == "BTC"]) < min_news_count
    ):
        past_time = current_time - timedelta(hours=hours_ago)
        url = APIInformation.NEWS_API_URL.value
        response = requests.get(url)
        data = response.json()
        news_list = data["Data"]

        for news in news_list:
            news_time = datetime.fromtimestamp(news["published_on"])
            if news_time > past_time:
                coin_type = None
                for coin in Coins.list():
                    if coin in news["body"]:
                        coin_type = coin
                        break
                if coin_type == "BTC":
                    news_hash = hash_news(news["title"], news["source"], news["body"])
                    news_data.append(
                        {
                            "title": news["title"],
                            "source": news["source"],
                            "content": news["body"],
                            "coin_type": coin_type,
                            "pos_neg": "neutral",  # 기본 값 설정 (추후에 감정 분석 추가 가능)
                            "published_on": news[
                                "published_on"
                            ],  # 뉴스가 발행된 타임스탬프 추가
                            "hash": news_hash,  # 해시 값 추가
                        }
                    )
        hours_ago *= 2  # 시간을 두 배로 늘려가며 계속 검색

        if len(news_data) >= 1000:  # 상한선을 두어 무한 루프 방지
            break

    return news_data


def clear_and_save_news(**context):
    news_data = fetch_btc_news(min_news_count=100)

    if not news_data:
        print("No BTC news found.")
        return

    engine = get_postgres_engine()
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        # 12시간이 지난 기사는 삭제
        twelve_hours_ago = datetime.now() - timedelta(hours=12)
        try:
            session.execute(
                news_table.delete().where(
                    news_table.c.published_on < twelve_hours_ago.timestamp()
                )
            )
            session.commit()
        except OverflowError:
            pass

        # 데이터베이스에 이미 존재하는 기사 해시 확인
        existing_hashes = set(row[0] for row in session.query(news_table.c.hash).all())
        new_news_data = [
            news for news in news_data if news["hash"] not in existing_hashes
        ]

        if new_news_data:
            session.execute(news_table.insert(), new_news_data)
            session.commit()
            print(f"{len(new_news_data)} new BTC news articles inserted.")
        else:
            print("No new BTC news articles to insert.")
    except Exception as e:
        session.rollback()
        print(f"Error occurred: {e}")
    finally:
        session.close()
