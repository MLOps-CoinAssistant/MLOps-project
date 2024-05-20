import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook
from dags.module.info.connections import AirflowConnections
from dags.module.info.api import APIInformation
from dags.module.info.coins import Coins
from sqlalchemy import (
    create_engine,
    MetaData,
    Table,
    Column,
    String,
    Integer,
    Text,
    DateTime,
)
from sqlalchemy.orm import sessionmaker
from datetime import datetime, timedelta
import requests
import hashlib
from tenacity import retry, stop_after_attempt, wait_fixed
from requests.exceptions import HTTPError
from airflow.models import Variable


# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 뉴스 테이블 정의
metadata = MetaData()

news_table = Table(
    "btc_news",
    metadata,
    Column("id", String, primary_key=True),
    Column("title", String, nullable=True),
    Column("source", String, nullable=False),
    Column("content", Text, nullable=False),
    Column("coin_type", String, nullable=True),
    Column("pos_neg", String, nullable=True),
    Column(
        "created_at", DateTime, nullable=False, default=datetime.utcnow
    ),  # 기사 생성 시간 컬럼 추가
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
            logger.info("btc_news table created.")


def hash_news(title, content):
    hash_object = hashlib.sha256()
    hash_object.update(f"{title}{content}".encode("utf-8"))
    return hash_object.hexdigest()


@retry(stop=stop_after_attempt(3), wait=wait_fixed(10))
def fetch_news_from_api(api_url, process_news_function):
    try:
        response = requests.get(api_url)
        response.raise_for_status()  # HTTP 오류가 발생하면 예외를 발생시킵니다.
    except HTTPError as http_err:
        logger.error(
            f"HTTP error occurred: {http_err}"
        )  # HTTP 오류 메시지를 로그에 기록합니다.
        raise
    except Exception as err:
        logger.error(
            f"Other error occurred: {err}"
        )  # 다른 오류 메시지를 로그에 기록합니다.
        raise
    return process_news_function(response.json())


def process_cryptocompare_news(json_response):
    return json_response["Data"]


def process_newsapi_news(json_response):
    return json_response["articles"]


def process_gdelt_news(json_response):
    return json_response["articles"]


def process_coindesk_news(json_response):
    return json_response["items"]


def fetch_btc_news(min_news_count=100, **context):
    news_data = []
    existing_hashes = set()

    engine = get_postgres_engine()
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        existing_hashes = set(row[0] for row in session.query(news_table.c.id).all())

        # Cryptocompare
        cryptocompare_news = fetch_news_from_api(
            APIInformation.CRYPTOCOMPARE_API_URL.value, process_cryptocompare_news
        )
        news_data.extend(parse_news_data(cryptocompare_news, existing_hashes))

        # NewsAPI
        newsapi_news = fetch_news_from_api(
            APIInformation.NEWSAPI_API_URL.value, process_newsapi_news
        )
        news_data.extend(parse_news_data(newsapi_news, existing_hashes))

        # GDELT
        gdelt_news = fetch_news_from_api(
            APIInformation.GDELT_API_URL.value, process_gdelt_news
        )
        news_data.extend(parse_news_data(gdelt_news, existing_hashes))

        # CoinDesk
        coindesk_news = fetch_news_from_api(
            APIInformation.COINDESK_API_URL.value, process_coindesk_news
        )
        news_data.extend(parse_news_data(coindesk_news, existing_hashes))

    except requests.exceptions.RequestException as e:
        logger.error(f"Error occurred while fetching news: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error occurred: {e}")
    finally:
        session.close()

    return news_data


def parse_news_data(news_list, existing_hashes):
    parsed_news = []
    for news in news_list:
        title = news.get("title", news.get("title_text", ""))
        content = news.get("body", news.get("description", ""))
        if not content:  # content가 비어있으면 건너뛰기
            continue

        source = news.get("source", news.get("source_name", "unknown"))
        if isinstance(source, dict):
            source = source.get("title", "unknown")

        news_hash = hash_news(title, content)
        if news_hash not in existing_hashes:
            parsed_news.append(
                {
                    "id": news_hash,
                    "title": title,
                    "source": source,
                    "content": content,
                    "coin_type": "BTC",
                    "pos_neg": "neutral",
                    "created_at": datetime.utcnow(),  # 현재 시간을 생성 시간으로 추가
                }
            )
            existing_hashes.add(news_hash)
    return parsed_news


def clear_and_save_news(**context):
    news_data = fetch_btc_news(min_news_count=100)

    if not news_data:
        logger.info("No BTC news found.")
        return

    engine = get_postgres_engine()
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        # 7일이 지난 기사는 삭제
        seven_days_ago = datetime.utcnow() - timedelta(days=7)
        try:
            session.execute(
                news_table.delete().where(news_table.c.created_at < seven_days_ago)
            )
            session.commit()
            logger.info("Old news articles deleted.")
        except OverflowError as e:
            logger.error(f"Error during deletion of old news: {e}")
            session.rollback()

        existing_hashes = set(row[0] for row in session.query(news_table.c.id).all())
        new_news_data = [
            news for news in news_data if news["id"] not in existing_hashes
        ]

        if new_news_data:
            for news in new_news_data:
                try:
                    session.execute(news_table.insert().values(news))
                    session.commit()
                    logger.info(f"Inserted news: {news['id']}")
                except Exception as e:
                    logger.error(f"Error inserting news {news['id']}: {e}")
                    session.rollback()
            logger.info(f"{len(new_news_data)} new BTC news articles inserted.")
        else:
            logger.info("No new BTC news articles to insert.")
    except Exception as e:
        session.rollback()
        logger.error(f"Error occurred: {e}")
    finally:
        session.close()
