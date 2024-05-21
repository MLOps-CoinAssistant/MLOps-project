import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook
from dags.module.info.connections import Connections
from dags.module.info.api import APIInformation
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
from typing import Callable, Dict, List, Set, Optional
from sqlalchemy.engine import Engine, Connection


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
    Column("created_at", DateTime, nullable=False, default=datetime.utcnow),
)


def get_postgres_engine(**context: Dict) -> Engine:
    postgres_hook = PostgresHook(postgres_conn_id=Connections.POSTGRES_DEFAULT.value)
    engine = create_engine(postgres_hook.get_uri())
    return engine


def create_database_if_not_exists(**context: Dict) -> None:
    engine = get_postgres_engine()
    with engine.connect() as conn:
        if not engine.dialect.has_table(conn, "btc_news"):
            metadata.create_all(engine)
            logger.info("btc_news table created.")


def hash_news(title: str, content: str) -> str:
    hash_object = hashlib.sha256()
    hash_object.update(f"{title}{content}".encode("utf-8"))
    return hash_object.hexdigest()


@retry(stop=stop_after_attempt(3), wait=wait_fixed(10))
def fetch_news_from_api(
    api_url: str, process_news_function: Callable[[Dict], List[Dict]]
) -> List[Dict]:
    try:
        response: requests.Response = requests.get(api_url)
        response.raise_for_status()
    except HTTPError as http_err:
        logger.error(f"HTTP error occurred: {http_err}")
        raise
    except Exception as err:
        logger.error(f"Other error occurred: {err}")
        raise
    return process_news_function(response.json())


def process_cryptocompare_news(json_response: Dict) -> List[Dict]:
    return json_response["Data"]


def process_newsapi_news(json_response: Dict) -> List[Dict]:
    return json_response["articles"]


def process_gdelt_news(json_response: Dict) -> List[Dict]:
    return json_response["articles"]


def process_coindesk_news(json_response: Dict) -> List[Dict]:
    return json_response["items"]


def fetch_btc_news(min_news_count: int = 100, **context: Dict) -> List[Dict]:
    news_data: List[Dict] = []
    existing_hashes: Set[str] = set()

    engine: Engine = get_postgres_engine()
    Session: sessionmaker = sessionmaker(bind=engine)
    session = Session()

    try:
        existing_hashes: Dict[str, List[Dict]] = set(
            row[0] for row in session.query(news_table.c.id).all()
        )

        cryptocompare_news: List[Dict] = fetch_news_from_api(
            APIInformation.CRYPTOCOMPARE_API_URL.value, process_cryptocompare_news
        )
        news_data.extend(parse_news_data(cryptocompare_news, existing_hashes))

        newsapi_news: List[Dict] = fetch_news_from_api(
            APIInformation.NEWSAPI_API_URL.value, process_newsapi_news
        )
        news_data.extend(parse_news_data(newsapi_news, existing_hashes))

        gdelt_news: List[Dict] = fetch_news_from_api(
            APIInformation.GDELT_API_URL.value, process_gdelt_news
        )
        news_data.extend(parse_news_data(gdelt_news, existing_hashes))

        coindesk_news: List[Dict] = fetch_news_from_api(
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


def parse_news_data(news_list: List[Dict], existing_hashes: Set[str]) -> List[Dict]:
    parsed_news: List[Dict] = []
    for news in news_list:
        title: str = news.get("title", news.get("title_text", ""))
        content: str = news.get("body", news.get("description", ""))
        if not content:
            continue

        source: str = news.get("source", news.get("source_name", "unknown"))
        if isinstance(source, dict):
            source = source.get("title", "unknown")

        news_hash: str = hash_news(title, content)
        if news_hash not in existing_hashes:
            parsed_news.append(
                {
                    "id": news_hash,
                    "title": title,
                    "source": source,
                    "content": content,
                    "coin_type": "BTC",
                    "pos_neg": "neutral",
                    "created_at": datetime.utcnow(),
                }
            )
            existing_hashes.add(news_hash)
    return parsed_news


def clear_and_save_news(**context: Dict) -> None:
    news_data: List[Dict] = fetch_btc_news(min_news_count=100)

    if not news_data:
        logger.info("No BTC news found.")
        return

    engine: Engine = get_postgres_engine()
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
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

        existing_hashes = set(
            row[0] for row in session.query(news_table.c.id).all()
        )  # 타입 힌트:
        new_news_data = [
            news for news in news_data if news["id"] not in existing_hashes
        ]  # 타입 힌트:

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
