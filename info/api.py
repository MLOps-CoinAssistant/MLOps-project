from enum import Enum
from airflow.models import Variable


class APIInformation(Enum):
    CRYPTOCOMPARE_API_URL = (
        "https://min-api.cryptocompare.com/data/v2/news/?lang=EN&categories=Bitcoin"
    )
    GDELT_API_URL = (
        "https://api.gdeltproject.org/api/v2/doc/doc?query=Bitcoin&format=json"
    )
    COINDESK_API_URL = "https://www.coindesk.com/arc/outboundfeeds/rss/?outputType=json"
    NAVER_NEWS_API_URL = "https://openapi.naver.com/v1/search/news.json"

    UPBIT_ACCESS_API_KEY = Variable.get("UPBIT_ACCESS_API_KEY")
    UPBIT_SECRET_API_KEY = Variable.get("UPBIT_SECRET_API_KEY")
    OPENAI_API_KEY = Variable.get("OPENAI_API_KEY")


# URL 템플릿을 반환하는 함수 추가
def get_upbit_api_url(market: str, to: str, count: int) -> str:
    return f"https://api.upbit.com/v1/candles/minutes/60?market={market}&to={to}&count={count}"
