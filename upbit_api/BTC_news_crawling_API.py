#!/usr/bin/env python
# coding: utf-8

# In[1]:


get_ipython().system("pip install requests schedule")


# In[3]:


import requests
from datetime import datetime, timedelta


def fetch_recent_btc_news():
    # 현재 시간과 1시간 전 시간 계산
    current_time = datetime.now()
    one_hour_ago = current_time - timedelta(hours=10)

    url = "https://min-api.cryptocompare.com/data/v2/news/?lang=EN&categories=Bitcoin"
    response = requests.get(url)
    data = response.json()
    news_list = data["Data"]
    recent_news = []

    # 1시간 이내의 뉴스만 필터링
    for news in news_list:
        news_time = datetime.fromtimestamp(news["published_on"])
        if news_time > one_hour_ago:
            recent_news.append(
                {
                    "title": news["title"],
                    "source": news["source"],
                    "body": news["body"],
                    "url": news["url"],
                }
            )
        if len(recent_news) >= 100:
            break

    # 뉴스가 없는 경우 None 반환
    if not recent_news:
        return None

    return recent_news


# 실행 예
btc_news = fetch_recent_btc_news()
if btc_news:
    for news in btc_news:
        print(f"Title: {news['title']}")
        print(f"Source: {news['source']}")
        print(f"Body: {news['body'][:100]}...")  # 첫 100자만 출력
        print(f"URL: {news['url']}")
        print("--------------------------------------------------")
else:
    print("No news found in the last hour.")


# In[5]:


# In[ ]:
