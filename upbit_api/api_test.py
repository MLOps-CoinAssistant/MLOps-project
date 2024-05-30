#!/usr/bin/env python
# coding: utf-8

# In[2]:


get_ipython().system("pip install pyupbit")


# In[3]:


import pyupbit


# In[4]:


### 거래할 코인 symbol
coin = "KRW-BTC"


# In[5]:


### API 키 파일 참조
with open("key.txt") as f:
    access_key, secret_key = [line.strip() for line in f.readlines()]


# In[8]:


### 업비트 연동
upbit = pyupbit.Upbit(access_key, secret_key)


# In[9]:


# 현재가격 조회
def get_cur_price(ticker):
    return pyupbit.get_current_price(ticker)


# In[10]:


print(get_cur_price(coin))


# In[11]:


import pyupbit
import datetime

# API 키 파일 참조
with open("key.txt") as f:
    access_key, secret_key = [line.strip() for line in f.readlines()]

# 업비트 연동
upbit = pyupbit.Upbit(access_key, secret_key)

# 현재 날짜를 기준으로 1년 전 날짜 계산
end_date = datetime.datetime.now()
start_date = end_date - datetime.timedelta(days=365)

# 일봉 데이터 가져오기 (예: BTC)
ticker = "KRW-BTC"  # 원화 비트코인 마켓
df = pyupbit.get_ohlcv(
    ticker, interval="day", to=end_date.strftime("%Y-%m-%d"), count=365
)

# 데이터 프레임 출력
print(df)


# In[12]:


import pyupbit
import datetime
import pandas as pd

# API 키 파일 참조
with open("key.txt") as f:
    access_key, secret_key = [line.strip() for line in f.readlines()]

# 업비트 연동
upbit = pyupbit.Upbit(access_key, secret_key)

# 데이터 수집 기간 설정
days_to_load = 7  # 7일치 데이터를 불러오는 예제
end_date = datetime.datetime.now()
start_date = end_date - datetime.timedelta(days=days_to_load)


# 60분 간격 데이터 수집 함수 정의
def get_hourly_data(ticker, start_date, end_date):
    df_list = []
    current_date = start_date
    while current_date < end_date:
        df = pyupbit.get_ohlcv(
            ticker,
            interval="minute60",
            to=current_date.strftime("%Y-%m-%d %H:%M:%S"),
            count=24,
        )
        if df is not None:
            df_list.append(df)
            current_date += datetime.timedelta(days=1)
        else:
            break
    if df_list:
        return pd.concat(df_list)
    return pd.DataFrame()


# 일봉 데이터 가져오기 (예: BTC)
ticker = "KRW-BTC"  # 원화 비트코인 마켓
df = get_hourly_data(ticker, start_date, end_date)

# 데이터 프레임 출력
print(df)


# In[ ]:
