# import jwt    # PyJWT
# import uuid
# import hashlib
# import requests
# from urllib.parse import urlencode

# # query는 dict 타입입니다.
# access_key = "BxX2huDxGlomxwD5JcIvmS7Z0EYgBUOHmZhxG8pW"
# secret_key = "6s8aDeq7UAQDjV8SjcA8HTCaWd3CHHkhlTtFffwp"
# payload = {
#     'access_key': access_key,
#     'nonce': str(uuid.uuid4())
# }
# jwt_token = jwt.encode(payload, secret_key, algorithm='HS256')
# print(f'jwt_token: {jwt_token}')
# authorization_token = f'Bearer {jwt_token}'
# print(jwt_token)
# url = "https://api.upbit.com/v1/candles/minutes/60?market=KRW-BTC&count=10"
# headers = {
#     "Accept": "application/json",
#     "Authorization": authorization_token
# }
# response = requests.get(url, headers=headers)
# print(response)
# data = response.json()
# print(data)
