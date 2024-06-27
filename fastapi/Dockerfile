FROM python:3.11.8-slim

WORKDIR /

RUN pip install poetry

COPY ./poetry.lock ./poetry.lock
COPY ./pyproject.toml ./pyproject.toml

RUN poetry install --no-root

COPY ./app /app
COPY ./entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENV UVICORN_PORT=8000

ENTRYPOINT ["bash", "/entrypoint.sh"]
