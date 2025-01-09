FROM python:3.8.14-slim

ARG DEBIAN_FRONTEND=noninteractive

ENV PYTHONUNBUFFERED=1

ENV AIRFLOW_HOME=/app/airflow

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR $AIRFLOW_HOME

COPY scripts scripts
COPY pyproject.toml poetry.lock ./

RUN chmod +x scripts/entrypoint.sh
RUN chmod +x scripts/init_connections.sh

RUN pip3 install --upgrade --no-cache-dir pip \
    && pip3 install poetry \
    && poetry install --only main
