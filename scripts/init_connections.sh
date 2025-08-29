#!/usr/bin/env bash
set -x

echo "Add connections"
airflow connections add 'postgres_connection' \
    --conn-type postgres \
    --conn-host "$POSTGRES_HOST" \
    --conn-schema "$POSTGRES_DB" \
    --conn-login "$POSTGRES_USER" \
    --conn-password "$POSTGRES_PASSWORD" \
    --conn-port "5432"

airflow connections add 'clickhouse_default' \
    --conn-type 'clickhouse' \
    --conn-host "$CLICKHOUSE_HOST" \
    --conn-port "8123" \
    --conn-schema "$CLICKHOUSE_DB" \
    --conn-login "$CLICKHOUSE_USER" \
    --conn-password "$CLICKHOUSE_PASSWORD" \
#    --conn-extra '{"protocol": "https", "verify": false}'
