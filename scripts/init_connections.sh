#!/usr/bin/env bash
set -x

echo "Add connection"
airflow connections add 'postgres_connection' \
                    --conn-type postgres \
                    --conn-host "$HOST" \
                    --conn-schema "$POSTGRES_DB" \
                    --conn-login "$POSTGRES_USER" \
                    --conn-password "$POSTGRES_PASSWORD" \
                    --conn-port "5432"

# we will create now a new connection for airbyte
echo "Add connection"
airflow connections add 'airbyte_connection' \
                    --conn-type http \
                    --conn-host "$AIRBYTE_URL" \
                    --conn-login "$AIRBYTE_USER" \
                    --conn-password "$AIRBYTE_PASSWORD" \
                    --conn-port "8001"
