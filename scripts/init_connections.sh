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
