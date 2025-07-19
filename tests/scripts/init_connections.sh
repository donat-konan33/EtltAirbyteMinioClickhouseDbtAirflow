#!/usr/bin/env bash
set -x

echo "Add connection"
airflow connections add 'sqlite_connection' \
                    --conn-type sqlite \
                    --conn-host "$PWD/tests/temp/airflow.db"

airflow variables set LAKE_BUCKET $LAKE_BUCKET
airflow variables set LAKE_BUCKET_2 $LAKE_BUCKET_2
