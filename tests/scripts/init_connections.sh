#!/usr/bin/env bash
set -x

echo "Add connection"
airflow connections add 'sqlite_connection' \
                    --conn-type sqlite \
                    --conn-host "$PWD/tests/temp/airflow.db"

airflow variables set LAKE_BUCKET $LAKE_BUCKET
airflow variables set LAKE_BUCKET_2 $LAKE_BUCKET_2
airflow variables set GCP_SERVICE_ACCOUNT_KEY_PATH $PATH_TO_SERVICE_ACCOUNT_DIRECTORY/weather-team-1923ba894f08.json
