#!/usr/bin/env bash

airflow db migrate

airflow users create -r Admin -u $AIRLFLOW_ADMIN_USERNAME -p $AIRFLOW_ADMIN_PASSWORD -e $AIRFLOW_ADMIN_EMAIL -f $AIRFLOW_ADMIN_FIRST_NAME -l $AIRFLOW_ADMIN_LAST_NAME

scripts/init_connections.sh

airflow variables set LAKE_BUCKET $LAKE_BUCKET
airflow variables set LAKE_BUCKET_2 $LAKE_BUCKET_2

airflow webserver
