#!/usr/bin/env bash

airflow db migrate

airflow users create -r Admin -u admin -p admin -e donatien.konan.pro@gmail.com -f admin -l airflow

scripts/init_connections.sh

airflow webserver
