#!/usr/bin/env bash

airflow db upgrade

airflow users create -r Admin -u admin -p admin -e donatien.konan.pro@gmail.com -f admin -l airflow

airflow webserver
