import os
import sys

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME")
sys.path.append(AIRFLOW_HOME)

from airflow import DAG
import pendulum
from project_functions.python.transform_airbyte_extract_data import get_and_store_data, delete_files_from_minio
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.empty import EmptyOperator
import asyncio
from project_functions.python.minio_utils import MinioUtils
import pandas as pd


bucket_name = "weather"
prefix = "raw/weatherdata/"
format_ = ".jsonl"

with DAG(
    dag_id="airbyte_data_transform",
    tags=["new_data"],
    default_args={'owner': 'python'},
    description="this one allow us to transform raw data extracted by airbyte before loading to Minio Datalake",
    start_date=pendulum.datetime(2025, 2, 7, tz="UTC"),
    schedule_interval="0 2 * * *",
    catchup=False
) as dag:
    wait_for_airbyte_data_sensor = ExternalTaskSensor(
        task_id="wait_for_airbyte_data",
        external_dag_id="http_sync_dag",
        external_task_id="all_done",
        mode="poke",
        poke_interval=60,
        timeout=3600,
        allowed_states=["success"]
    )

    transform_and_load_data = PythonOperator(
        task_id="transform_and_load_data",
        python_callable=get_and_store_data,
        op_kwargs={"bucket_name": bucket_name, "prefix": prefix, "format": format_}
    )

    delete_jsonl_files = PythonOperator(
        task_id="delete_jsonl_files",
        python_callable=delete_files_from_minio,
        op_kwargs={"bucket_name": bucket_name, "prefix": prefix, "format": format_},
    )

    transformations_done = EmptyOperator(
        task_id="transformations_done"
    )
    wait_for_airbyte_data_sensor >> transform_and_load_data >> delete_jsonl_files >> transformations_done
