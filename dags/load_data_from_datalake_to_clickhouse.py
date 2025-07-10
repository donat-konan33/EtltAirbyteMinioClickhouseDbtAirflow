import os
import sys

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME")
sys.path.append(AIRFLOW_HOME)

from project_functions.python.minio_client import get_minio_client
from project_functions.python.clickhouse_hook import ClickHouseHook
from airflow import DAG
import pendulum
minio_client = get_minio_client()
clickhouse_hook = ClickHouseHook()


with DAG(
    dag_id="load_data_from_datalake_to_clickhouse",
    schedule_interval=None,
    start_date=pendulum.datetime(2025, 7, 10, tz="UTC"),
    catchup=False
    ) as dag:
    pass
