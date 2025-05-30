import os
import sys

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME")
sys.path.append(AIRFLOW_HOME)

from project_functions.python.minio_client import get_minio_client
from project_functions.python.clickhouse_hook import ClickHouseHook
from project_functions.python.clickhouse_crud import create_clickhouse_table
from airflow.triggers import TriggerRule
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator

minio_client = get_minio_client()
clickhouse_hook = ClickHouseHook()

with dag("create_table_into_clickhouse", schedule_interval=None, catchup=False) as dag:
    @task
    def create_table(sql_file_name: str, table_name: str):
        sql_file_path = os.path.join(AIRFLOW_HOME, "project_functions/python/sql", sql_file_name)
        create_clickhouse_table(sql_file_path, table_name)

    start_task = EmptyOperator(task_id="start_create_table_into_clickhouse")
    create_table_raw_weather_task = create_table("create_weather_table.sql", "raw_weather")
    create_table_raw_depcode_task = create_table("create_location_table.sql", "raw_depcode")
    end_task = EmptyOperator(
        task_id="end_create_table_into_clickhouse",
        trigger_rule=TriggerRule.ALL_SUCCESS  # Ensure this task runs only if all previous tasks succeed
    )

    start_task >> [create_table_raw_weather_task, create_table_raw_depcode_task] >> end_task
