import os
import sys

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME")
sys.path.append(AIRFLOW_HOME)

from project_functions.python.minio_client import get_minio_client
from project_functions.python.minio_utils import MinioUtils
from project_functions.python.clickhouse_hook import ClickHouseHook
from project_functions.python.clickhouse_crud import ClickHouseQueries

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.triggers import TriggerRule


import pendulum
minio_client = get_minio_client()
clickhouse_hook = ClickHouseHook()
object_name = "weather/staging/daily/france_weather_{{ ds }}.parquet"

with DAG(
    dag_id="load_data_from_datalake_to_clickhouse",
    schedule_interval=None,
    start_date=pendulum.datetime(2025, 7, 19, tz="UTC"),
    catchup=False
    ) as dag:
    # retrieve data from MinIO daily directory
    retrieving_data_task = PythonOperator(
        task_id="retrieve_daily_data",
        python_callable=MinioUtils(bucket_name="weather").retrieve_data,
        op_kwargs={"object_name": object_name}
    )
    storing_data_task = PythonOperator(
        task_id="store_data",
        python_callable=ClickHouseQueries().load_data_to_clickhouse,
        op_kwargs={"table_name": "raw_weather", "data": retrieving_data_task.output, "is_to_truncate": True} # retrieve retrieved_data_task output since tthis task returns a DataFrame
    )
    end_task = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_SUCCESS)  # Ensure this task runs only if all previous tasks succeed

    retrieving_data_task >> storing_data_task >> end_task
