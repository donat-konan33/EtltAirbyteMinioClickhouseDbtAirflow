from pathlib import Path
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.branch import BranchPythonOperator
import pendulum
from airflow.providers.postgres.operators import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.providers.airbyte.sensors.airbyte import AirbyteJobSensor
import json
from airflow.models import Variable

AIRFLOW_HOME = Variable.get("AIRFLOW_HOME")

POSTGRES_CONN_ID = "postgres_connection"
AIRBYTE_CONNECTION_ID = "airbyte_connection"

with open(f"{AIRFLOW_HOME}/airbyte/job_sync_id.json", "r") as file:
    job_sync_id = json.load(file)
    AIRBYTE_SYNC_JOBS_ID = job_sync_id.values()


with DAG(
    dag_id="airbyte_eltl_dag",
    tags=["airbyte-postgres"],
    default_args={'owner': 'airbyte-postgres'},
    schedule_interval="@daily",
    start_date=pendulum.datetime(2025, 1, 1),
    catchup=False,
) as dag:

    sql_file = f"{AIRFLOW_HOME}/project_functions/sql/insert_raw_data_into_weather_table.sql"
    with open(sql_file, 'r') as file:
        insert_raw_data_into_weather_table = file.read()

    # Task 1: Trigger Airbyte Sync from airbyte API
    trigger_airbyte_sync = [ AirbyteTriggerSyncOperator(
                                task_id="trigger_airbyte_extract_sync",
                                airbyte_conn_id=AIRBYTE_CONNECTION_ID,
                                connection_id=connection_id,
                                asynchronous=True,
                                timeout=3600,
                                wait_seconds=3
    ) for connection_id in AIRBYTE_SYNC_JOBS_ID ]


    # Task 2: Wait for Airbyte Sync to Complete , why not as EmptyOperator to gather trigger_airbyte_sync outputs??!
    wait_for_airbyte_sync = [ AirbyteJobSensor(
        task_id="wait_for_airbyte_sync",
        connection_id=connection_id,
        airbyte_job_id=trigger_airbyte_sync.output,
    ) for connection_id in AIRBYTE_SYNC_JOBS_ID ]

    # Task 3: Load Data into Postgres
    load_data_into_postgres = PostgresOperator(
        task_id="load_data_into_postgres",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=f"""{insert_raw_data_into_weather_table}""",
    )

    trigger_airbyte_sync >> wait_for_airbyte_sync >> load_data_into_postgres
