import json
import os
import sys
import pendulum
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.providers.airbyte.sensors.airbyte import AirbyteJobSensor
from airflow.operators.empty import EmptyOperator

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME")
sys.path.append(AIRFLOW_HOME)
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
    start_date=pendulum.datetime(2025, 1, 1, 0, 59, 59),
    catchup=False,
) as dag:

    sql_file = f"{AIRFLOW_HOME}/project_functions/sql/insert_raw_data_into_weather_table.sql"
    with open(sql_file, 'r') as file:
        insert_raw_data_into_weather_table = file.read()

    trigger_airbyte_sync = [ AirbyteTriggerSyncOperator(
                                task_id=f"trigger_airbyte_extract_sync_{_}",
                                airbyte_conn_id=AIRBYTE_CONNECTION_ID,
                                connection_id=connection_id,
                                asynchronous=True,
                                timeout=3600,
                                wait_seconds=3
    ) for _, connection_id in enumerate(AIRBYTE_SYNC_JOBS_ID) ]


    wait_for_airbyte_sync = [ AirbyteJobSensor(
        task_id=f"wait_for_airbyte_sync_{_}",
        airbyte_conn_id=AIRBYTE_CONNECTION_ID,
        airbyte_job_id=trigger_airbyte_sync[_].output,
    ) for _, connection_id in enumerate(AIRBYTE_SYNC_JOBS_ID)]

    gather_complete = EmptyOperator(
        task_id="previous_end_task"
    )

    load_data_into_postgres = PostgresOperator(
        task_id="load_data_into_postgres",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=f"""{insert_raw_data_into_weather_table}""",
        trigger_rule='all_success'
    )


for _, task in enumerate(trigger_airbyte_sync):
    task >> wait_for_airbyte_sync[_] >> gather_complete

gather_complete >> load_data_into_postgres
