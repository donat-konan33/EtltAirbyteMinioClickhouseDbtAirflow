from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.branch import BranchPythonOperator
from pendulum import Pendulum
from airflow.providers.postgres.operators import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.providers.airbyte.sensors.airbyte import AirbyteJobSensor

POSTGRES_CONN_ID = "postgres_connection"
AIRBYTE_CONNECTION_ID = "airbyte_connection"
AIRBYTE_SYNC_JOB_ID = "d920c950-12bd-4525-88c6-55ccbfee5ffe"


with DAG(
    dag_id="airbyte_eltl_dag",
    tags=["airbyte-postgres"],
    default_args={'owner': 'airbyte-postgres'},
    schedule_interval="@daily",
    start_date=Pendulum(2021, 1, 1),
    catchup=False,
) as dag:

    with open("project_functions/sql/insert_raw_data_into_weather_table.sql", 'r') as file:
        insert_raw_data_into_weather_table = file.read()

    # Task 1: Trigger Airbyte Sync from airbyte API
    trigger_airbyte_sync = AirbyteTriggerSyncOperator(

        task_id="trigger_airbyte_sync",
        airbyte_conn_id=AIRBYTE_CONNECTION_ID,
        connection_id=AIRBYTE_SYNC_JOB_ID,
        asynchronous=True,
        timeout=3600,
        wait_seconds=3
    )


    # Task 2: Wait for Airbyte Sync to Complete
    wait_for_airbyte_sync = AirbyteJobSensor(
        task_id="wait_for_airbyte_sync",
        connection_id=AIRBYTE_SYNC_JOB_ID,
        airbyte_job_id=trigger_airbyte_sync.output,
    )

    # Task 3: Load Data into Postgres
    load_data_into_postgres = PostgresOperator(
        task_id="load_data_into_postgres",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=f"""{insert_raw_data_into_weather_table}""",
    )

    trigger_airbyte_sync >> wait_for_airbyte_sync >> load_data_into_postgres
