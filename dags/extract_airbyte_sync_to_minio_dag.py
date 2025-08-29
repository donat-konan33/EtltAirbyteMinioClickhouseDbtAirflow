from airflow import DAG
import sys
import pendulum
import os
sys.path.append(os.environ.get("AIRFLOW_HOME"))
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator

connections_id = "b7a1bcd4-926d-4782-89e4-c7f9e579d69a" # for test
airbyte_conn_id = "http_server_connection"

# for Airbyte cloud or OSS with Auth to join Airbyte Modern API
with DAG(
    dag_id="airbyte_extract_sync_dag",
    tags=["airbyte sync launch by airflow"],
    default_args={'owner': 'airbyte'},
    start_date=pendulum.datetime(2025, 7, 10, tz="UTC"),
    schedule="0 2 * * *", #CronTriggerTimetable("0 2 * * *", timezone="UTC"),
    catchup=False,  # Do not backfill, don't run any historical data
) as dag:
    airbyte_sync_task = AirbyteTriggerSyncOperator(
        task_id="airbyte_sync_task",
        airbyte_conn_id=airbyte_conn_id,
        connection_id=connections_id
    )
    airbyte_sync_task
