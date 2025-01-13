
from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from pendulum import Pendulum
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import (
    LocalFilesystemToGCSOperator,
)

from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.providers.airbyte.sensors.airbyte import AirbyteJobSensor

gcp_conn_id = "google_cloud_gcs" # defined on gcs service account created
AIRBYTE_CONNECTION_ID = "airbyte_connection"
connection_id = ''


with DAG(
    dag_id="upload_old_data_dag",
    tags=["hostVM -> GCS : Airbytejob -> BigQuery"],
    default_args={'owner': 'hostvm-airbyte'},
    start_date=Pendulum(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,  # Do not backfill, don't run any historical data
) as dag:

    # upload old data to GCS
    upload_to_gcs = LocalFilesystemToGCSOperator(
        task_id="upload_to_gcs",
        src="data/old_data_transformed/france_weather_old_data.parquet",
        dst="raw/france_weather_old_data.parquet",
        bucket='weather_airbyte_dk_bucket',
        gcp_conn_id=gcp_conn_id
    )

    # Trigger the Airbyte sync
    trigger_airbyte_sync = AirbyteTriggerSyncOperator(
        task_id="trigger_airbyte_sync",
        airbyte_conn_id=AIRBYTE_CONNECTION_ID,
        connection_id=connection_id,
        asynchronous=True,
        timeout=3600,
        wait_seconds=3
)

    # Wait for the Airbyte sync to finish
    wait_for_airbyte_sync = AirbyteJobSensor(
        task_id="wait_for_airbyte_sync",
        connection_id="airbyte_connection",
        source_id="airbyte_source",
        mode="poke",
        poke_interval=60,
        timeout=60 * 60 * 1,  # 1 hour
    )

    # Upload the data to BigQuery
    upload_to_bigquery = GCSToBigQueryOperator(
        task_id="upload_to_bigquery",
        bucket="weather_airbyte_dk_bucket",
        source_objects=["my-data.csv"],
        destination_project_dataset_table="my_dataset.my_table",
        schema_fields=[

        ],
        write_disposition="WRITE_TRUNCATE",
        skip_leading_rows=1,
    )

    # Set the task dependencies
    wait_for_dbt_models >> trigger_airbyte_sync >> wait_for_airbyte_sync >> upload_to_bigquery
