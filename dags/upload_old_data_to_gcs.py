
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
        src="/path/to/my-data.csv",
        dst="my-bucket",
        bucket_name="my-bucket",
    )

    # Trigger the Airbyte sync
    trigger_airbyte_sync = AirbyteTriggerSyncOperator(
        task_id="trigger_airbyte_sync",
        connection_id="airbyte_connection",
        source_id="airbyte_source",
    )

    # Wait for the Airbyte sync to finish
    wait_for_airbyte_sync = AirbyteJobSensor(
        task_id="wait_for_airbyte_sync",
        connection_id="airbyte_connection",
        source_id="airbyte_source",
        mode="poke",
        poke_interval=60,
        timeout=60 * 60 * 24,  # 24 hours
    )

    # Upload the data to BigQuery
    upload_to_bigquery = GCSToBigQueryOperator(
        task_id="upload_to_bigquery",
        bucket="my-bucket",
        source_objects=["my-data.csv"],
        destination_project_dataset_table="my_dataset.my_table",
        schema_fields=[
            {"name": "id", "type": "INTEGER", "mode": "REQUIRED"},
            {"name": "name", "type": "STRING", "mode": "REQUIRED"},
            {"name": "age", "type": "INTEGER", "mode": "REQUIRED"},
        ],
        write_disposition="WRITE_TRUNCATE",
        skip_leading_rows=1,
    )

    # Set the task dependencies
    wait_for_dbt_models >> trigger_airbyte_sync >> wait_for_airbyte_sync >> upload_to_bigquery
