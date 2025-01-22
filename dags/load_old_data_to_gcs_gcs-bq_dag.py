
from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
import pendulum
import sys
import os
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME")
sys.path.append(AIRFLOW_HOME)

from project_functions.python.schema_fields import old_data_schema_field
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.providers.airbyte.sensors.airbyte import AirbyteJobSensor
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import ( BigQueryCreateEmptyDatasetOperator,
                                                        BigQueryCreateEmptyTableOperator,
)


AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME")
gcp_conn_id_gcs = "google_cloud_gcs" # defined for gcs service account created
gcp_conn_id_bq = "google_cloud_bq"
AIRBYTE_CONNECTION_ID = "airbyte_conn"
connection_id = '11b7e0fe-1b59-4596-a63a-455fdb31970a'


with DAG(
    dag_id="upload_old_data_dag",
    tags=["hostVM_airflow -> GCS"],
    default_args={'owner': 'hostvm_airflow-airbyte'},
    start_date=pendulum.datetime(2025, 1, 1, 0, 59, 59),
    schedule_interval="@daily",
    catchup=False,  # Do not backfill, don't run any historical data
) as dag:

    file = f"{AIRFLOW_HOME}/data/old_data_transformed/france_weather_old_data.parquet"

    wait_for_local_transformation = ExternalTaskSensor(
        task_id='old_data_transformation_sensor',
        external_dag_id='old_data_transformation_dag',
        external_task_id="old_data_transformation",
        mode = 'poke',
        poke_interval=10,
        timeout=600,
        allowed_states=["success"]
    )


    upload_to_gcs = LocalFilesystemToGCSOperator(
        task_id="upload_to_gcs",
        src=f"{AIRFLOW_HOME}/data/old_data_transformed/france_weather_old_data.parquet",
        dst=f"raw/{file.split('/')[-1]}",
        bucket='weather_airbyte_dk_bucket',
        gcp_conn_id=gcp_conn_id_gcs
    )

    create_old_data_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_old_weather_dataset",
        project_id='wagon-bootcamp-437909',
        gcp_conn_id=gcp_conn_id_bq,
        dataset_id="raw_olddata_weatherteam",
        location='europe-west1'
    )

    #### under conditions for exemple if not exist
    create_old_data_table = BigQueryCreateEmptyTableOperator(
        task_id="create_old_weather_table",
        project_id='wagon-bootcamp-437909',
        gcp_conn_id=gcp_conn_id_bq,
        dataset_id='raw_olddata_weatherteam',
        table_id='weather',
        schema_fields=old_data_schema_field

    )

    # Trigger the Airbyte sync
    trigger_airbyte_sync = AirbyteTriggerSyncOperator(
        task_id="trigger_airbyte_sync",
        airbyte_conn_id=AIRBYTE_CONNECTION_ID,
        connection_id=connection_id,
        asynchronous=True,
        timeout=3600, ##
        wait_seconds=4
)

    # Wait for the Airbyte sync to finish
    wait_for_airbyte_sync = AirbyteJobSensor(
        task_id="wait_for_airbyte_sync",
        airbyte_conn_id=AIRBYTE_CONNECTION_ID,
        airbyte_job_id=trigger_airbyte_sync.output
)

# Set the chain of task
create_old_data_dataset >> create_old_data_table
wait_for_local_transformation >> upload_to_gcs >> trigger_airbyte_sync >> wait_for_airbyte_sync
