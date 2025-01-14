
from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
import pendulum
from project_functions.python.schema_fields import old_data_schema_field, new_data_schema_field
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import ( BigQueryCreateEmptyDatasetOperator,
                                                        BigQueryCreateEmptyTableOperator,
                                                        BigQueryInsertJobOperator,
)

from airflow.models import Variable
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.providers.airbyte.sensors.airbyte import AirbyteJobSensor

AIRFLOW_HOME = Variable.get("AIRFLOW_HOME")
gcp_conn_id = "google_cloud_gcs" # defined on gcs service account created
AIRBYTE_CONNECTION_ID = "airbyte_connection"
connection_id = ''


with DAG(
    dag_id="upload_old_data_dag",
    tags=["hostVM -> GCS : Airbytejob -> BigQuery"],
    default_args={'owner': 'hostvm-airbyte'},
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
        gcp_conn_id=gcp_conn_id
    )

    create_old_data_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_old_weather_dataset",
        project_id='wagon-bootcamp-437909',
        gcp_conn_id="google_cloud_bq",
        dataset_id="",
        location='EU'
    )

    #### under conditions for exemple if not exist
    create_table_task = BigQueryCreateEmptyTableOperator(
        task_id="create_table",
        project_id='wagon-bootcamp-437909',
        gcp_conn_id="google_cloud_connection",
        dataset_id='de_ko_airflow_taxi_gold',
        table_id='trips',
        schema_fields=old_data_schema_field

    )
    # Trigger the Airbyte sync
    trigger_airbyte_sync = AirbyteTriggerSyncOperator(
        task_id="trigger_airbyte_sync",
        airbyte_conn_id=AIRBYTE_CONNECTION_ID,
        connection_id=connection_id,
        asynchronous=True,
        timeout=3600,
        wait_seconds=4
)

    # Wait for the Airbyte sync to finish
    wait_for_airbyte_sync = AirbyteJobSensor(
        task_id="wait_for_airbyte_sync",
        airbyte_conn_id=AIRBYTE_CONNECTION_ID,
        airbyte_job_id=trigger_airbyte_sync.output
    )

    # Upload the data to BigQuery
    upload_to_bigquery = GCSToBigQueryOperator(
        task_id="upload_to_bigquery",
        bucket="weather_airbyte_dk_bucket",
        source_objects=["my-data.csv"],
        destination_project_dataset_table="my_dataset.my_table",
        schema_fields=new_data_schema_field,
        write_disposition="WRITE_TRUNCATE",
        skip_leading_rows=1,
    )

    # Set the task dependencies
wait_for_local_transformation >> trigger_airbyte_sync >> wait_for_airbyte_sync >> upload_to_bigquery
