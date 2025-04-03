import os
import sys
import io
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME")
sys.path.append(AIRFLOW_HOME)

from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from project_functions.python.schema_fields import new_data_schema_field
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from project_functions.python.transform_airbyte_extract_data import run_async, get_file_names
from airflow.models import Variable
from airflow import DAG
from google.cloud import storage
import pendulum


bucket_name = Variable.get("LAKE_BUCKET_2")
PROJECT_ID = os.environ.get("PROJECT_ID")
STAGING_PREFIX = "staging/weather_1"
path_to_json_key = Variable.get('GCP_SERVICE_ACCOUNT_KEY_PATH')
gcs_client = storage.Client.from_service_account_json(json_credentials_path=path_to_json_key)
gcp_conn_id = "google_cloud_default"
DATASET_NAME = "raw_entiredata_weatherteam"
TABLE_NAME = "most_recent_entire_weather"


def concat_all_data_and_store_to_bq(gcs_client:storage.Client, bucket_name:str, STAGING_PREFIX:str):
    """
    concatenate all files in staging/weather_1 and store it to BigQuery
    """
    list_of_files = get_file_names(gcs_client, bucket_name, STAGING_PREFIX)
    data = run_async(gcs_client, bucket_name, list_of_files)
    print(data.head())

    output_file = f"{STAGING_PREFIX}/merged_data.parquet"
    output_stream = io.BytesIO()
    data.to_parquet(output_stream, index=False)
    output_stream.seek(0)

    bucket = gcs_client.bucket(bucket_name)
    blob = bucket.blob(output_file)
    blob.upload_from_file(output_stream, content_type="application/octet-stream")

    print(f"✅ {output_file} Data loaded to GCS into {bucket_name} bucket")


def delete_files(client, bucket_name, prefix):
    """
    delete only merged files in staging/weather_1
    """
    output_file = f"{prefix}/merged_data.parquet"
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(output_file)
    blob.delete()
    print(f"✅ {output_file} Data deleted from GCS into {bucket_name} bucket")


with DAG(
    dag_id="load_all_staging_data_from_gcs_to_bigquery",
    tags=["gcs to bigquery"],
    default_args={'owner': 'gcs & bigquery', "depends_on_past": True},
    start_date=pendulum.datetime(2025, 2, 7, tz="UTC"),
    schedule_interval='@weekly', # execute every week to visualize the trend of the next week
    catchup=False,
) as dag:

    # create dataset and table in BigQuery
    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_all_weather_data_dataset",
        dataset_id=DATASET_NAME,
        project_id=PROJECT_ID,
        gcp_conn_id=gcp_conn_id,
        location="europe-west1",
        )

    create_table = BigQueryCreateEmptyTableOperator(
        task_id="create_all_weather_data_table",
        dataset_id=DATASET_NAME,
        table_id=TABLE_NAME,
        schema_fields=new_data_schema_field,
        project_id=PROJECT_ID,
        gcp_conn_id=gcp_conn_id
    )

    create_all_data = PythonOperator(
        task_id="concat_staging_weather_1_files",
        python_callable=concat_all_data_and_store_to_bq,
        op_args=[gcs_client, bucket_name, STAGING_PREFIX],
        provide_context=True
    )

    load_data_to_bq = GCSToBigQueryOperator(
        task_id=f"load_data_to_{DATASET_NAME}_weather_table",
        bucket=bucket_name,
        source_format='parquet',
        source_objects=[f"{STAGING_PREFIX}/merged_data.parquet"],
        destination_project_dataset_table=f"{PROJECT_ID}.{DATASET_NAME}.{TABLE_NAME}",
        schema_fields=new_data_schema_field,
        write_disposition='WRITE_TRUNCATE',
        gcp_conn_id=gcp_conn_id
)

    delete_data_created_into_gcs = PythonOperator(
        task_id="delete_merged_data_into_staging_weather",
        python_callable=delete_files,
        op_args=[gcs_client, bucket_name, STAGING_PREFIX],
        provide_context=True
    )
    #wait_for_transform_gcs_data >>
    create_dataset >> create_table >> create_all_data >> load_data_to_bq >> delete_data_created_into_gcs
