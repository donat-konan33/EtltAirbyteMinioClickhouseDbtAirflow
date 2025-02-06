
import os
import sys

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME")
sys.path.append(AIRFLOW_HOME)

from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from project_functions.python.schema_fields import new_data_schema_field
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
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

with DAG(
    dag_id="load_most_data_from_gcs_to_bigquery",
    start_date=pendulum.datetime(2025, 2, 7, tz="UTC"),
    schedule_interval="10 2 * * *",
    catchup=False,
) as dag:

    def get_most_recent_file_name(**kwargs):
        """
        """
        date = str(kwargs["tomorrow_ds"])
        execution_date = kwargs["ds"] # in case where the most recent task failed
        bucket = gcs_client.bucket(bucket_name)
        list_of_blobs = list(bucket.list_blobs(prefix=STAGING_PREFIX))
        blob_name = [blob.name for blob in list_of_blobs if date in blob.name]
        if blob_name == []:
            last_blob_name = [blob.name for blob in list_of_blobs if execution_date in blob.name]
            print(f"last_date is {execution_date}")
            return last_blob_name[0]
        return blob_name[0]

    wait_for_transform_gcs_data = ExternalTaskSensor(
        task_id="wait_gcs_data_transformation",
        external_dag_id="airbyte_data_transform",
        external_task_id="data_to_staging",
        mode = 'poke',
        poke_interval=10,
        timeout=600,
        allowed_states=["success"]
    )

    get_most_recent_file = PythonOperator(
        task_id="recent_file_name",
        python_callable=get_most_recent_file_name,
        provide_context=True,
    )

    load_data = GCSToBigQueryOperator(
        task_id="load_data_to_most_recent_weather_table",
        bucket=bucket_name,
        source_format='parquet',
        source_objects=["{{ ti.xcom_pull(task_ids='recent_file_name') }}" ],
        destination_project_dataset_table=f"{PROJECT_ID}.raw_olddata_weatherteam.most_recent_weather",
        schema_fields=new_data_schema_field,
        write_disposition='WRITE_TRUNCATE',
        gcp_conn_id=gcp_conn_id
    )

wait_for_transform_gcs_data >> get_most_recent_file >> load_data
