from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow import DAG
from google.cloud import storage
import pendulum


import os
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME")
bucket_name = Variable.get("LAKE_BUCKET_2")
STAGING_PREFIX = "staging/weather_1"
path_to_json_key = Variable.get('GCP_SERVICE_ACCOUNT_KEY_PATH')
gcs_client = storage.Client.from_service_account_json(json_credentials_path=path_to_json_key)
with DAG(
    dag_id="load_most_data_from_gcs_to_bigquery",
    catchup=False,
    start_date=pendulum.datetime(2025, 1, 1, 0, 59, 59),
    schedule_interval="@daily",
) as dag:



    def get_most_recent_file_name(**kwargs):
        """
        """
        date = str(kwargs["tomorrow_ds"])
        print(f"{date} with type {type(date)}")
        bucket = gcs_client.bucket(bucket_name)
        list_of_blobs = list(bucket.list_blobs(prefix=STAGING_PREFIX))
        print(list_of_blobs)
        blob_name = [blob.name for blob in list_of_blobs if date in blob.name]
        print(blob_name)
        return blob_name[0]

    get_most_recent_file = PythonOperator(
        task_id="recent_file_name",
        python_callable=get_most_recent_file_name,
        provide_context=True,
    )

get_most_recent_file
