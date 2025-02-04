import os
import sys

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME")
sys.path.append(AIRFLOW_HOME)

from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule
import pendulum

from project_functions.python.transform_airbyte_extract_data import create_table_from_airbyte_data, retrieve_diff_date, concat_data_by_date
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from google.cloud import storage
from airflow.providers.google.cloud.hooks.gcs import GCSHook


gcp_conn_id = "google_cloud_default"
bucket = Variable.get("LAKE_BUCKET_2")
path_to_json_key = Variable.get("GCP_SERVICE_ACCOUNT_KEY_PATH")
gcs_client = storage.Client.from_service_account_json(json_credentials_path=path_to_json_key)
SOURCE_PREFIX = "raw/weather_1/"
STAGING_PREFIX = "staging/weather_1/"

with DAG(
    dag_id="airbyte_data_transform",
    tags=["new_data"],
    default_args={'owner': 'local'},
    description="this one allow us to transform raw data extracted by airbyte before loading to BigQuery",
    start_date=pendulum.datetime(2025, 1, 1, 0, 59, 59),
    schedule_interval="@daily",
    catchup=False
) as dag:

    get_raw_files_names = GCSListObjectsOperator(
        task_id="get_available_files_names",
        bucket=bucket,
        prefix=SOURCE_PREFIX,
        gcp_conn_id=gcp_conn_id
    )

    def get_files_path(ti):
        """
        """
        file_name = ti.xcom_pull(task_ids="get_available_files_names")
        filenames = [os.path.basename(f) for f in file_name]
        return filenames

    def get_diff_date(ti):
        """
        """
        file_name = ti.xcom_pull(task_ids="get_path_list")
        diff_date = retrieve_diff_date(file_name)
        return diff_date

    def transform_and_upload_to_staging(ti, client, bucket, source_prefix, staging_prefix):
        """
        """
        dates = ti.xcom_pull(task_ids="get_existing_date")
        create_table_from_airbyte_data(client=client, dates=dates, bucket=bucket,
                                       source_prefix=source_prefix,
                                       staging_prefix=staging_prefix)

    def merge_data_by_file_date(ti):
        """
        """
        dates = ti.xcom_pull(task_ids="get_existing_date")
        concat_data_by_date(dates=dates, client=gcs_client, bucket_name=bucket, prefix=SOURCE_PREFIX)


    get_path_task = PythonOperator(
        task_id='get_path_list',
        python_callable=get_files_path,
    )

    get_diff_date_task = PythonOperator(
        task_id="get_existing_date",
        python_callable=get_diff_date
    )

    merge_airbyte_file_task = PythonOperator(
        task_id='merge_raw_data',
        python_callable=merge_data_by_file_date,

    )

    create_staging_data_task = PythonOperator(
        task_id="data_to_staging",
        python_callable=transform_and_upload_to_staging,
        op_kwargs={
            "client": gcs_client,
            "bucket": bucket,
            "source_prefix": SOURCE_PREFIX,
            "staging_prefix": STAGING_PREFIX
        }
    )

get_raw_files_names >> get_path_task >> get_diff_date_task >> merge_airbyte_file_task >> create_staging_data_task
