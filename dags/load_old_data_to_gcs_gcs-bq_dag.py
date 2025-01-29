
from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
import pendulum
import sys
import os
import pathlib
from typing import List
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME")
sys.path.append(AIRFLOW_HOME)

from project_functions.python.schema_fields import ( old_data_schema_field, regdep_france_raw_schema_field,
                                                    mun_dep_france_raw_schema_field, )
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import ( BigQueryCreateEmptyDatasetOperator,
                                                        BigQueryCreateEmptyTableOperator,
)
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator
from airflow.models import Variable

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME")
PROJECT_ID = os.environ.get("PROJECT_ID")
gcp_conn_id = "google_cloud_default"


with DAG(
    dag_id="upload_old_data_dag",
    tags=["hostVM_airflow -> GCS"],
    default_args={'owner': 'hostvm_airflow'},
    start_date=pendulum.datetime(2025, 1, 1, 0, 59, 59),
    schedule_interval="@daily",
    catchup=False,  # Do not backfill, don't run any historical data
) as dag:

    file = f"{AIRFLOW_HOME}/data/old_data_transformed/france_weather_old_data.parquet"
    bucket = Variable.get("LAKE_BUCKET")
    bigquery_schema_field = dict(
        weather = old_data_schema_field,
        depcode = regdep_france_raw_schema_field,
        municipality = mun_dep_france_raw_schema_field,
    )

    def create_dataset_table(table_name: str, schema_field: List[dict], dataset_id: str='raw_olddata_weatherteam') -> BigQueryCreateEmptyTableOperator:
        """allow us to create multi table for a dataset """
        return  BigQueryCreateEmptyTableOperator(
        task_id=f"create_{table_name}_table",
        project_id=PROJECT_ID,
        dataset_id=dataset_id,
        table_id=table_name,
        schema_fields=schema_field,
        gcp_conn_id=gcp_conn_id,
    )

    launch_table_creation = [ create_dataset_table(table_name, schema_field) for table_name, schema_field in bigquery_schema_field.items() ]


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
        dst=f"olddata/raw/{file.split('/')[-1]}",
        bucket=bucket,
        gcp_conn_id=gcp_conn_id
    )

    upload_opendatasoft_to_gcs = LocalFilesystemToGCSOperator(
        task_id="upload_opendatasoft_to_gcs",
        src=f"{AIRFLOW_HOME}/data/opendatasoft_2024/*.parquet",
        dst=f"opendatasoft_2024/raw/",
        bucket=bucket,
        gcp_conn_id=gcp_conn_id
    )

    create_old_data_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_old_weather_dataset",
        project_id=PROJECT_ID,
        gcp_conn_id=gcp_conn_id,
        dataset_id="raw_olddata_weatherteam",
        location='europe-west1'
    )

    createbucket = GCSCreateBucketOperator(
        task_id="create_bucket",
        bucket_name=bucket,
        location='EU',
        project_id=PROJECT_ID,
        gcp_conn_id=gcp_conn_id
    )


    transfer_weather_to_bq = GCSToBigQueryOperator(
        task_id="transfer_old_to_bq",
        bucket=bucket,
        source_format='parquet',
        source_objects=[f"olddata/raw/{file.split('/')[-1]}", ],
        destination_project_dataset_table=f"{PROJECT_ID}.raw_olddata_weatherteam.weather",
        write_disposition='WRITE_TRUNCATE',
        gcp_conn_id=gcp_conn_id
    )

    transfer_depcode_to_bq = GCSToBigQueryOperator(
        task_id="transfer_depcode_to_bq",
        bucket=bucket,
        source_format='parquet',
        source_objects=[f"opendatasoft_2024/raw/france_region_department96.parquet", ],
        destination_project_dataset_table=f"{PROJECT_ID}.raw_olddata_weatherteam.depcode",
        write_disposition='WRITE_TRUNCATE',
        gcp_conn_id=gcp_conn_id
    )

    transfer_municipality_to_bq = GCSToBigQueryOperator(
        task_id="transfer_municipality_to_bq",
        bucket=bucket,
        source_format='parquet',
        source_objects=[f"opendatasoft_2024/raw/municipality_and_departments.parquet", ],
        destination_project_dataset_table=f"{PROJECT_ID}.raw_olddata_weatherteam.municipality",
        write_disposition='WRITE_TRUNCATE',
        gcp_conn_id=gcp_conn_id
    )

    table_creation_end = EmptyOperator(
        task_id="end",
        trigger_rule='all_success'
    )

    upload_end = EmptyOperator(
        task_id="upload_end",
        trigger_rule='all_success'
    )


# Set the chain of tasks
createbucket >> create_old_data_dataset >> launch_table_creation >> table_creation_end >> wait_for_local_transformation
wait_for_local_transformation >> [upload_to_gcs, upload_opendatasoft_to_gcs] >> upload_end
upload_end >> [transfer_weather_to_bq, transfer_depcode_to_bq, transfer_municipality_to_bq, ]
