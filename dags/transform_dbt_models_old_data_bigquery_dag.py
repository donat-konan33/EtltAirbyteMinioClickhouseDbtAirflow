from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryInsertJobOperator,
)

AIRFLOW_HOME = Variable.get("AIRFLOW_HOME")

with DAG(
    dag_id="dbt_models_old_data_bigquery_dag",
    tags=["dbt on bigquery"],
    default_args={'owner': 'dbt'},
    start_date=pendulum.datetime(2025, 1, 1, 0, 59, 59),
    schedule_interval="@daily",
    catchup=False,  # Do not backfill, don't run any historical data
) as dag:

    wait_for_airbyte_sensor_gcs_to_bq = ExternalTaskSensor(
        task_id="gcs_to_bq_sensor",
        external_dag_id="upload_old_data_dag",
        external_task_id="wait_for_airbyte_sync",
        mode = 'poke',
        poke_interval=10,
        timeout=600,
        allowed_states=["success"]

    )

wait_for_airbyte_sensor_gcs_to_bq
