from airflow import DAG
from pendulum import Pendulum

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
    dag_id="dbt_models_dag",
    tags=["dbt on bigquery"],
    default_args={'owner': 'dbt'},
    start_date=Pendulum(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,  # Do not backfill, don't run any historical data
) as dag:

    # Create the dataset in BigQuery
    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_dataset",
        dataset_id="my_dataset",
        project_id="my_project",
        location="US",
    )

    # Create the table in BigQuery
    create_table = BigQueryCreateEmptyTableOperator(
        task_id="create_table",
        dataset_id="my_dataset",
        table_id="my_table",
        project_id="my_project",
        schema_fields=[
            {"name": "id", "type": "INTEGER", "mode": "REQUIRED"},
            {"name": "name", "type": "STRING", "mode": "REQUIRED"},
            {"name": "age", "type": "INTEGER", "mode": "REQUIRED"},
        ],
    )

    # Run the dbt models
    run_dbt_models = BashOperator(
        task_id="run_dbt_models",
        bash_command="dbt run",
    )

    # Load the data into BigQuery
    load_data_into_bigquery = BigQueryInsertJobOperator(
        task_id="load_data_into_bigquery",
        project_id="my_project",
        configuration={
            "load": {
                "sourceFormat": "CSV",
                "sourceUris": ["gs://my-bucket/my-data.csv"],
                "destinationTable": {
                    "projectId": "my_project",
                    "datasetId": "my_dataset",
                    "tableId": "my_table",
                },
            }
        },
    )

    create_dataset >> create_table >> run_dbt_models >> load_data_into_bigquery
