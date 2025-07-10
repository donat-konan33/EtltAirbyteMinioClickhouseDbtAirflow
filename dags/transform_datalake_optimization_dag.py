import os
import sys

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME")
sys.path.append(AIRFLOW_HOME)

from airflow import DAG
import pendulum

from project_functions.python.transform_airbyte_extract_data import (create_table_from_airbyte_data,
                                                                     retrieve_diff_date,
                                                                     concat_data_by_date,
                                                                     delete_chunked_data,
                                                                     )

from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.empty import EmptyOperator



with DAG(
    dag_id="airbyte_data_transform",
    tags=["new_data"],
    default_args={'owner': 'python'},
    description="this one allow us to transform raw data extracted by airbyte before loading to Minio Datalake",
    start_date=pendulum.datetime(2025, 2, 7, tz="UTC"),
    schedule='0 2 * * *',
    catchup=False
) as dag:
    pass
