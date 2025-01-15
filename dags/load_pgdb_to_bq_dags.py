from airflow.providers.google.cloud.operators.bigquery import ( BigQueryCreateEmptyDatasetOperator, BigqueryInsertJobOperatror,
                                                        BigQueryCreateEmptyTableOperator
)
from airflow.providers.airbyte.operators import AirbyteTriggerSyncOperator, AirbyteJobSensor
from airflow.sensors.external_task import ExternalTaskSensor
from project_functions.python.schema_fields import new_data_schema_field
from airflow import DAG
import pendulum
from airflow.models import Variable

AIRFLOW_HOME = Variable.get("AIRFLOW_HOME")
AIRBYTE_CONNECTION_ID = "airbyte_connection"
connection_id = 'c9e28298-c03e-4341-ad9c-e197f40b825f'

with DAG(
    dag_id="load_pgdb_to_bq",
    tags=["airbyte-postgres"],
    default_args={'owner': 'airbyte-postgres'},
    schedule_interval="@daily",
    start_date=pendulum.datetime(2025, 1, 1, 0, 59, 59),
    catchup=False,
) as dag:


    wait_for_pgdb_insert = ExternalTaskSensor(
        task_id='pgdb_sensor',
        external_dag_id="airbyte_eltl_dag",
        external_task_id="load_data_into_postgres",
        mode = 'poke',
        poke_interval=10,
        timeout=600,
        allowed_states=["success"]
    )

    create_old_data_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_old_weather_dataset",
        project_id='wagon-bootcamp-437909',
        gcp_conn_id="google_cloud_bq",
        dataset_id="raw_data_weatherteam",
        location='europe-west1'   # it's strongly recommended to set EU as multiregion to ensure the extreme availability of our data
    )                              # for this project this setup might reduce costs

    #### under conditions for exemple if not exist
    create_table_task = BigQueryCreateEmptyTableOperator(
        task_id="create_table",
        project_id='wagon-bootcamp-437909',
        gcp_conn_id="google_cloud_bq",
        dataset_id='raw_data_weatherteam',
        table_id='weather',
        schema_fields=new_data_schema_field

    )

    trigger_airbyte_pg_to_bq_sync = AirbyteTriggerSyncOperator(
                                task_id="airbyte_load_from_pg_to_bq_raw_data_weatherteam",
                                airbyte_conn_id=AIRBYTE_CONNECTION_ID,
                                connection_id=connection_id,
                                asynchronous=True,
                                timeout=3600,
                                wait_seconds=3
    )

    wait_for_airbyte_pg_to_bq_sync = AirbyteJobSensor(
        task_id="wait_for_airbyte_load_from_pg_to_bq_raw_data_weatherteam",
        connection_id=connection_id,
        airbyte_job_id=trigger_airbyte_pg_to_bq_sync.output,
    )

create_old_data_dataset >> create_table_task
wait_for_pgdb_insert >> trigger_airbyte_pg_to_bq_sync >> wait_for_airbyte_pg_to_bq_sync
