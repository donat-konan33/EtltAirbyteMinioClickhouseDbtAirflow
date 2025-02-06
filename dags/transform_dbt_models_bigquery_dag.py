from airflow import DAG
import pendulum
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.timetables.trigger import CronTriggerTimetable

import os
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME")
DBT_DIR = os.environ.get("DBT_DIR")

with DAG(
    dag_id="dbt_models_bigquery_dag",
    tags=["dbt on bigquery"],
    default_args={'owner': 'dbt', "depends_on_past": True},
    start_date=pendulum.datetime(2025, 2, 7, tz="UTC"),
    timetable=CronTriggerTimetable("20 2 * * *", timezone="UTC"),
    catchup=False,  # Do not backfill, don't run any historical data
) as dag:

    wait_for_airbyte_sensor_pg_to_bq = ExternalTaskSensor(
        task_id="loading_recent_data_to_bq_sensor",
        external_dag_id="load_most_data_from_gcs_to_bigquery",
        external_task_id="load_data_to_most_recent_weather_table",
        mode = 'poke',
        poke_interval=10,
        timeout=600,
        allowed_states=["success"]
    )

    dbt_transformation = BashOperator(
        task_id="dbt_build",
        bash_command=f"dbt build -m +mart_newdata --project-dir {DBT_DIR}"
    )

wait_for_airbyte_sensor_pg_to_bq >> dbt_transformation
