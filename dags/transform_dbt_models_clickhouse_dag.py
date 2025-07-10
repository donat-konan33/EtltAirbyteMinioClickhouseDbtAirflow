from airflow import DAG
import pendulum
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.timetables.trigger import CronTriggerTimetable

import os
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME")
DBT_DIR = os.environ.get("DBT_DIR")

with DAG(
    dag_id="dbt_models_clickHouse_dag",
    tags=["dbt on ClickHouse"],
    default_args={'owner': 'dbt'},
    start_date=pendulum.datetime(2025, 7, 10, tz="UTC"),
    schedule_interval="0 2 * * *", #CronTriggerTimetable("0 2 * * *", timezone="UTC"),
    catchup=False,  # Do not backfill, don't run any historical data
) as dag:

    wait_for_airbyte_sensor_pg_to_bq = ExternalTaskSensor(
        task_id="loading_recent_data_to_bq_sensor",
        external_dag_id="", # to fill
        external_task_id="", # to fill
        mode = 'poke',
        poke_interval=10,
        timeout=1200,
        allowed_states=["success"]
    )

    dbt_transformation = BashOperator(
        task_id="dbt_build",
        bash_command=f"dbt build -m +mart_newdata --project-dir {DBT_DIR}"
    )

    wait_for_airbyte_sensor_pg_to_bq >> dbt_transformation
