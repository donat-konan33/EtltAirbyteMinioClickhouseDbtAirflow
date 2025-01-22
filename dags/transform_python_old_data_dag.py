
import os
import sys

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME")
sys.path.append(AIRFLOW_HOME)

from airflow.decorators import dag, task
import pendulum
import pathlib
from project_functions.python.old_data_functions import get_existing_csv_paths, read_csv_file, write_data_from_old_data, rename_filename
from airflow.utils.trigger_rule import TriggerRule


@dag(
    dag_id="old_data_transformation_dag",
    tags=["old_data"],
    default_args={'owner': 'local'},
    start_date=pendulum.datetime(2025, 1, 1, 0, 59, 59),
    schedule_interval="@daily",
    catchup=False
)
def old_data_transformation_dag():
    """
    """

    file = f"{AIRFLOW_HOME}/data/old_data_transformed/france_weather_old_data.parquet"

    @task(
        task_id='old_data_transformation',
        trigger_rule = TriggerRule.ALL_SUCCESS
    )
    def old_data_transformation_task():
        filespaths = get_existing_csv_paths()
        if filespaths:
            try:
                input_dataframes = read_csv_file(filespaths)
                write_data_from_old_data(input_dataframes, file)
                rename_filename(filespaths)
            except Exception as e:
                print(f"Something went wrong: {e}")

    old_data_transformation_task()

old_data_transformation_dag()
