from multiprocessing import context
from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.http.sensors.http import HttpSensor
import pendulum
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.task_group import TaskGroup
import json
import requests
from airflow.sensors.python import PythonSensor
#import time

# list of connection ID for synchronization

def get_connection_ids():
    with open("./airbyte/weather_connections.json", "r") as f:
        data = json.load(f)
    return [conn["connectionId"] for conn in data]

CONNECTION_IDS = get_connection_ids()
default_args = {
    "owner": "airflow",
    "retries": 0,
}

with DAG(
    dag_id="http_sync_dag",
    default_args=default_args,
    schedule_interval="0 2 * * *",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
) as dag:

    def check_airbyte_job(ti, conn_id, **context):
        job_id = ti.xcom_pull(task_ids=f"sync_{conn_id}")
        url = "http://airbyte-abctl-control-plane:80/api/v1/jobs/get"
        headers = {"Content-Type": "application/json"}

        try:
            resp = requests.post(url, headers=headers, json={"id": job_id})
            resp.raise_for_status()
            data = resp.json()
            status = data["job"]["status"]
            print(f"Job {job_id} status = {status}")
            return status == "succeeded"
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 500:
                print(f"Job {job_id} continue to poke")
                return False
            else:
                print(f"Job {job_id} failed with status {e.response.status_code}")
                raise
    # generate trigger tasks for each connection_id
    tasks = []
    sensor_tasks = []
    for conn_id in CONNECTION_IDS:
        task = HttpOperator(
            task_id=f"sync_{conn_id}",
            http_conn_id="http_server_connection",
            endpoint="/api/v1/connections/sync",  # classic airbyte endpoint
            method="POST",
            headers={"Content-Type": "application/json"},
            data=json.dumps({"connectionId": conn_id}),
            log_response=True,
            response_filter=lambda response: response.json()["job"]["id"],  # <-- retrieve jobId
            do_xcom_push=True
        )
        # The HttpSensor must use Jinja templating to ensure it operates within the TaskInstance context.
        # The template {{ ti.xcom_pull(task_ids="sync_<conn_id>") }} will be rendered at runtime by Airflow.
        sensor_task = PythonSensor(
            task_id=f"wait_for_sync_{conn_id}",
            poke_interval=10,
            timeout=3600,
            mode="poke",
            python_callable=check_airbyte_job,
            op_kwargs={"conn_id": conn_id}
        )

        tasks.append(task)
        sensor_tasks.append(sensor_task)


    # create 3 groups of task
    def create_group_of_task(tasks, parallelism):
        """
        This groups help airbyte handle jobs without struggling or overwhelming the system.
        parallelism: number of tasks to run concurrently

        """
        return [tasks[i:i + parallelism] for i in range(0, len(tasks), parallelism)]

    start_task = EmptyOperator(task_id="start")
    done = EmptyOperator(task_id="all_done", trigger_rule=TriggerRule.ALL_SUCCESS)

    def create_grouped_tasks(tasks, sensor_tasks, parallelism=4):
        Batches = create_group_of_task(tasks, parallelism)
        Sensors_batches = create_group_of_task(sensor_tasks, parallelism)

        previous_group_done = start_task
        for i, (group, sensor_group) in enumerate(zip(Batches, Sensors_batches)):
            with TaskGroup(group_id=f"group_{i}") as tg:
                group_done = EmptyOperator(task_id=f"group_{i}_done", trigger_rule=TriggerRule.ALL_SUCCESS)
                for j, (task, sensor_task) in enumerate(zip(group, sensor_group)):
                    previous_group_done >> task
                    task >> sensor_task >> group_done
            previous_group_done = group_done
        previous_group_done >> done
    # Launch the grouped tasks
    create_grouped_tasks(tasks, sensor_tasks, parallelism=4)
