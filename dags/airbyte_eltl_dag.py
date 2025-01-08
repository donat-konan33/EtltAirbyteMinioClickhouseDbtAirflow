from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from pendulum import Pendulum
from airflow.operators.postgres_operator import PostgresOperator, PostgresHook
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.providers.airbyte.sensors.airbyte import AirbyteJobSensor
