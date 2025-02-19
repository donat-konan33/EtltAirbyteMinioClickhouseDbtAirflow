from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="trigger_dags",
    start_date=datetime(2025, 2, 7),
    catchup=False,
    schedule_interval=None,
) as dag:

    start_task = EmptyOperator(task_id="start")

    trigger_A = TriggerDagRunOperator(
        task_id="trigger_airbyte_data_transform",
        trigger_dag_id="airbyte_data_transform",
        logical_date="{{ ts }}",
    )

    trigger_B = TriggerDagRunOperator(
        task_id="trigger_load_most_data_from_gcs_to_bigquery",
        trigger_dag_id="load_most_data_from_gcs_to_bigquery",
        logical_date="{{ ts }}",
    )

    trigger_C = TriggerDagRunOperator(
        task_id='trigger_dbt_models_bigquery_dag',
        trigger_dag_id="dbt_models_bigquery_dag",
        logical_date="{{ ts }}",
    )

    start_task >> [trigger_A, trigger_B, trigger_C]
