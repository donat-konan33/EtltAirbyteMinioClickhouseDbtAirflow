from airflow.models import DagBag
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.empty import EmptyOperator
import pytest
import os

@pytest.fixture()
def dag_bag():
    """
    """
    DAG_BAG = os.path.join(os.path.dirname(__file__), "../dags")
    dag_bag = DagBag(dag_folder=DAG_BAG, include_examples=False)
    assert not dag_bag.import_errors, f"Errors from DagBag import : {dag_bag.import_errors}"
    return dag_bag

# tasks executions order
def test_task_dependencies(dag_bag):
    dag = dag_bag.get_dag("trigger_dags")
    assert dag is not None, "Retrieving failed, can't access dag"

    start = dag.get_task("start")
    trigger_task_1 = dag.get_task("trigger_airbyte_data_transform")
    trigger_task_2 = dag.get_task("trigger_load_most_data_from_gcs_to_bigquery")
    trigger_task_3 = dag.get_task("trigger_dbt_models_bigquery_dag")

    assert start is not None, "Can't access task, May be `start` does not exist"
    assert trigger_task_1 is not None, "Can't access task, May be `trigger_airbyte_data_transform` does not exist"
    assert trigger_task_2 is not None, "Can't access task, May be `trigger_load_most_data_from_gcs_to_bigquery` does not exist"
    assert trigger_task_3 is not None, "Can't access task, May be `trigger_dbt_models_bigquery_dag` does not exist"

    # checkout all tasks begin at the same time after start task
    assert start.downstream_task_ids == {
            "trigger_airbyte_data_transform",
            "trigger_load_most_data_from_gcs_to_bigquery",
            "trigger_dbt_models_bigquery_dag"
    }, "tasks do not depend correctly on start task"
