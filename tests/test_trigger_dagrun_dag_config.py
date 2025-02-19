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

@pytest.mark.parametrize("task_id",
                         ["trigger_airbyte_data_transform",
                          "trigger_load_most_data_from_gcs_to_bigquery",
                          "trigger_dbt_models_bigquery_dag"]
                         ) # testing code without ducplicate it
def test_trigger_tasks(mocker, dag_bag, task_id):
    dag = dag_bag.get_dag("trigger_dags")
    assert dag is not None, "Retrieving failed, can't access dag"
    assert len(dag.tasks) > 0, "DAG 'triggers_dag' does not contain task."

    task = dag.get_task(task_id)

    # Mock of trigger
    mock_trigger = mocker.patch.object(TriggerDagRunOperator, "execute")

    # mock execution
    context = {"dag_run": None, "execution_date": None}
    task.execute(context)

    # check out taht the dag has been triggered as exepcted
    mock_trigger.assert_called_once()
