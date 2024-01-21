import pytest
from dag_validator.dag_validation import AirflowDAGValidation


@pytest.fixture
def airflow_dag_validator():
    return AirflowDAGValidation("dags/")


@pytest.fixture
def expected_orphaned_all_models_with_no_upstream():
    return {
        # Specify expected layers and tasks with no upstream tasks
        # e.g. "staging_to_source": {
        #           "example_dag.py": [
        #               "staging_task_with_no_upstream",
        #           },
    }


@pytest.fixture
def expected_prefix_mapping():
    return {"landing": "staging", "staging": "source"}
