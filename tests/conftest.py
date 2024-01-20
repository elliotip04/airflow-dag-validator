import pytest
from dag_validator.dag_validation import AirflowDAGValidation


@pytest.fixture
def airflow_dag_validator():
    return AirflowDAGValidation("dags/")


@pytest.fixture
def expected_orphaned_all_models_with_no_upstream():
    return {
        # "vault_to_curated": {
        #     "Alliant_master_dag_distribution.py": [
        #         "vault_raw_date_reference_s",
        #         "vault_raw_date_h",
        #     ],
        #     "Alliant_master_dag_initialise.py": [
        #         "vault_raw_date_reference_s",
        #         "vault_raw_date_h",
        #     ],
        #     "Alliant_master_dag_non_distribution.py": [
        #         "vault_raw_date_reference_s",
        #         "vault_raw_date_h",
        #     ],
        # },
    }


@pytest.fixture
def expected_prefix_mapping():
    return {"landing": "staging", "staging": "source"}
