"""Test validity of all DAGs."""
import os
from pathlib import Path

import pytest
from airflow import models as airflow_models
from airflow.utils.dag_cycle_tester import test_cycle as _test_cycle


DAG_PATHS = Path(__file__).parents[1].glob("*.py")

@pytest.mark.parametrize("dag_path", DAG_PATHS)
def test_dag_integrity(dag_path, dm_config_data):
    """Import DAG files and check for a valid DAG instance."""
    os.environ["DM_CONFIG"] = str(dm_config_data["main_config"])
    os.environ["DM_SYSTEM"] = "test"

    dag_name = dag_path.name
    module = _import_file(dag_name, dag_path)

    # Validate if there is at least 1 DAG object in the file
    dag_objects = [
        var
        for var in vars(module).values()
        if isinstance(var, airflow_models.DAG)
    ]
    assert dag_objects

    # For every DAG object, test for cycles
    for dag in dag_objects:
        _test_cycle(dag)


def _import_file(module_name, module_path):
    import importlib.util

    spec = importlib.util.spec_from_file_location(module_name, str(module_path))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module
