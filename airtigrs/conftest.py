"""py.test configuration"""

from datetime import timedelta
from pathlib import Path
import pytest

from airflow.models import DAG
from airflow.utils.dates import days_ago

pytest_plugins = ["helpers_namespace"]


data_dir = Path(__file__).parent / "data" / "tests"

_config_data = {
    "main_config": data_dir / "test_config.yaml",
    "study_config": data_dir / "TEST_settings.yaml",
}


@pytest.fixture
def dm_config_data(scope="session"):
    """Create a shared config dictionary across tests to pull data in."""
    return _config_data


@pytest.fixture
def test_dag():
    """Airflow DAG for testing."""
    return DAG(
        "test_dag",
        start_date=days_ago(1),
        schedule_interval=timedelta(days=1),
    )


@pytest.helpers.register
def run_task(task, dag):
    """Run an Airflow task."""
    dag.clear()
    task.run(start_date=dag.start_date, end_date=dag.start_date)
