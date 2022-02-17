"""py.test configuration"""

from pathlib import Path
import pytest


data_dir = Path(__file__).parent / "data" / "tests"

_config_data = {
    "main_config": data_dir / "test_config.yaml",
    "study_config": data_dir / "TEST_settings.yaml",
}


@pytest.fixture()
def dm_config_data(scope="session"):
    """Create a shared config dictionary across tests to pull data in."""
    return _config_data
