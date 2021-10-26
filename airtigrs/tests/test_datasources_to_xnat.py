import os

import pytest
from airflow.models import DagBag
import datman.config
import importlib
from .utils import assert_dag_dict_equal

FIXTURE_DIR = "tests/test_datasources_to_xnat"


class TestConfiguredDAGBuild:
    @classmethod
    def setup_class(cls):
        '''
        Setup DATMAN environment variables for configuration file to load
        properly
        '''
        os.environ['DM_CONFIG'] = os.path.join(FIXTURE_DIR, "test_config.yaml")
        os.environ['DM_SYSTEM'] = 'test'

        # Initialize DAG object
        cls.d2x = importlib.import_module(
            'airtigrs.dags.datasources_to_xnat.datasources_to_xnat')

        cls.config = datman.config.config()
        cls.default_args = {'owner': 'test'}

    def test_sftp_task_included_if_connection_configured(self, mocker):
        '''
        Check if SFTP is configured that it is included in DAG
        '''
        mocker.patch('airtigrs.dags.datasources_to_xnat.utils.conn_id_exists',
                     return_value=True)

        result = self.d2x.make_dag(dag_id="TEST",
                                   study="TEST",
                                   config=self.config,
                                   default_args=self.default_args)

        # Does task structure match expected?
        assert_dag_dict_equal(
            {
                'TEST_TESTA_fetcher': ['link_zips_to_datman'],
                'link_zips_to_datman': ['xnat_upload'],
                'xnat_upload': []
            }, result)

    def test_fetch_xnat_included_if_connection_configured(self, mocker):
        mocker.patch(
            'airtigrs.dags.datasources_to_xnat.utils.external_xnat_is_configured',
            return_value=True)

        result = self.d2x.make_dag(dag_id="TEST",
                                   study="TEST",
                                   config=self.config,
                                   default_args=self.default_args)

        # Does task structure match expected?
        assert_dag_dict_equal(
            {
                'xnat_fetch_session': ['link_zips_to_datman'],
                'link_zips_to_datman': ['xnat_upload'],
                'xnat_upload': []
            }, result)
