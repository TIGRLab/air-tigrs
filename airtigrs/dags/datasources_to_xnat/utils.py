'''
Utility functions for datasources_to_xnat.py
'''

from airflow.models.connection import Connection
from airflow.exceptions import AirflowNotFoundException
from datman.config import UndefinedSetting


def conn_id_exists(conn_id):
    try:
        Connection.get_connection_from_secrets(conn_id)
    except AirflowNotFoundException:
        return False
    else:
        return True


def external_xnat_is_configured(config, site):
    try:
        config.get_key('XnatSourceCredential', site=site)
        config.get_key('XnatSource', site=site)
        config.get_key('XnatSourceArchive', site=site)
    except UndefinedSetting:
        return False
    else:
        return True
