'''
Utilities for querying the Airflow connection DB
'''
from airflow.models.connection import Connection
from airflow.exceptions import AirflowNotFoundException

def conn_id_exists(conn_id):
    try:
        Connection.get_connection_from_secrets(conn_id)
    except AirflowNotFoundException:
        return False
    else:
        return True
