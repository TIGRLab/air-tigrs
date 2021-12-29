'''
This is an example DAG showing how Airflow can trigger
SFTP data pulls and upload
'''

from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.operators.bash import BashOperator
from airtigrs.operators.sftp import SFTPFetchOperator
import datman.config
import airtigrs.utils.connections as conn
import airtigrs.utils.xnat as xnat

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['jerrold.jeyachandra@camh.ca'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}


def make_dag(dag_id, study, config, default_args):
    '''
    Generate a DAG for a given study

    Will only return a DAG if XNAT is configured or if
    SFTP is configured. If not returns None
    '''

    config.set_study(study)
    dag = DAG(dag_id,
              default_args=default_args,
              description='Ingest from external SFTP and XNAT servers'
              ' and push to XNAT',
              schedule_interval=timedelta(days=1),
              tags=['sftp', 'xnat', 'ingest', study],
              start_date=days_ago(1))

    dag.doc_md = """
    This DAG ingests data from external datasources (SFTP, XNAT)
    then uploads the files to an XNAT instance after mangling
    names to match the desired convention
    """

    # Check if XNAT or SFTP is configured at all for study
    sftp_config = []
    fetch_xnat = False
    for site in config.get_sites():
        conn_id = f"{study}_{site}_sftp"
        if conn.conn_id_exists(conn_id):
            sftp_config.append((site, conn_id))

        if xnat.external_xnat_is_configured(config, site):
            fetch_xnat = True

    if not fetch_xnat and not sftp_config:
        return

    with dag:

        datasource_ops = []
        for site, conn_id in sftp_config:
            datasource_ops.append(
                SFTPFetchOperator(task_id=f"{study}_{site}_fetcher",
                                  sftp_conn_id=conn_id,
                                  local_path=config.get_path("zips"),
                                  retries=0))

        if fetch_xnat:
            datasource_ops.append(
                BashOperator(
                    task_id='xnat_fetch_session',
                    bash_command='xnat_fetch_sessions.py {{params.study}}',
                    params={"study": study}))

        dm_link = BashOperator(task_id='link_zips_to_datman',
                               bash_command='dm_link.py {{ params.study }}',
                               params={"study": study},
                               trigger_rule="all_done")

        dm_xnat_upload = BashOperator(
            task_id='xnat_upload',
            bash_command='dm_xnat_upload.py {{ params.study }}',
            params={"study": study})

        datasource_ops >> dm_link >> dm_xnat_upload

    return dag


config = datman.config.config()
projects = config.get_key("Projects")

for project in projects:
    dag_id = f"ingest_external_mri_{project}"
    dag = make_dag(dag_id, project, config, default_args)
    if dag is not None:
        globals()[dag_id] = dag
