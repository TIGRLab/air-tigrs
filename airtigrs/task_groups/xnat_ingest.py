from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

from airtigrs.operators.sftp import SFTPFetchOperator
import airtigrs.utils.connections as conn
import airtigrs.utils.xnat as xnat


def build_xnat_ingest_taskgroup(study, config):
    """
    Generate a DAG for a given study

    Will only return a DAG if XNAT is configured or if
    SFTP is configured. If not returns None
    """

    with TaskGroup("xnat_ingest_taskgroup") as xnat_ingest:

        for site in config.get_sites():
            with TaskGroup(group_id="dm_sftp") as dm_sftp:
                conn_id = f"{study}_{site}_sftp"
                if conn.conn_id_exists(conn_id):
                    SFTPFetchOperator(
                        task_id=f"{study}_{site}_fetcher",
                        sftp_conn_id=conn_id,
                        local_path=config.get_path("zips"),
                        retries=0,
                    )

                if xnat.external_xnat_is_configured(config, site):
                    BashOperator(
                        task_id="xnat_fetch_session",
                        bash_command="xnat_fetch_sessions.py {{params.study}}",
                        params={"study": study},
                    )

        dm_link = BashOperator(
            task_id="link_zips_to_datman",
            bash_command="dm_link.py {{ params.study }}",
            params={"study": study},
            trigger_rule="all_done",
        )

        dm_xnat_upload = BashOperator(
            task_id="xnat_upload",
            bash_command="dm_xnat_upload.py {{ params.study }}",
            params={"study": study},
        )

        dm_sftp >> dm_link >> dm_xnat_upload
