from datetime import timedelta

from airflow import DAG
from airflow.utils.dates import days_ago

import datman.config
from airtigrs.task_groups.xnat_ingest import build_xnat_ingest_taskgroup

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["tigr.lab@camh.ca"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}


def create_dag(study, config, default_args):
    """Generates a DAG for a given study."""

    dag = DAG(
        study,
        default_args=default_args,
        description="Create end-to-end DAG for a given study",
        schedule_interval=timedelta(days=1),
        tags=[study],
        start_date=days_ago(1),
    )

    with dag:

        xnat_ingest = build_xnat_ingest_taskgroup(study, config)

        xnat_ingest

    return dag


config = datman.config.config()
projects = config.get_key("Projects")
for project in projects:
    config.set_study(project)
    dag = create_dag(project, config, default_args)
