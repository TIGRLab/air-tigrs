from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
#from airflow.providers.sftp.operators.sftp import SFTPOperator
#from airflow.providers.sftp.sensors.sftp import SFTPSensor


default_args = {
    "owner": "mjoseph",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "depends_on_past": False,
    "email": ["michael.joseph@camh.ca"],
    "email_on_failure": True,
    "email_on_retry": False
}

with DAG(
    dag_id="sftp_to_xnat",
    default_args=default_args,
    description="Transfer new MRI data from scanner SFTP servers to local filesystem and upload to XNAT",
    start_date=datetime(2021, 10, 7, 2),
    schedule_interval="@daily"
    ) as dag:
    from_sftp = BashOperator(
        task_id="from_sftp",
        bash_command="dm_sftp.py SPASD",
    )

    rename_session = BashOperator(
        task_id="rename_session",
        bash_command="dm_link.py SPASD"
    )

    xnat_upload = BashOperator(
        task_id="xnat_upload",
        bash_command="dm_xnat_upload.py SPASD"
    )

    from_sftp >> rename_session >> xnat_upload
