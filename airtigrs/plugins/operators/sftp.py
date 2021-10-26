from typing import Optional

from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.models.baseoperator import BaseOperator
from airflow.exceptions import AirflowSkipException

from airtigrs.utils import as_datetime

import os


class SFTPFetchOperator(BaseOperator):
    """
    Checks for file in SFTP directory that are not present
    in the local filesystem. Pulls mismatched files

    :param remote_path: Remote file or directory path
    :type remote_path: str
    :param local_path: Remote file or directory path
    :type local_path: str
    :param sftp_conn_id: The connection to run the sensor against
    :type sftp_conn_id: str
    """

    template_fields = (
        'sftp_conn_id',
        'remote_path',
        'local_path',
    )

    def __init__(self,
                 *,
                 sftp_conn_id: str,
                 local_path: str,
                 remote_path: str = None,
                 **kwargs) -> None:

        super().__init__(**kwargs)

        self.sftp_conn_id = sftp_conn_id
        self.remote_path = remote_path
        self.local_path = local_path
        self.hook: Optional[SFTPHook] = None

    def _get_difference_files(self):

        try:
            remote_files = sorted(self.hook.list_directory(self.remote_path))
        except OSError:
            self.log.info(f"Could not access {self.remote_path}, ensure that"
                          " directory exists!")
            raise

        try:
            local_files = sorted(os.listdir(self.local_path))
        except OSError:
            self.log.info(f"Could not access {self.local_path}, ensure that"
                          " directory exists!")

        # Check files that need updating
        pull_files = []
        matched = [r for r in remote_files if r in local_files]
        self.log.info(f'Checking {self.remote_path} that need updating...')
        for m in matched:
            remote = os.path.join(self.remote_path, m)
            local = os.path.join(self.local_path, m)

            remote_mtime = self.hook.get_mod_time(remote)
            local_mtime = as_datetime(os.path.getmtime(local))
            self.log.info(f"Local: {local_mtime}")
            self.log.info(f"Remote: {remote_mtime}")
            if remote_mtime != local_mtime:
                pull_files.append(m)
        if matched:
            self.log.info(f"Found {len(matched)} files needing to be updated!")

        # Check for missing files
        self.log.info(
            f'Checking {self.remote_path} for files not in {self.local_path}')
        new_files = [r for r in remote_files if r not in local_files]
        if new_files:
            self.log.info(f"Found {len(new_files)} new files!")
        pull_files.extend(new_files)

        if not pull_files:
            self.log.info("No files need updating...")
            raise AirflowSkipException

        return pull_files

    def execute(self, context: dict) -> bool:

        self.hook = SFTPHook(self.sftp_conn_id)

        # Process remote_path from extras
        if self.remote_path is None:
            extras = self.hook.get_connection(self.sftp_conn_id).extra_dejson
            try:
                self.remote_path = extras['remote_path']
            except KeyError:
                self.log.error(
                    "remote_path not provided to SFTP config! "
                    "Set in Airflow MetaDB connections or provide on "
                    "instance construction!")
                raise

        pull_files = self._get_difference_files()

        # If files need to be pulled, initiate
        with self.hook.get_conn() as sftp:
            with sftp.cd(self.remote_path):
                for p in pull_files:
                    self.log.info(f"Downloading {p}...")
                    sftp.get(remotepath=p,
                             localpath=os.path.join(self.local_path, p),
                             preserve_mtime=True)


