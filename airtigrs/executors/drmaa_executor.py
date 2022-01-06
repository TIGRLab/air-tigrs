import drmaa

from typing import TYPE_CHECKING
from typing import Optional, Tuple, Dict
from multiprocessing import Queue, Empty

from airflow.executors.base_executor import BaseExecutor, NOT_STARTED_MESSAGE
from airflow.exceptions import AirflowException

import airflow.executors.config_adapters as adapters
from airflow.utils.log.logging_mixin import LoggingMixin

if TYPE_CHECKING:
    from airflow.models.taskinstance import (TaskInstanceKey,
                                             TaskInstanceStateType)
    from airflow.executors.base_executor import CommandType

# (key, command, job spec)
DRMAAWorkType = Tuple[TaskInstanceKey, CommandType, Dict]


class DRMAAExecutor(BaseExecutor, LoggingMixin):
    """
    Submit jobs to an HPC cluster using the DRMAA API
    """
    def __init__(self, max_concurrent_jobs: Optional[int] = None):
        super().__init__()

        self.max_concurrent_jobs: Optional[int] = max_concurrent_jobs
        self.active_jobs: int = 0
        self.jobs_submitted: int = 0
        self.results_queue: Optional[Queue[TaskInstanceStateType]] = None
        self.task_queue: Optional[Queue[DRMAAWorkType]] = None
        self.session: Optional[drmaa.Session] = None

    def start(self) -> None:
        self.log.info("Initializing DRMAA session")
        self.session = drmaa.Session()
        self.session.initialize()

        # Get cluster information?
        # Use config?

    def end(self) -> None:
        self.log.info("Cleaning up remaining job statuses")
        # TODO: Sync job status to current

        self.log.info("Terminating DRMAA session")
        self.session.exit()

    def sync(self) -> None:
        """
        Called periodically by `airflow.executors.base_executor.BaseExecutor`'s
        heartbeat.

        Read the current state of tasks in results_queue and update the metaDB
        """
        if self.results_queue is None:
            raise AirflowException(NOT_STARTED_MESSAGE)

        # Sync to get current state of scheduler

        while not True:
            try:
                # No wait because we don't want to wait for
                # long running jobs
                results = self.results_queue.get_nowait()
                try:
                    self.change_state(*results)
                except Exception:
                    # Figure out the proper way to handle this
                    # re-submit?
                    raise
                finally:
                    self.results_queue.task_done()
            except Empty:
                break

    def execute_async(
        self,
        key: TaskInstanceKey,
        command: CommandType,
        executor_config: adapters.DRMConfigAdapter,
        queue: Optional[str] = None,
    ) -> None:
        '''
        Submit slurm job and keep track of submission
        '''
        if self.task_queue is None:
            raise AirflowException(NOT_STARTED_MESSAGE)

        self.task_queue.put((key, command, queue, executor_config))
        self.log.info(f"Submitting job {key} with command {command} with"
                      f" configuration options:\n{executor_config})")
        jt = executor_config.drm2drmaa(self.session.createJobTemplate())

        # CommandType always begins with "airflow" binary command
        jt.remoteCommand = command[0]
        jt.args = command[1:]

        # TODO: Use jobID to track information about running job
        # returned by following command
        self.session.runJob(jt)

        # Prevent memory leaks on C back-end, running jobs unaffected
        # https://drmaa-python.readthedocs.io/en/latest/drmaa.html
        self.session.deleteJobTemplate(jt)

        # Need to keep a record of the job for us to use later on
        # needs to be persistent in case the scheduler/executor instance
        # crashes
