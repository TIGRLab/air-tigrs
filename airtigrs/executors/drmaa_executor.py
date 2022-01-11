import drmaa

from typing import TYPE_CHECKING
from typing import Optional, Tuple, Dict, List
from multiprocessing import Queue, Empty

from airflow.executors.base_executor import BaseExecutor, NOT_STARTED_MESSAGE
from airflow.exceptions import AirflowException

import airflow.executors.config_adapters as adapters
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.session import provide_session
from airflow.models import Variable

if TYPE_CHECKING:
    from airflow.models.taskinstance import (TaskInstanceKey,
                                             TaskInstanceStateType)
    from airflow.executors.base_executor import CommandType
    from sqlalchemy.orm import Session

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

    # TODO: Make `scheduler_job_ids` configurable under [executor]
    @provide_session
    def _get_or_create_job_var(self,
                               session: Optional[Session] = None) -> List[int]:
        current_jobs = Variable.get("scheduler_job_ids",
                                    default_var=None,
                                    deserialize_json=True)
        if not current_jobs:
            self.log.info("Setting up job tracking Airflow variable...")
            Variable.set("scheduler_job_ids", {"jobs": []},
                         description="Scheduler Job ID tracking",
                         serialize_json=True,
                         session=session)
            self.log.info("Created `scheduler_job_ids` variable")

        return current_jobs["jobs"]

    @provide_session
    def _push_job_id(self,
                     job_id: int,
                     session: Optional[Session] = None) -> None:

        current_jobs = self._get_or_create_job_var()
        current_jobs.append(job_id)
        write_json = {"jobs": current_jobs}
        Variable.update("scheduler_job_ids",
                        write_json,
                        serialize_json=True,
                        session=session)

    def push_to_tracking(self, job_id: int) -> None:
        self.log.info(
            "Adding Job {job_id} to tracking variable `scheduler_job_ids`")
        self._push_job_id(job_id)
        self.log.info("Successfully added {job_id} to `scheduler_job_ids`")

    def start(self) -> None:
        self.log.info("Initializing DRMAA session")
        self.session = drmaa.Session()
        self.session.initialize()

        self.log.info(
            "Getting job tracking Airflow Variable: `scheduler_job_ids`")
        current_jobs = self._get_or_create_job_var()

        if current_jobs:
            print_jobs = "\n".join([j_id for j_id in current_jobs["jobs"]])
            self.log.info(f"Jobs from previous session:\n{print_jobs}")

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
        job_id = self.session.runJob(jt)
        self.log.info(f"Submitted Job {job_id}")
        self.push_to_tracking(job_id)

        # Prevent memory leaks on C back-end, running jobs unaffected
        # https://drmaa-python.readthedocs.io/en/latest/drmaa.html
        self.session.deleteJobTemplate(jt)
