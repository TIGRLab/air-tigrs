import drmaa

from typing import TYPE_CHECKING
from typing import Optional, List

from airflow.executors.base_executor import BaseExecutor, NOT_STARTED_MESSAGE
from airflow.exceptions import AirflowException

import airflow.executors.config_adapters as adapters
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.session import provide_session
from airflow.utils.state import State
from airflow.models import Variable

if TYPE_CHECKING:
    from airflow.models.taskinstance import (TaskInstanceKey)
    from airflow.executors.base_executor import CommandType
    from sqlalchemy.orm import Session

JOB_STATE_MAP = {
    drmaa.JobState.QUEUED_ACTIVE: State.QUEUED,
    drmaa.JobState.RUNNING: State.RUNNING,
    drmaa.JobState.DONE: State.SUCCESS,
    drmaa.JobState.FAILED: State.FAILED
}


class DRMAAV1Executor(BaseExecutor, LoggingMixin):
    """
    Submit jobs to an HPC cluster using the DRMAA v1 API
    """
    def __init__(self, max_concurrent_jobs: Optional[int] = None):
        super().__init__()

        self.max_concurrent_jobs: Optional[int] = max_concurrent_jobs
        self.active_jobs: int = 0
        self.jobs_submitted: int = 0
        self.session: Optional[drmaa.Session] = None

    # TODO: Make `scheduler_job_ids` configurable under [executor]
    @provide_session
    def _get_or_create_job_ids(self,
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
    def _update_job_tracker(self,
                            jobs: List[int],
                            session: Optional[Session] = None) -> None:
        write_json = {"jobs": jobs}
        Variable.update("scheduler_job_ids",
                        write_json,
                        serialize_json=True,
                        session=session)

    def _drop_from_tracking(self, job_id: int) -> None:
        self.log.info(
            "Removing Job {job_id} from tracking variable `scheduler_job_ids`")
        new_state = [j for j in self._get_or_create_job_ids() if j != job_id]
        self._update_job_tracker(new_state)
        self.log.info("Successfully removed {job_id} from `scheduler_job_ids`")

    def _push_to_tracking(self, job_id: int) -> None:
        self.log.info(
            "Adding Job {job_id} to tracking variable `scheduler_job_ids`")
        current_jobs = self._get_or_create_job_ids()
        current_jobs.append(job_id)
        self._update_job_tracker(current_jobs)
        self.log.info("Successfully added {job_id} to `scheduler_job_ids`")

    def start(self) -> None:
        self.log.info("Initializing DRMAA session")
        self.session = drmaa.Session()
        self.session.initialize()

        self.log.info(
            "Getting job tracking Airflow Variable: `scheduler_job_ids`")
        current_jobs = self._get_or_create_job_ids()

        if current_jobs:
            print_jobs = "\n".join([j_id for j_id in current_jobs["jobs"]])
            self.log.info(f"Jobs from previous session:\n{print_jobs}")
        else:
            self.log.info("No jobs are currently being tracked")

    def end(self) -> None:
        self.log.info("Cleaning up remaining job statuses")
        self.sync()

        self.log.info("Terminating DRMAA session")
        self.session.exit()

    def sync(self) -> None:
        """
        Called periodically by `airflow.executors.base_executor.BaseExecutor`'s
        heartbeat.

        Read the current state of tasks in the scheduler and update the metaDB
        """

        # Go through currently running jobs and update state
        scheduled_jobs = self._get_or_create_job_ids()
        for job_id in scheduled_jobs:
            drmaa_status = self.jobStatus(job_id)
            try:
                status = JOB_STATE_MAP[drmaa_status]
            except KeyError:
                self.log.info(
                    "Got unexpected state {drmaa_status} for job #{job_id}"
                    " Cannot be mapped into an Airflow TaskInstance State"
                    " Will try again in next sync attempt...")
            else:
                self.change_state(status, None)
                self._drop_from_tracking(job_id)

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

        self.log.info(f"Submitting job {key} with command {command} with"
                      f" configuration options:\n{executor_config})")
        jt = executor_config.drm2drmaa(self.session.createJobTemplate())

        # CommandType always begins with "airflow" binary command
        jt.remoteCommand = command[0]

        # args to airflow follow
        jt.args = command[1:]

        job_id = self.session.runJob(jt)
        self.log.info(f"Submitted Job {job_id}")
        self._push_to_tracking(job_id)

        # Prevent memory leaks on C back-end, running jobs unaffected
        # https://drmaa-python.readthedocs.io/en/latest/drmaa.html
        self.session.deleteJobTemplate(jt)
