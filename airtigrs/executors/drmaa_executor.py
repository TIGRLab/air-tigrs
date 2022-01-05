import drmaa

from typing import TYPE_CHECKING
from typing import Optional, Any, Tuple, Dict
from multiprocessing import Queue, Empty

from airflow.executors.base_executor import BaseExecutor, NOT_STARTED_MESSAGE
from airflow.exceptions import AirflowException

if TYPE_CHECKING:
    from airflow.models.taskinstance import (TaskInstanceKey,
                                             TaskInstanceStateType)
    from airflow.executors.base_executor import CommandType

# (key, command, job spec)
DRMAAWorkType = Tuple[TaskInstanceKey, CommandType, Dict]


class DRMAAExecutor(BaseExecutor):
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
        self.session = drmaa.Session()
        self.session.initialize()

    def end(self) -> None:
        # TODO wait for jobs to complete? Maybe not since
        # we may be able to re-attach at any point?
        self.session.exit()

    def sync(self) -> None:
        """
        Called periodically by `airflow.executors.base_executor.BaseExecutor`'s
        heartbeat.

        Read the current state of tasks in results_queue and update the metaDB
        """
        if self.results_queue is None:
            raise AirflowException(NOT_STARTED_MESSAGE)

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

    def execute_async(self,
                      key: TaskInstanceKey,
                      command: CommandType,
                      queue: Optional[str] = None,
                      executor_config: Optional[Any] = None) -> None:
        '''
        Submit slurm job and keep track of submission
        '''
        if self.task_queue is None:
            raise AirflowException(NOT_STARTED_MESSAGE)

        # Implements checks?
        # Submit jobs immediately?
        # Yeah why not... can implement limits later...

        self.task_queue.put((key, command, queue, executor_config))

        # TODO: Track task info
        # Sync step should run jobs in a batched fashion
        # If using unlimited, the allow submission directly to the queue
        # using DRMAA API
        jt = self.session.createJobTemplate()
        jt.remoteCommand = command[0]
        jt.args = command[1:]

        # Need to configure job attributes
        # We have a bunch of recognized stuff and a bunch
        # of unrecognized stuff

