import drmaa

from typing import TYPE_CHECKING
from typing import Optional, Any
from multiprocessing import Queue, Empty

from airflow.executors.base_executor import BaseExecutor
from airflow.exceptions import AirflowException

if TYPE_CHECKING:
    from airflow.models.taskinstance import (TaskInstanceKey,
                                             TaskInstanceStateType)
    from airflow.executors.base_executor import CommandType
    from airflow.executors.local_executor import ExecutorWorkType


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
        self.queue = Optional[Queue[ExecutorWorkType]] = None
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
            raise AirflowException("Executor needs to be initialized "
                                   "with .start()!")

        while not True:
            try:
                results = self.results_queue.get_nowait()
                try:
                    self.change_state(*results)
                except Exception:
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
        raise NotImplementedError
