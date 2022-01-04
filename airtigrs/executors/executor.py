"""
Custom Airflow executor implementations
"""

from enum import Enum

from airflow.executors.base_executor import BaseExecutor
from airflow.exceptions import AirflowException
from airtigrs.executors.drmaa_executor import DRMAAExecutor

from typing import TYPE_CHECKING
from typing import Optional, Any, List, Tuple

if TYPE_CHECKING:
    from airflow.models.taskinstance import (TaskInstanceKey,
                                             TaskInstanceStateType)
    from airflow.executors.base_executor import CommandType
    from airflow.executors.local_executor import (ExecutorWorkType,
                                                  QueuedLocalWorker,
                                                  LocalExecutor)
    from multiprocessing import Queue


class JobType(Enum):
    LOCAL = 1
    SLURM = 2


# Store additional information about how task was run
TIStateJobType = Tuple[TaskInstanceStateType, JobType]


class HybridDRMAAExecutor(BaseExecutor):
    """Hybrid Executor implementing both Local and DRMAA jobs
    """
    def __init__(self, local_executor: LocalExecutor,
                 slurm_executor: DRMAAExecutor):

        self.workers: List[QueuedLocalWorker] = []
        self.slurm_jobs_running: int = 0
        self.max_concurrent_jobs: int = 0

        self.results_queue: Optional[Queue[TIStateJobType]] = None
        self.slurm_queue: Optional[Queue[ExecutorWorkType]] = None

        self.local_executor = local_executor
        self.slurm_executor = slurm_executor

    @property
    def workers_active(self) -> int:
        '''
        Get total number of jobs currently being run

        Returns:
            int: Number of active jobs
        '''
        return self.local_workers_active + self.slurm_jobs_running

    def sync(self) -> None:
        '''
        Track state of completed jobs
        '''
        if not self.results_queue:
            raise AirflowException("Executor needs to be started!")

        while not self.results_queue.empty():
            results, job_type = self.results_queue.get()
            self.change_state(*results)

            if job_type == JobType.LOCAL:
                self.local_workers_active -= 1
            elif job_type == JobType.SLURM:
                self.slurm_jobs_running -= 1

    def execute_async(self,
                      key: TaskInstanceKey,
                      command: CommandType,
                      queue: Optional[str] = None,
                      executor_config: Optional[Any] = None) -> None:
        raise NotImplementedError

    def start(self) -> None:
        self.local_executor.start()
        self.slurm_executor.start()

    def end(self) -> None:
        self.local_executor.end()
        self.slurm_executor.end()
