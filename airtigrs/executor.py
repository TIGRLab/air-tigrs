"""
Custom Airflow executor implementations
"""

from airflow.executors.base_executor import BaseExecutor, QueuedLocalWorker

from typing import TYPE_CHECKING
from typing import Optional, Any, List

if TYPE_CHECKING:
    from airflow.models.taskinstance import (TaskInstanceKey,
                                             TaskInstanceStateType)
    from airflow.executors.base_executor import CommandType
    from airflow.executors.local_executor import ExecutorWorkType
    from multiprocessing import Queue


class HybridSlurmExecutor(BaseExecutor):
    def __init__(self):
        self.workers: List[QueuedLocalWorker] = []
        self.workers_used: int = 0
        self.slurm_jobs_running: int = 0
        self.max_concurrent_jobs: int = 0

        self.results_queue: Optional[Queue[TaskInstanceStateType]] = None
        self.slurm_queue: Optional[Queue[ExecutorWorkType]] = None
        self.local_queue: Optional[Queue[ExecutorWorkType]] = None

    @property
    def workers_active(self) -> int:
        '''
        Get total number of jobs currently being run

        Returns:
            int: Number of active jobs
        '''
        return self.local_workers_active + self.slurm_jobs_running

    def sync(self) -> None:
        raise NotImplementedError

    def execute_async(self,
                      key: TaskInstanceKey,
                      command: CommandType,
                      queue: Optional[str] = None,
                      executor_config: Optional[Any] = None) -> None:
        raise NotImplementedError

    def start(self) -> None:
        raise NotImplementedError

    def end(self) -> None:
        raise NotImplementedError
