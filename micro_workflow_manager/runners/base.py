from abc import ABC, abstractmethod
from typing import Callable, Iterable


class BaseRunner(ABC):
    @abstractmethod
    def run_jobs(self, node_name: str, jobs: list, run_one: Callable):
        pass

    def run_job_source(self, node_name: str, job_source: Iterable, run_one: Callable):
        """Run jobs from a lazy source.

        Subclasses can override this to avoid materializing huge job lists. The
        default preserves compatibility for any future runner that only
        implements run_jobs(...).
        """
        return self.run_jobs(node_name, list(job_source), run_one)
