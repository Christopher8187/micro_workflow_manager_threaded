from abc import ABC, abstractmethod


class BaseRunner(ABC):
    @abstractmethod
    def run_jobs(self, node_name: str, jobs: list, run_one):
        pass