from __future__ import annotations

import importlib.util
import multiprocessing as mp
import pickle
import sys
from concurrent.futures import FIRST_COMPLETED, FIRST_EXCEPTION, ProcessPoolExecutor, wait
from pathlib import Path
from types import ModuleType
from typing import Callable, Iterable

from .base import BaseRunner

_PROCESS_WORKFLOW = None


def _import_graph_file(path: Path) -> ModuleType:
    path = Path(path).resolve()
    root = path.parent

    for item in [root.parent, root]:
        text = str(item)
        if text not in sys.path:
            sys.path.insert(0, text)

    module_name = "mwf_process_graph"
    spec = importlib.util.spec_from_file_location(module_name, path)

    if spec is None or spec.loader is None:
        raise ImportError(path)

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def _read_edges(module: ModuleType) -> list[tuple[str, str]]:
    if hasattr(module, "EDGES"):
        return list(module.EDGES)

    if hasattr(module, "edges"):
        value = module.edges
        return list(value() if callable(value) else value)

    raise RuntimeError("Graph file must define EDGES or edges()")


def _init_process_worker(
    project_dir: str,
    graph_path: str,
    allowed_run_nodes: tuple[str, ...] | None,
    autostart_mode: str,
):
    """Build a fresh workflow inside each worker process.

    Process workers cannot safely reuse the parent process' Python objects. They
    reconstruct the workflow by importing the same graph and node_behavior files
    that the CLI uses. This is why process mode is intended for normal project
    files, not anonymous in-memory functions.
    """
    from micro_workflow_manager.system import MicroWorkflow

    global _PROCESS_WORKFLOW

    project = Path(project_dir).resolve()
    graph_file = Path(graph_path).resolve()
    module = _import_graph_file(graph_file)
    workflow = MicroWorkflow(
        project_dir=project,
        runner="direct",
        process_graph_path=graph_file,
    )
    workflow.graph(_read_edges(module))
    workflow.include_node_dir(graph_file.parent / "node_behavior")
    workflow.allowed_run_nodes = None if allowed_run_nodes is None else set(allowed_run_nodes)
    workflow.autostart_mode = autostart_mode

    _PROCESS_WORKFLOW = workflow


def _run_job_in_initialized_process(node_name: str, job_id: int):
    if _PROCESS_WORKFLOW is None:
        raise RuntimeError("Process worker was not initialized with a workflow")

    result = _PROCESS_WORKFLOW.run_job(
        node_name=node_name,
        job_id=job_id,
        ignore_readiness=True,
    )

    try:
        pickle.dumps(result)
    except Exception as error:
        raise TypeError(
            "Process runner job results must be pickleable. "
            "The job output files and output.json were already written, but the "
            "Python return value could not be sent back to the parent process. "
            "Return a JSON-like value, string, number, list, dict, or Path instead."
        ) from error

    return result


class ProcessPoolRunner(BaseRunner):
    """Run node jobs in a local ProcessPoolExecutor.

    This mirrors ThreadedRunner's public behavior, but each job runs in a child
    Python process. Because child processes must reconstruct the workflow, this
    runner needs the graph file path used by the CLI or by MicroWorkflow(...,
    process_graph_path=...).
    """

    def __init__(
        self,
        max_processes: int,
        *,
        project_dir: str | Path | None = None,
        graph_path: str | Path | None = None,
        allowed_run_nodes: set[str] | None = None,
        autostart_mode: str = "immediate",
    ):
        if type(max_processes) is not int or max_processes < 1:
            raise ValueError("max_processes must be an integer >= 1")

        self.max_processes = max_processes
        self.project_dir = Path(project_dir).resolve() if project_dir is not None else None
        self.graph_path = Path(graph_path).resolve() if graph_path is not None else None
        self.allowed_run_nodes = None if allowed_run_nodes is None else tuple(sorted(allowed_run_nodes))
        self.autostart_mode = autostart_mode

    def _require_project_loader(self):
        if self.project_dir is None or self.graph_path is None:
            raise RuntimeError(
                "The process runner needs a graph file so child processes can "
                "rebuild the workflow. Use the CLI (`mwf graph src/graph.py "
                "--runner process`) or construct MicroWorkflow with "
                "process_graph_path='src/graph.py'."
            )

    def _executor(self):
        self._require_project_loader()
        return ProcessPoolExecutor(
            max_workers=self.max_processes,
            mp_context=mp.get_context("spawn"),
            initializer=_init_process_worker,
            initargs=(
                str(self.project_dir),
                str(self.graph_path),
                self.allowed_run_nodes,
                self.autostart_mode,
            ),
        )

    def run_jobs(self, node_name: str, jobs: list, run_one: Callable):
        if not jobs:
            return []

        results_by_index = [None] * len(jobs)

        with self._executor() as executor:
            futures = {
                executor.submit(
                    _run_job_in_initialized_process,
                    node_name,
                    job.job_id,
                ): index
                for index, job in enumerate(jobs)
            }

            done, not_done = wait(futures, return_when=FIRST_EXCEPTION)

            first_error = None
            for future in done:
                index = futures[future]
                try:
                    results_by_index[index] = future.result()
                except Exception as error:
                    first_error = error
                    break

            if first_error is not None:
                for future in not_done:
                    future.cancel()
                wait(not_done)
                raise first_error

            for future in not_done:
                index = futures[future]
                results_by_index[index] = future.result()

        return results_by_index

    def run_job_source(self, node_name: str, job_source: Iterable, run_one: Callable):
        """Run jobs from a lazy source without loading every job first."""
        iterator = iter(job_source)
        futures = {}
        results = []
        source_exhausted = False

        with self._executor() as executor:
            def submit_one() -> bool:
                nonlocal source_exhausted

                if source_exhausted:
                    return False

                try:
                    job_id = next(iterator)
                except StopIteration:
                    source_exhausted = True
                    return False

                future = executor.submit(
                    _run_job_in_initialized_process,
                    node_name,
                    job_id,
                )
                futures[future] = job_id
                return True

            submit_one()

            while futures:
                while len(futures) < self.max_processes and submit_one():
                    pass

                done, _ = wait(futures, return_when=FIRST_COMPLETED)

                for future in done:
                    futures.pop(future)

                    try:
                        results.append(future.result())
                    except Exception as error:
                        for pending in futures:
                            pending.cancel()
                        wait(futures)
                        raise error

        return results
