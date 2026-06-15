from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from pathlib import Path
from threading import RLock
from typing import Callable

import networkx as nx

from .context import JobContext
from .errors import InvalidGraphError, InvalidJobError, JobFailedError
from .models import (
    CANCELLED,
    DONE,
    FAILED,
    Job,
    NODE_COMPLETE_STATUSES,
    QUEUED,
    RUNNING,
    SKIPPED,
    SUCCESSFUL_JOB_TERMINAL_STATUSES,
)
from .node import JobNode, validate_non_negative_int, validate_positive_int
from .router import NodeRouter, import_modules_from_dir, routers_from_module
from .runners.direct import DirectRunner
from .runners.threaded import ThreadedRunner
from .storage import FileStorage


class MicroWorkflow:
    def __init__(
        self,
        project_dir: str | Path = "project",
        runner: str = "threaded",
    ):
        if runner == "thread":
            runner = "threaded"

        if runner not in {"prefect", "direct", "threaded"}:
            raise ValueError(f"Unknown runner: {runner}")

        self.storage = FileStorage(project_dir)
        self.runner = runner

        self.graph_obj = nx.DiGraph()
        self.nodes: dict[str, JobNode] = {}
        self.lock = RLock()
        self._included_router_ids: set[int] = set()

        # CLI safety controls. Normal library use keeps immediate autostarts.
        self.allowed_run_nodes: set[str] | None = None
        self.autostart_mode = "immediate"

    def graph(self, edges: list[tuple[str, str]]):
        with self.lock:
            for start, end in edges:
                self.ensure_node(start)
                self.ensure_node(end)
                self.graph_obj.add_edge(start, end)

            if not nx.is_directed_acyclic_graph(self.graph_obj):
                raise InvalidGraphError("Graph must be a DAG. Cycles are not allowed.")

            self.storage.write_graph(edges)

    def include_router(self, router):
        """Mount a NodeRouter or a module that exports router/routers.

        This is similar in spirit to FastAPI's app.include_router(...).
        """
        if isinstance(router, NodeRouter):
            router_id = id(router)

            if router_id in self._included_router_ids:
                return router

            router.mount_to(self)
            self._included_router_ids.add(router_id)
            return router

        found = routers_from_module(router)

        if not found:
            raise ValueError("include_router expected a NodeRouter or a module with router/routers")

        for item in found:
            self.include_router(item)

        return router

    def include_routers(self, *routers):
        for router in routers:
            self.include_router(router)

    def include_router_dir(
        self,
        directory: str | Path,
        package: str | None = None,
        recursive: bool = False,
    ):
        """Import every node file in a folder and mount its router.

        This supports the recommended layout:

            src/node_behavior/load_recipes.py
            src/node_behavior/make_card.py
            src/node_behavior/index_cards.py
        """
        modules = import_modules_from_dir(
            directory=directory,
            package=package,
            recursive=recursive,
        )

        for module in modules:
            for router in routers_from_module(module):
                self.include_router(router)

        return modules

    # Friendly aliases if you prefer thinking in nodes instead of routers.
    include_node = include_router
    include_nodes = include_routers
    include_node_dir = include_router_dir

    def ensure_node(self, name: str, max_threads: int = 5) -> JobNode:
        name = self.storage.validate_node_name(name)
        max_threads = validate_positive_int("max_threads", max_threads)

        with self.lock:
            if name not in self.nodes:
                self.nodes[name] = JobNode(name, max_threads=max_threads)
                self.graph_obj.add_node(name)
                self.storage.init_node_folders(name)

                if self.storage.get_node_status(name) is None:
                    self.storage.set_node_status(name, QUEUED)

            return self.nodes[name]

    def task(
        self,
        node_name: str,
        max_threads: int = 5,
        retries: int = 0,
        repeats: int = 1,
    ):
        max_threads_checked = validate_positive_int("max_threads", max_threads)
        retries_checked = validate_non_negative_int("retries", retries)
        repeats_checked = validate_positive_int("repeats", repeats)

        def decorator(fn: Callable):
            node = self.ensure_node(node_name, max_threads=max_threads_checked)
            node.max_threads = max_threads_checked
            node.mount_main(fn, retries=retries_checked, repeats=repeats_checked)

            assert node.main_task is not None

            self.storage.write_node_schema(
                node_name=node_name,
                allowed_params=node.main_task.allowed_params,
                required_params=node.main_task.required_params,
                retries=node.main_task.retries,
                repeats=node.main_task.repeats,
                fallbacks=node.fallback_order,
            )

            return fn

        return decorator

    def fallback(
        self,
        node_name: str,
        name: str | None = None,
        retries: int = 0,
        repeats: int = 1,
    ):
        retries_checked = validate_non_negative_int("retries", retries)
        repeats_checked = validate_positive_int("repeats", repeats)

        def decorator(fn: Callable):
            node = self.ensure_node(node_name)
            node.mount_fallback(
                handler=fn,
                name=name,
                retries=retries_checked,
                repeats=repeats_checked,
            )

            if node.main_task is not None:
                self.storage.write_node_schema(
                    node_name=node_name,
                    allowed_params=node.main_task.allowed_params,
                    required_params=node.main_task.required_params,
                    retries=node.main_task.retries,
                    repeats=node.main_task.repeats,
                    fallbacks=node.fallback_order,
                )

            return fn

        return decorator

    def validate_edge(self, from_node: str, to_node: str):
        if not self.graph_obj.has_edge(from_node, to_node):
            raise InvalidGraphError(f"{from_node} cannot create jobs on {to_node}")

    def start(
        self,
        node_name: str,
        job_id: int | None = None,
        autostart: bool = False,
        **params,
    ):
        return self.add_job(
            from_node=None,
            to_node=node_name,
            job_id=job_id,
            autostart=autostart,
            **params,
        )

    def add_job(
        self,
        from_node: str | None,
        to_node: str,
        job_id: int | None = None,
        autostart: bool = False,
        _parent_job_id: int | None = None,
        **params,
    ):
        if job_id is not None:
            self.storage.validate_job_id(job_id)

        if _parent_job_id is not None:
            self.storage.validate_job_id(_parent_job_id)

        if from_node is not None:
            self.validate_edge(from_node, to_node)

        if autostart and self.allowed_run_nodes is not None and to_node not in self.allowed_run_nodes:
            parent = f"{from_node}/{_parent_job_id}" if _parent_job_id is not None else str(from_node)
            raise InvalidGraphError(
                f"Autostart from {parent} to {to_node} was blocked because "
                f"{to_node} is outside the approved run set. "
                "Use mwf run/runfrom and approve detected autostarts, or include "
                "the target node in the run set. Dynamic autostarts may not be "
                "found by the static scanner."
            )

        node = self.ensure_node(to_node)

        with node.lock:
            node.validate_params(params)

            if job_id is None:
                job_id = self.storage.next_job_id(to_node)

            parent = None
            if from_node is not None:
                parent = {
                    "from_node": from_node,
                    "from_job_id": _parent_job_id,
                }

            job = Job(
                job_id=job_id,
                node_name=to_node,
                params=params,
                parent=parent,
            )

            self.storage.create_job(job)
            self.storage.set_node_status(to_node, QUEUED)

        if autostart and self.autostart_mode == "immediate":
            return self.run_job(
                node_name=to_node,
                job_id=job_id,
                ignore_readiness=True,
            )

        return job

    def node_complete(self, node_name: str) -> bool:
        return self.storage.get_node_status(node_name) in NODE_COMPLETE_STATUSES

    def node_ready(self, node_name: str) -> bool:
        predecessors = set(self.graph_obj.predecessors(node_name))
        return all(self.node_complete(node) for node in predecessors)

    def refresh_node_status(self, node_name: str, allow_complete: bool = False):
        """Refresh a node's status from its jobs without unsafe early completion.

        A single job finishing does not mean the node is complete. This matters
        for autostarted downstream jobs: a node can receive and finish one job
        before all of its predecessor nodes have finished generating every job
        that should flow into it.

        Therefore DONE is only written when allow_complete=True and the node is
        actually ready, meaning all predecessor nodes are complete.
        """
        rows = self.storage.list_jobs(node_name)

        if not rows:
            if self.storage.get_node_status(node_name) is None:
                self.storage.set_node_status(node_name, QUEUED)
            return

        statuses = {row.get("status") for row in rows}

        if FAILED in statuses:
            self.storage.set_node_status(node_name, FAILED)
            return

        if RUNNING in statuses:
            self.storage.set_node_status(node_name, RUNNING)
            return

        if QUEUED in statuses:
            self.storage.set_node_status(node_name, QUEUED)
            return

        if statuses and statuses.issubset(SUCCESSFUL_JOB_TERMINAL_STATUSES):
            if allow_complete and self.node_ready(node_name):
                self.storage.set_node_status(node_name, DONE)
            else:
                # Finished jobs are not enough to complete the node if earlier
                # predecessor nodes may still create more jobs for this node.
                self.storage.set_node_status(node_name, QUEUED)
            return

        self.storage.set_node_status(node_name, QUEUED)

    def finalize_ready_nodes(self):
        for node_name in self.graph_obj.nodes:
            if self.node_ready(node_name):
                self.refresh_node_status(node_name, allow_complete=True)

    def ready_nodes(self) -> list[str]:
        self.finalize_ready_nodes()
        ready = []

        for node_name in self.graph_obj.nodes:
            queued = self.storage.queued_jobs(node_name)

            if queued and self.node_ready(node_name):
                ready.append(node_name)

        return ready

    def make_runner(self, node: JobNode):
        if self.runner == "direct":
            return DirectRunner()

        if self.runner == "threaded":
            return ThreadedRunner(max_threads=node.max_threads)

        if self.runner == "prefect":
            try:
                from .runners.prefect_thread import PrefectThreadRunner
            except ModuleNotFoundError as error:
                raise ModuleNotFoundError(
                    "runner='prefect' needs Prefect. Install it with: pip install prefect"
                ) from error

            return PrefectThreadRunner(max_threads=node.max_threads)

        raise ValueError(f"Unknown runner: {self.runner}")

    def run(self):
        if self.runner == "threaded":
            return self.run_concurrently()

        ran = []

        while True:
            ready = self.ready_nodes()

            if not ready:
                break

            for node_name in ready:
                self.run_node(node_name)
                ran.append(node_name)

        return ran

    def run_concurrently(
        self,
        nodes: list[str] | None = None,
        ready_check: Callable[[str], bool] | None = None,
    ) -> list[str]:
        """Run ready nodes concurrently, and jobs inside each node concurrently.

        This is the dependency-free local equivalent of the Prefect-based mode,
        but it also schedules multiple ready nodes at the same time. Each node
        still uses its own max_threads value for jobs inside that node.
        """
        selected = list(nodes) if nodes is not None else list(self.graph_obj.nodes)
        selected_set = set(selected)
        if not selected:
            return []

        def default_ready_check(node_name: str) -> bool:
            return self.node_ready(node_name)

        check = ready_check or default_ready_check
        max_workers = max(1, len(selected))
        ran: list[str] = []
        in_flight: set[str] = set()
        futures = {}

        with ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix="mwf-node",
        ) as executor:
            while True:
                self.finalize_ready_nodes()

                ready = [
                    node_name
                    for node_name in selected
                    if node_name not in in_flight
                    and self.storage.queued_jobs(node_name)
                    and check(node_name)
                ]

                for node_name in ready:
                    future = executor.submit(
                        self.run_node,
                        node_name,
                        True,
                    )
                    futures[future] = node_name
                    in_flight.add(node_name)

                if not futures:
                    break

                done, _ = wait(futures, return_when=FIRST_COMPLETED)

                for future in done:
                    node_name = futures.pop(future)
                    in_flight.remove(node_name)

                    try:
                        future.result()
                    except Exception:
                        for pending in futures:
                            pending.cancel()
                        wait(futures)
                        raise

                    ran.append(node_name)

        self.finalize_ready_nodes()
        return ran

    def run_node(self, node_name: str, ignore_readiness: bool = False):
        if not ignore_readiness and not self.node_ready(node_name):
            raise InvalidGraphError(f"Node {node_name} is not ready yet")

        node = self.nodes[node_name]
        jobs = self.storage.queued_jobs(node_name)

        if not jobs:
            self.refresh_node_status(node_name, allow_complete=True)
            return []

        self.storage.set_node_status(node_name, RUNNING)

        runner = self.make_runner(node)

        try:
            result = runner.run_jobs(
                node_name=node_name,
                jobs=jobs,
                run_one=lambda job: self.run_job(
                    node_name=job.node_name,
                    job_id=job.job_id,
                    ignore_readiness=True,
                ),
            )

        except Exception:
            self.storage.set_node_status(node_name, FAILED)
            raise

        self.refresh_node_status(node_name, allow_complete=True)
        return result

    def run_job(
        self,
        node_name: str,
        job_id: int,
        ignore_readiness: bool = False,
    ):
        if not ignore_readiness and not self.node_ready(node_name):
            raise InvalidGraphError(f"Node {node_name} is not ready yet")

        job = self.storage.load_job(node_name, job_id)
        node = self.nodes[node_name]

        if node.main_task is None:
            raise InvalidJobError(f"Node {node_name} has no mounted task")

        self.storage.set_job_status(node_name, job_id, RUNNING)

        try:
            result = self.execute_with_fallbacks(job)
            stored_files = self.storage.store_returned_files(node_name, job_id, result)

            self.storage.write_output(
                node_name,
                job_id,
                {
                    "status": DONE,
                    "stored_files": stored_files,
                    "result_type": type(result).__name__,
                    "result_repr": repr(result),
                },
            )

            self.storage.set_job_status(node_name, job_id, DONE)

            if self.storage.get_node_status(node_name) != RUNNING:
                self.refresh_node_status(node_name, allow_complete=False)

            return result

        except Exception as error:
            self.storage.write_debug(
                node_name,
                f"job {job_id} failed: {error}",
            )

            self.storage.write_output(
                node_name,
                job_id,
                {
                    "status": FAILED,
                    "error": repr(error),
                },
            )

            self.storage.set_job_status(node_name, job_id, FAILED)

            if self.storage.get_node_status(node_name) != RUNNING:
                self.refresh_node_status(node_name, allow_complete=False)

            raise JobFailedError(f"Job {node_name}/{job_id} failed") from error

    def execute_with_fallbacks(self, job: Job):
        node = self.nodes[job.node_name]
        assert node.main_task is not None

        try:
            return self.execute_mounted_task(job, node.main_task)

        except Exception as main_error:
            self.storage.write_debug(
                job.node_name,
                f"job {job.job_id} main task failed: {main_error}",
            )

            for fallback_name in node.fallback_order:
                fallback = node.fallbacks[fallback_name]

                self.storage.write_debug(
                    job.node_name,
                    f"job {job.job_id} trying fallback {fallback_name}",
                )

                try:
                    return self.execute_mounted_task(
                        job,
                        fallback,
                        previous_error=main_error,
                    )

                except Exception as fallback_error:
                    self.storage.write_debug(
                        job.node_name,
                        f"job {job.job_id} fallback {fallback_name} failed: {fallback_error}",
                    )

            raise main_error

    def execute_mounted_task(
        self,
        job: Job,
        mounted,
        previous_error: Exception | None = None,
    ):
        attempts = mounted.retries + 1
        all_results = []

        for attempt in range(1, attempts + 1):
            try:
                repeat_results = []

                for repeat_index in range(1, mounted.repeats + 1):
                    ctx = JobContext(
                        system=self,
                        current_node=job.node_name,
                        current_job=job,
                        current_task=mounted.name,
                        attempt=attempt,
                        repeat_index=repeat_index,
                        error=previous_error,
                    )

                    params = {
                        key: value
                        for key, value in job.params.items()
                        if key in mounted.allowed_params
                    }

                    if "error" in mounted.allowed_params:
                        params["error"] = previous_error

                    missing = mounted.required_params - set(params)
                    if missing:
                        raise InvalidJobError(
                            f"Missing params for {job.node_name}.{mounted.name}: {missing}"
                        )

                    result = mounted.handler(ctx, **params)
                    repeat_results.append(result)

                all_results.extend(repeat_results)
                return all_results[0] if len(all_results) == 1 else all_results

            except Exception as error:
                if attempt < attempts:
                    self.storage.write_debug(
                        job.node_name,
                        f"job {job.job_id} retrying {mounted.name} "
                        f"attempt {attempt + 1}/{attempts}: {error}",
                    )
                    continue

                raise

    def run_one(self, node_name: str, **params):
        job = self.start(
            node_name,
            autostart=False,
            **params,
        )

        result = self.run_job(
            node_name=node_name,
            job_id=job.job_id,
            ignore_readiness=True,
        )
        self.refresh_node_status(node_name, allow_complete=True)
        return result

    def run_node_once(self, node_name: str):
        return self.run_node(
            node_name,
            ignore_readiness=True,
        )

    def list_jobs(self, node_name: str, status: str | None = None):
        return self.storage.list_jobs(node_name, status=status)

    def cancel_job(self, node_name: str, job_id: int):
        self.storage.set_job_status(node_name, job_id, CANCELLED)
        self.refresh_node_status(node_name, allow_complete=False)

    def retry_job(self, node_name: str, job_id: int):
        self.storage.set_job_status(node_name, job_id, QUEUED)
        self.storage.set_node_status(node_name, QUEUED)

    def skip_node(self, node_name: str):
        self.storage.set_node_status(node_name, SKIPPED)

    def mark_node_done(self, node_name: str):
        self.storage.set_node_status(node_name, DONE)

    def input_dir(self, node_name: str) -> Path:
        return self.storage.node_input_dir(node_name)

    def output_dir(self, node_name: str) -> Path:
        return self.storage.node_output_dir(node_name)