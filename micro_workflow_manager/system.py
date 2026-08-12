from pathlib import Path
from threading import RLock

import networkx as nx

from .workflow.component_scheduler import ComponentSchedulerMixin
from .workflow.component_state import ComponentStateMixin
from .workflow.dag_scheduler import DagSchedulerMixin
from .workflow.job_creation import JobCreationMixin
from .workflow.job_execution import JobExecutionMixin
from .node import JobNode
from .workflow.runner_config import RunnerFactoryMixin, normalize_workflow_runner
from .workflow.supervisor import SchedulerSupervisor
from .storage import FileStorage
from .workflow.workflow_registration import WorkflowRegistrationMixin
from .fibers import FiberLocal


class MicroWorkflow(
    WorkflowRegistrationMixin,
    JobCreationMixin,
    ComponentStateMixin,
    RunnerFactoryMixin,
    ComponentSchedulerMixin,
    DagSchedulerMixin,
    JobExecutionMixin,
):
    def __init__(
        self,
        project_dir: str | Path = "project",
        runner: str = "threaded",
        process_graph_path: str | Path | None = None,
        *,
        persist_graph: bool = True,
        initialize_node_folders: bool = True,
    ):
        runner = normalize_workflow_runner(runner)

        self.storage = FileStorage(project_dir)
        self.runner = runner
        self.process_graph_path = (
            Path(process_graph_path).resolve()
            if process_graph_path is not None
            else None
        )

        self.persist_graph = bool(persist_graph)
        self.initialize_node_folders = bool(initialize_node_folders)

        self.graph_obj = nx.DiGraph()
        # Explicit autostart edges add reverse reachability when constructing
        # Hoeflein components. Ordinary graph edges retain their direction.
        self.autostart_edges: set[tuple[str, str]] = set()
        self.nodes: dict[str, JobNode] = {}
        self.lock = RLock()
        self._included_routers: set[object] = set()
        self.scheduler_supervisor = SchedulerSupervisor(self)

        # Runtime max_threads overrides are local testing controls stored in
        # .mwf/threads.json. The cache is refreshed only when that one file's
        # stat signature changes, so active runners do not repeatedly parse JSON.
        self._thread_override_lock = RLock()
        self._thread_override_signature: tuple[int, int, int, int] | None | object = object()
        self._thread_overrides: dict[str, int] = {}
        self._api_total_limit: int | None = None

        # CLI safety controls. Normal library use keeps immediate autostarts.
        self.allowed_run_nodes: set[str] | None = None
        self.autostart_mode = "immediate"
        # The CLI enables generation-fenced job restarts for active run/runfrom
        # sessions. Programmatic workflows keep the original direct execution
        # path unless they explicitly opt in, avoiding supervisory thread and
        # filesystem-polling overhead.
        self.active_job_restart_enabled = False

        # Job-spawn context. A task may create more jobs with autostart=True,
        # but those spawned jobs must be treated like newly-created entities in
        # a game loop: enqueue them and let the component scheduler run them.
        # Running them recursively from inside the parent job can deadlock a
        # cyclic component when every worker is waiting for a child worker.
        self._job_context = FiberLocal()
        # Non-fatal graph/router configuration reminders collected while routers
        # mount. CLI loading prints these to stderr once per invocation.
        self.configuration_notices: list[str] = []
    def _refresh_runtime_limits(self) -> dict[str, int]:
        path = self.storage.thread_overrides_file()
        try:
            stat = path.stat()
            signature: tuple[int, int, int, int] | None = (
                stat.st_mtime_ns,
                stat.st_ctime_ns,
                stat.st_size,
                stat.st_ino,
            )
        except FileNotFoundError:
            signature = None

        with self._thread_override_lock:
            if signature == self._thread_override_signature:
                return self._thread_overrides
            state = self.storage.read_runtime_limit_state()
            self._thread_overrides = dict(state["overrides"])
            self._api_total_limit = state.get("api_total_limit")
            self._thread_override_signature = signature
            return self._thread_overrides

    def thread_override(self, node_name: str) -> int | None:
        return self._refresh_runtime_limits().get(node_name)

    def api_total_limit_override(self) -> int | None:
        """Return the run-scoped aggregate API admission budget, if configured."""
        self._refresh_runtime_limits()
        return self._api_total_limit

    def effective_api_total_limit(self) -> int:
        """Aggregate requested capacity after an optional proportional budget."""
        limits = self._effective_api_node_limits()
        return max(1, sum(limits.values()))

    def _effective_api_node_limits(self) -> dict[str, int]:
        overrides = self._refresh_runtime_limits()
        allowed = self.allowed_run_nodes
        requested = {
            name: overrides.get(name, node.max_threads)
            for name, node in self.nodes.items()
            if (node.runner_override or self.runner) == "api"
            and (allowed is None or name in allowed)
        }
        if not requested:
            return {}

        configured = self._api_total_limit
        requested_total = sum(requested.values())
        if configured is None or configured >= requested_total:
            return requested
        if configured < len(requested):
            raise ValueError(
                "aggregate API limit must be at least the number of active API nodes "
                f"({len(requested)})"
            )

        shares = {
            name: max(1, (configured * value) // requested_total)
            for name, value in requested.items()
        }
        used = sum(shares.values())
        remainders = sorted(
            requested,
            key=lambda name: (
                -((configured * requested[name]) % requested_total),
                name,
            ),
        )
        while used < configured:
            changed = False
            for name in remainders:
                if shares[name] >= requested[name]:
                    continue
                shares[name] += 1
                used += 1
                changed = True
                if used == configured:
                    break
            if not changed:
                break
        while used > configured:
            changed = False
            for name in reversed(remainders):
                if shares[name] <= 1:
                    continue
                shares[name] -= 1
                used -= 1
                changed = True
                if used == configured:
                    break
            if not changed:
                break
        return shares

    def effective_max_threads(self, node_name: str) -> int:
        node = self.nodes[node_name]
        effective_runner = node.runner_override or self.runner
        if effective_runner == "direct":
            return 1
        if effective_runner == "api" and self.api_total_limit_override() is not None:
            return self._effective_api_node_limits().get(node_name, node.max_threads)
        override = self.thread_override(node_name)
        return override if override is not None else node.max_threads

    def invalidate_thread_override_cache(self) -> None:
        with self._thread_override_lock:
            self._thread_override_signature = object()
