from pathlib import Path
from threading import Lock, RLock

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
from .api_limits import allocate_api_capacity

_CURRENT_EXECUTION_CONTEXT = object()


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
        self._execution_entry_lock = Lock()
        self._included_routers: set[object] = set()
        self.scheduler_supervisor = SchedulerSupervisor(self)

        # Native per-node overrides are read through the owning execution
        # session. SQLite is the sole writable runtime-limit source.
        self._api_admission_lock = RLock()
        # Exact API nodes admitted by the live DAG scheduler. This is narrower
        # than allowed_run_nodes: completed and not-yet-started nodes must not
        # retain a share of a run-scoped aggregate admission budget.
        self._active_api_admission_nodes: frozenset[str] | None = None

        # CLI safety controls. Normal library use keeps immediate autostarts.
        self.allowed_run_nodes: set[str] | None = None
        self.autostart_mode = "immediate"
        self.execution_session_context = None

        # Job-spawn context. A task may create more jobs with autostart=True,
        # but those spawned jobs must be treated like newly-created entities in
        # a game loop: enqueue them and let the component scheduler run them.
        # Running them recursively from inside the parent job can deadlock a
        # cyclic component when every worker is waiting for a child worker.
        self._job_context = FiberLocal()
        # Non-fatal graph/router configuration reminders collected while routers
        # mount. CLI loading prints these to stderr once per invocation.
        self.configuration_notices: list[str] = []

    def execution_claim_context(self, node_name: str, *, context=_CURRENT_EXECUTION_CONTEXT):
        if context is _CURRENT_EXECUTION_CONTEXT:
            context = self.execution_session_context
        elif context is None:
            raise RuntimeError('Execution requires an explicit native session scope')
        if context is not self.execution_session_context:
            raise RuntimeError('Execution scope no longer belongs to this workflow')
        if context is None or node_name not in context[1]:
            raise RuntimeError(f'No active execution session owns node {node_name}')
        return context[0], context[1][node_name]

    def _runtime_limit_session_id(self, node_name: str | None = None):
        context = self.execution_session_context
        if context is None:
            return None
        if node_name is not None and node_name not in context[1]:
            return None
        return context[0]

    def thread_override(self, node_name: str) -> int | None:
        return self.storage.read_thread_override_for_session(
            node_name, self._runtime_limit_session_id(node_name),
        )

    def api_total_limit_override(self) -> int | None:
        """Return the project-wide aggregate API admission budget, if configured."""
        return self.storage.read_api_total_limit()

    def effective_api_total_limit(self) -> int:
        """Aggregate requested capacity after an optional proportional budget."""
        limits = self._effective_api_node_limits()
        return max(1, sum(limits.values()))

    def active_api_admission_nodes(self) -> frozenset[str] | None:
        with self._api_admission_lock:
            return self._active_api_admission_nodes

    def set_active_api_admission_nodes(self, nodes) -> None:
        with self._api_admission_lock:
            self._active_api_admission_nodes = (
                None if nodes is None else frozenset(nodes)
            )

    def _effective_api_node_limits(self) -> dict[str, int]:
        active = self.active_api_admission_nodes()
        allowed = active if active is not None else self.allowed_run_nodes
        requested = {
            name: self.requested_max_threads(name)
            for name, node in self.nodes.items()
            if (node.runner_override or self.runner) == "api"
            and (allowed is None or name in allowed)
        }
        if not requested:
            return {}

        return allocate_api_capacity(requested, self.api_total_limit_override())

    def requested_max_threads(self, node_name: str) -> int:
        """Return a node's requested limit before aggregate API allocation."""
        node = self.nodes[node_name]
        effective_runner = node.runner_override or self.runner
        if effective_runner == "direct":
            return 1
        override = self.thread_override(node_name)
        return override if override is not None else node.max_threads

    def effective_max_threads(self, node_name: str) -> int:
        node = self.nodes[node_name]
        effective_runner = node.runner_override or self.runner
        if effective_runner == "direct":
            return 1
        if effective_runner == "api" and self.api_total_limit_override() is not None:
            active = self.active_api_admission_nodes()
            if active is not None and node_name not in active:
                return 1
            return self._effective_api_node_limits().get(node_name, node.max_threads)
        return self.requested_max_threads(node_name)
