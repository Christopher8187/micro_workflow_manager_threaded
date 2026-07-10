from pathlib import Path
from threading import RLock, local

import networkx as nx

from .workflow.component_scheduler import ComponentSchedulerMixin
from .workflow.component_state import ComponentStateMixin
from .workflow.dag_scheduler import DagSchedulerMixin
from .workflow.job_creation import JobCreationMixin
from .workflow.job_execution import JobExecutionMixin
from .node import JobNode
from .workflow.runner_config import RunnerFactoryMixin, normalize_workflow_runner
from .storage import FileStorage
from .workflow.workflow_registration import WorkflowRegistrationMixin


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
        self.nodes: dict[str, JobNode] = {}
        self.lock = RLock()
        self._included_router_ids: set[int] = set()

        # CLI safety controls. Normal library use keeps immediate autostarts.
        self.allowed_run_nodes: set[str] | None = None
        self.autostart_mode = "immediate"

        # Job-spawn context. A task may create more jobs with autostart=True,
        # but those spawned jobs must be treated like newly-created entities in
        # a game loop: enqueue them and let the component scheduler run them.
        # Running them recursively from inside the parent job can deadlock a
        # cyclic component when every worker is waiting for a child worker.
        self._job_context = local()
