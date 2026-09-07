from __future__ import annotations

from pathlib import Path

import networkx as nx

from micro_workflow_manager.models import NODE_COMPLETE_STATUSES
from micro_workflow_manager.paths import state_database_file
from micro_workflow_manager.project_format import (
    is_link_or_reparse_point, read_native_project_config,
)
from micro_workflow_manager.topology import ComponentTopology
from micro_workflow_manager.storage.sqlite.preview_snapshot import open_preview_snapshot

from .autostart_scan import scan_autostarts
from .engine import _stored_edges
from .project import resolve_stored_graph_path


class PreviewStorage:
    """Read persisted metadata without creating storage, nodes, or jobs."""

    def __init__(self, root: Path):
        read_native_project_config(root)
        self.project_dir = root
        self.connection = None
        database = state_database_file(root)
        if is_link_or_reparse_point(database) or not database.is_file():
            raise RuntimeError("Native MWF project state is missing or is not an ordinary file")
        self.connection = open_preview_snapshot(database)

    def close(self) -> None:
        if self.connection is not None:
            self.connection.close()
            self.connection = None

    def get_node_status(self, node: str) -> str | None:
        row = self.connection.execute("SELECT status FROM nodes WHERE node_name=?", (node,)).fetchone()
        return None if row is None else row["status"]

    def job_exists(self, node: str, job_id: int) -> bool:
        return self.connection.execute(
            "SELECT 1 FROM jobs WHERE node_name=? AND job_id=?", (node, job_id),
        ).fetchone() is not None

    def get_job_status(self, node: str, job_id: int) -> str | None:
        row = self.connection.execute(
            "SELECT status FROM jobs WHERE node_name=? AND job_id=?", (node, job_id),
        ).fetchone()
        return None if row is None else row["status"]

    def node_job_summary(self, node: str) -> dict:
        rows = self.connection.execute(
            "SELECT status, COUNT(*) AS count FROM jobs WHERE node_name=? GROUP BY status",
            (node,),
        )
        return {"counts": {row["status"]: row["count"] for row in rows}}


class PreviewWorkflow:
    """Topology and persisted observations for a plan, without a runtime."""

    read_only = True

    def __init__(self, root: Path):
        config = read_native_project_config(root)
        self.graph_obj = nx.DiGraph(_stored_edges(config))
        self.autostart_edges: set[tuple[str, str]] = set()
        graph_path = config.get("graph_path")
        if graph_path:
            directory = resolve_stored_graph_path(root, graph_path).parent / "node_behavior"
            self.autostart_edges = {
                (start, end)
                for start, targets in scan_autostarts(directory).items()
                for end in targets
                if self.graph_obj.has_edge(start, end)
            }
        self.topology = ComponentTopology(self.graph_obj, self.autostart_edges)
        self.storage = PreviewStorage(root)

    def node_complete(self, node: str) -> bool:
        return self.storage.get_node_status(node) in NODE_COMPLETE_STATUSES

    def node_ready(self, node: str) -> bool:
        return all(self.node_complete(previous) for previous in
                   self.component_predecessors(self.component_for(node)))

    def component_key(self, component):
        return self.topology.component_key(component)

    def component_id(self, node_or_component):
        return self.topology.component_id(node_or_component)

    def component_for(self, node: str):
        return self.topology.component_for(node)

    def component_descendants(self, component):
        return self.topology.component_descendants(component)

    def component_predecessors(self, component):
        return self.topology.component_predecessors(component)

    def component_interval(self, start: str, end: str):
        return self.topology.component_interval(start, end)

    def execution_components(self, nodes=None):
        return self.topology.execution_components(nodes)


def load_preview(root: Path) -> PreviewWorkflow:
    return PreviewWorkflow(root)
