from __future__ import annotations

import networkx as nx

from ..errors import InvalidGraphError
from ..topology import ComponentTopology
from ..models import (
    CANCELLED,
    DONE,
    FAILED,
    NODE_COMPLETE_STATUSES,
    QUEUED,
    WAITING,
    RUNNING,
    SKIPPED,
)


class ComponentStateMixin:
    """Hoeflein-component dependency and lifecycle state.

    The project graph keeps its ordinary directed edges.  An explicitly declared
    autostart edge ``u -> v`` additionally contributes the reverse reachability
    arc ``v -> u`` for component construction.  Hoeflein components are the
    strongly connected components of that augmented graph.  The quotient keeps
    only the direction of the original graph edges and is therefore a DAG.
    """

    @property
    def topology(self) -> ComponentTopology:
        return ComponentTopology(self.graph_obj, self.autostart_edges)

    def set_autostart_edges(self, edges) -> None:
        normalized: set[tuple[str, str]] = set()
        for start, end in edges:
            start = self.storage.validate_node_name(start)
            end = self.storage.validate_node_name(end)
            if self.graph_obj.has_edge(start, end):
                normalized.add((start, end))
        self.autostart_edges = normalized

    def register_autostart_edge(self, start: str, end: str) -> None:
        if not self.graph_obj.has_edge(start, end):
            return
        self.autostart_edges.add((start, end))

    def explicit_autostart(self, start: str, end: str) -> bool:
        return (start, end) in self.autostart_edges

    def hoeflein_graph(self) -> nx.DiGraph:
        return self.topology.hoeflein_graph()

    def hoeflein_components(self) -> list[set[str]]:
        return self.topology.hoeflein_components()

    # Compatibility name. Scheduling now consistently means Hoeflein components.
    def strongly_connected_components(self) -> list[set[str]]:
        return self.hoeflein_components()

    def component_key(self, component: set[str] | tuple[str, ...] | list[str]) -> tuple[str, ...]:
        return self.topology.component_key(component)

    def component_for(self, node_name: str) -> set[str]:
        return self.topology.component_for(node_name)

    def component_id(self, node_or_component: str | set[str] | tuple[str, ...] | list[str]) -> tuple[str, ...]:
        return self.topology.component_id(node_or_component)

    def component_map(self) -> tuple[list[set[str]], dict[str, int]]:
        return self.topology.component_map()

    def component_dag(self) -> nx.DiGraph:
        return self.topology.component_dag()

    def component_descendants(self, component: set[str] | tuple[str, ...] | list[str]) -> list[tuple[str, ...]]:
        return self.topology.component_descendants(component)

    def component_interval(self, start_node: str, end_node: str) -> list[tuple[str, ...]]:
        return self.topology.component_interval(start_node, end_node)

    def component_is_cyclic(self, component: set[str]) -> bool:
        return self.topology.component_is_cyclic(component)

    def validate_waiting_configuration(self, node_name: str) -> None:
        """Validate one node's waiting declaration against its Hoeflein component."""
        node = self.nodes[node_name]
        if not node.waiting:
            return
        component = self.component_for(node_name)
        peers = component - {node_name}
        declared = node.wait_for
        if declared is not None:
            invalid_self = node_name in declared
            unknown = sorted(name for name in declared if name not in self.graph_obj)
            outside = sorted(name for name in declared if name in self.graph_obj and name not in peers)
            if invalid_self:
                raise InvalidGraphError(f"Waiting node {node_name} cannot wait for itself")
            if unknown:
                raise InvalidGraphError(
                    f"Waiting node {node_name} references unknown node(s): {', '.join(unknown)}"
                )
            if outside:
                raise InvalidGraphError(
                    f"Waiting node {node_name} can only wait for vertices in Hoeflein component "
                    f"{{{', '.join(sorted(component))}}}; outside target(s): {', '.join(outside)}"
                )
        if not peers:
            message = (
                f"Node {node_name} is declared waiting, but Hoeflein component "
                f"{{{node_name}}} is trivial. DAG-type nodes have no queue-independent "
                "waiting functionality; ordinary predecessor readiness still applies."
            )
            notices = getattr(self, "configuration_notices", None)
            if notices is not None and message not in notices:
                notices.append(message)

    def waiting_dependencies(self, node_name: str) -> set[str]:
        node = self.nodes[node_name]
        if not node.waiting:
            return set()
        self.validate_waiting_configuration(node_name)
        peers = self.component_for(node_name) - {node_name}
        if node.wait_for is None:
            return peers
        return set(node.wait_for)

    def waiting_blockers(self, node_name: str) -> set[str]:
        """Peers with queued, running, or failed work that block this node."""
        dependencies = self.waiting_dependencies(node_name)
        return self.storage.nodes_with_job_statuses(
            dependencies,
            {QUEUED, RUNNING, FAILED},
        )

    def node_is_waiting(self, node_name: str) -> bool:
        if not self.storage.has_queued_jobs(node_name):
            return False
        if self.storage.job_status_counts(node_name).get(RUNNING, 0):
            return False
        return bool(self.waiting_blockers(node_name))

    def node_waiting_ready(self, node_name: str) -> bool:
        return not self.waiting_blockers(node_name)

    def component_predecessors(self, component: set[str]) -> set[str]:
        return self.topology.component_predecessors(component)

    def component_predecessor_components(self, component: set[str]) -> set[tuple[str, ...]]:
        return self.topology.component_predecessor_components(component)

    def component_ready(self, component: set[str]) -> bool:
        return all(self.node_complete(node) for node in self.component_predecessors(component))

    def component_has_any_jobs(self, component: set[str]) -> bool:
        return any(self.storage.list_jobs(node_name) for node_name in component)

    def node_complete(self, node_name: str) -> bool:
        return self.storage.get_node_status(node_name) in NODE_COMPLETE_STATUSES

    def mark_component_failed(self, component: set[str]) -> None:
        self.storage.set_node_statuses({node_name: FAILED for node_name in component})

    def refresh_component_status(self, component: set[str], allow_complete: bool = False):
        """Refresh one Hoeflein component as an indivisible lifecycle unit."""
        component = set(component)
        counts_by_node = {
            node_name: self.storage.job_status_counts(node_name)
            for node_name in component
        }
        totals_by_node = {
            node_name: sum(counts.values())
            for node_name, counts in counts_by_node.items()
        }
        total_jobs = sum(totals_by_node.values())

        if any(counts.get(FAILED, 0) for counts in counts_by_node.values()):
            self.mark_component_failed(component)
            return

        has_running = any(
            counts.get(RUNNING, 0)
            for counts in counts_by_node.values()
        )
        if has_running:
            # Actual running jobs make the scheduler-owned component active, but
            # an idle waiting node with queued work keeps its own WAITING state.
            statuses = {}
            for node_name in component:
                counts = counts_by_node[node_name]
                if counts.get(RUNNING, 0):
                    status = RUNNING
                elif counts.get(QUEUED, 0) and self.waiting_blockers(node_name):
                    status = WAITING
                else:
                    status = RUNNING
                statuses[node_name] = status
            self.storage.set_node_statuses(statuses)
            return

        has_queued = any(
            counts.get(QUEUED, 0)
            for counts in counts_by_node.values()
        )
        if has_queued:
            self.storage.set_node_statuses({
                node_name: WAITING if self.node_is_waiting(node_name) else QUEUED
                for node_name in component
            })
            return

        if total_jobs == 0:
            terminal = {
                self.storage.get_node_status(node_name)
                for node_name in component
            }
            if terminal and terminal.issubset({DONE, FAILED, CANCELLED, SKIPPED}):
                return
            self.storage.set_node_statuses({node_name: QUEUED for node_name in component})
            return

        successful_terminal = {DONE, SKIPPED}
        all_terminal_success = all(
            sum(counts_by_node[node_name].get(status, 0) for status in successful_terminal)
            == totals_by_node[node_name]
            for node_name in component
        )

        if all_terminal_success:
            status = DONE if allow_complete and self.component_ready(component) else QUEUED
            self.storage.set_node_statuses({node_name: status for node_name in component})
            return

        self.storage.set_node_statuses({node_name: QUEUED for node_name in component})

    def node_ready(self, node_name: str) -> bool:
        return self.component_ready(self.component_for(node_name))

    def refresh_node_status(self, node_name: str, allow_complete: bool = False):
        component = self.component_for(node_name)
        if len(component) > 1 or self.component_is_cyclic(component):
            self.refresh_component_status(component, allow_complete=allow_complete)
            return

        counts = self.storage.job_status_counts(node_name)
        total = sum(counts.values())
        if total == 0:
            current_status = self.storage.get_node_status(node_name)
            if current_status in {DONE, FAILED, CANCELLED, SKIPPED}:
                return
            self.storage.set_node_status(node_name, QUEUED)
            return
        if counts.get(FAILED, 0):
            self.storage.set_node_status(node_name, FAILED)
            return
        if counts.get(RUNNING, 0):
            self.storage.set_node_status(node_name, RUNNING)
            return
        if counts.get(QUEUED, 0):
            self.storage.set_node_status(
                node_name,
                WAITING if self.node_is_waiting(node_name) else QUEUED,
            )
            return
        successful = counts.get(DONE, 0) + counts.get(SKIPPED, 0)
        if successful == total:
            self.storage.set_node_status(node_name, DONE if allow_complete and self.node_ready(node_name) else QUEUED)
            return
        self.storage.set_node_status(node_name, QUEUED)

    def finalize_ready_nodes(self, skip_components=None):
        # This method runs after every completed concurrent DAG unit. The old
        # implementation reread predecessor state and rewrote already-terminal
        # siblings on every pass, turning a 20-way fan-out into O(width^2) node
        # status traffic. One bulk snapshot preserves the same readiness rule
        # while terminal components become true no-ops.
        statuses = self.storage.get_node_statuses(self.graph_obj.nodes)
        skipped = {tuple(key) for key in (skip_components or ())}
        seen: set[tuple[str, ...]] = set()
        for node_name in self.graph_obj.nodes:
            component = self.component_for(node_name)
            key = self.component_key(component)
            if key in seen:
                continue
            seen.add(key)
            # An in-flight unit owns its own lifecycle publication. Refreshing
            # it from the outer DAG loop only rewrites RUNNING while siblings
            # finish, producing O(width^2) status traffic.
            if key in skipped:
                continue
            if all(statuses.get(name) in NODE_COMPLETE_STATUSES for name in component):
                continue
            predecessors = self.component_predecessors(component)
            if all(statuses.get(name) in NODE_COMPLETE_STATUSES for name in predecessors):
                self.refresh_component_status(component, allow_complete=True)
                # A refresh can make a later component ready during this same
                # pass. Keep the snapshot coherent without rereading every node.
                statuses.update(self.storage.get_node_statuses(component))

    def ready_nodes(self) -> list[str]:
        self.finalize_ready_nodes()
        ready = []
        for node_name in self.graph_obj.nodes:
            if (
                self.storage.has_queued_jobs(node_name)
                and self.node_ready(node_name)
                and self.node_waiting_ready(node_name)
            ):
                ready.append(node_name)
        return ready
