from __future__ import annotations

import networkx as nx

from ..models import (
    CANCELLED,
    DONE,
    FAILED,
    NODE_COMPLETE_STATUSES,
    QUEUED,
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
        graph = self.graph_obj.copy()
        for start, end in self.autostart_edges:
            if graph.has_edge(start, end):
                graph.add_edge(end, start)
        return graph

    def hoeflein_components(self) -> list[set[str]]:
        return [set(component) for component in nx.strongly_connected_components(self.hoeflein_graph())]

    # Compatibility name. Scheduling now consistently means Hoeflein components.
    def strongly_connected_components(self) -> list[set[str]]:
        return self.hoeflein_components()

    def component_key(self, component: set[str] | tuple[str, ...] | list[str]) -> tuple[str, ...]:
        # Component identity must remain stable when graph edge declaration order
        # changes. Lexicographic node order gives provenance a portable key.
        return tuple(sorted(set(component)))

    def component_for(self, node_name: str) -> set[str]:
        for component in self.hoeflein_components():
            if node_name in component:
                return component
        return {node_name}

    def component_id(self, node_or_component: str | set[str] | tuple[str, ...] | list[str]) -> tuple[str, ...]:
        component = self.component_for(node_or_component) if isinstance(node_or_component, str) else set(node_or_component)
        return self.component_key(component)

    def component_map(self) -> tuple[list[set[str]], dict[str, int]]:
        components = self.hoeflein_components()
        by_node = {
            node_name: index
            for index, component in enumerate(components)
            for node_name in component
        }
        return components, by_node

    def component_dag(self) -> nx.DiGraph:
        components, by_node = self.component_map()
        dag = nx.DiGraph()
        for component in components:
            dag.add_node(self.component_key(component))
        for start, end in self.graph_obj.edges:
            start_key = self.component_key(components[by_node[start]])
            end_key = self.component_key(components[by_node[end]])
            if start_key != end_key:
                dag.add_edge(start_key, end_key)
        return dag

    def component_descendants(self, component: set[str] | tuple[str, ...] | list[str]) -> list[tuple[str, ...]]:
        key = self.component_key(component)
        dag = self.component_dag()
        descendants = nx.descendants(dag, key)
        return [item for item in nx.topological_sort(dag) if item in descendants]

    def component_is_cyclic(self, component: set[str]) -> bool:
        return len(component) > 1 or any(
            self.graph_obj.has_edge(node_name, node_name)
            for node_name in component
        )

    def component_predecessors(self, component: set[str]) -> set[str]:
        predecessors: set[str] = set()
        for node_name in component:
            predecessors.update(self.graph_obj.predecessors(node_name))
        return predecessors - component

    def component_predecessor_components(self, component: set[str]) -> set[tuple[str, ...]]:
        return {self.component_id(node) for node in self.component_predecessors(component)}

    def component_ready(self, component: set[str]) -> bool:
        return all(self.node_complete(node) for node in self.component_predecessors(component))

    def component_has_any_jobs(self, component: set[str]) -> bool:
        return any(self.storage.list_jobs(node_name) for node_name in component)

    def node_complete(self, node_name: str) -> bool:
        return self.storage.get_node_status(node_name) in NODE_COMPLETE_STATUSES

    def mark_component_failed(self, component: set[str]) -> None:
        for node_name in component:
            self.storage.set_node_status(node_name, FAILED)

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
            # Actual running jobs make the whole scheduler-owned component
            # active. Queued work alone must not do this: before ``mwf run`` a
            # ready component is queued, not already running.
            for node_name in component:
                self.storage.set_node_status(node_name, RUNNING)
            return

        has_queued = any(
            counts.get(QUEUED, 0)
            for counts in counts_by_node.values()
        )
        if has_queued:
            for node_name in component:
                self.storage.set_node_status(node_name, QUEUED)
            return

        if total_jobs == 0:
            terminal = {
                self.storage.get_node_status(node_name)
                for node_name in component
            }
            if terminal and terminal.issubset({DONE, FAILED, CANCELLED, SKIPPED}):
                return
            for node_name in component:
                self.storage.set_node_status(node_name, QUEUED)
            return

        successful_terminal = {DONE, SKIPPED}
        all_terminal_success = all(
            sum(counts_by_node[node_name].get(status, 0) for status in successful_terminal)
            == totals_by_node[node_name]
            for node_name in component
        )

        if all_terminal_success:
            status = DONE if allow_complete and self.component_ready(component) else QUEUED
            for node_name in component:
                self.storage.set_node_status(node_name, status)
            return

        for node_name in component:
            self.storage.set_node_status(node_name, QUEUED)

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
            self.storage.set_node_status(node_name, QUEUED)
            return
        successful = counts.get(DONE, 0) + counts.get(SKIPPED, 0)
        if successful == total:
            self.storage.set_node_status(node_name, DONE if allow_complete and self.node_ready(node_name) else QUEUED)
            return
        self.storage.set_node_status(node_name, QUEUED)

    def finalize_ready_nodes(self):
        seen: set[tuple[str, ...]] = set()
        for node_name in self.graph_obj.nodes:
            component = self.component_for(node_name)
            key = self.component_key(component)
            if key in seen:
                continue
            seen.add(key)
            if self.component_ready(component):
                self.refresh_component_status(component, allow_complete=True)

    def ready_nodes(self) -> list[str]:
        self.finalize_ready_nodes()
        ready = []
        for node_name in self.graph_obj.nodes:
            if self.storage.has_queued_jobs(node_name) and self.node_ready(node_name):
                ready.append(node_name)
        return ready
