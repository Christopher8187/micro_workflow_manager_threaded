from __future__ import annotations

import networkx as nx

from .errors import InvalidGraphError


class ComponentTopology:
    """Calculate components and quotient selections without runtime state.

    The caller supplies the current raw graph and autostart relationships.
    Methods only read those inputs and return new graphs or collections.
    """

    def __init__(self, graph: nx.DiGraph, autostart_edges):
        self.graph_obj = graph
        self.autostart_edges = autostart_edges

    def hoeflein_graph(self) -> nx.DiGraph:
        graph = self.graph_obj.copy()
        for start, end in self.autostart_edges:
            if graph.has_edge(start, end):
                graph.add_edge(end, start)
        return graph

    def hoeflein_components(self) -> list[set[str]]:
        return [set(component) for component in nx.strongly_connected_components(self.hoeflein_graph())]

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

    def component_interval(self, start_node: str, end_node: str) -> list[tuple[str, ...]]:
        """Return the half-open directed quotient interval [start, end)."""
        for name in (start_node, end_node):
            if name not in self.graph_obj:
                raise InvalidGraphError(f"Unknown interval endpoint node {name!r}")
        dag = self.component_dag()
        start = self.component_id(start_node)
        end = self.component_id(end_node)
        descendants = nx.descendants(dag, start)
        if end not in descendants:
            raise InvalidGraphError(
                f"End component {end!r} must be a strict directed descendant of {start!r}"
            )
        selected = ({start} | descendants) & ({end} | nx.ancestors(dag, end))
        selected.discard(end)
        return list(nx.lexicographical_topological_sort(dag.subgraph(selected)))

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

    def execution_components(self, nodes: list[str] | None = None) -> list[tuple[str, ...]]:
        """Return Hoeflein execution units in quotient-DAG topological order."""
        selected = set(self.graph_obj.nodes if nodes is None else nodes)
        if not selected:
            return []
        dag = self.component_dag()
        return [
            component
            for component in nx.topological_sort(dag)
            if set(component).intersection(selected)
        ]
