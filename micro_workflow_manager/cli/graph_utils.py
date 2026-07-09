from __future__ import annotations

import networkx as nx

from micro_workflow_manager.system import MicroWorkflow

def ready_for_run_set(
    workflow: MicroWorkflow,
    node: str,
    run_set: set[str],
    ignore_external: bool,
) -> bool:
    component = workflow.component_for(node)

    for previous in workflow.component_predecessors(component):
        if previous not in run_set and ignore_external:
            continue

        if not workflow.node_complete(previous):
            return False

    return True

def direct_incomplete_inputs(workflow: MicroWorkflow, nodes: set[str]) -> set[str]:
    blockers = set()

    for node in nodes:
        for previous in workflow.graph_obj.predecessors(node):
            if previous not in nodes and not workflow.node_complete(previous):
                blockers.add(previous)

    return blockers

def descendants_in_order(workflow: MicroWorkflow, node: str) -> list[str]:
    descendants = nx.descendants(workflow.graph_obj, node)
    descendants.discard(node)
    return component_topological_nodes(workflow, descendants)

def topo_subset(workflow: MicroWorkflow, nodes: set[str]) -> list[str]:
    return component_topological_nodes(workflow, nodes)

def expand_to_components(workflow: MicroWorkflow, nodes: set[str]) -> set[str]:
    expanded: set[str] = set()

    for node in nodes:
        expanded.update(workflow.component_for(node))

    return expanded

def component_topological_nodes(
    workflow: MicroWorkflow,
    nodes: set[str] | None = None,
) -> list[str]:
    """Return nodes ordered by the DAG of strongly connected components."""
    graph = workflow.graph_obj
    selected = set(graph.nodes) if nodes is None else set(nodes)
    node_order = list(graph.nodes)
    components = [set(component) for component in nx.strongly_connected_components(graph)]
    component_by_node = {
        node: index
        for index, component in enumerate(components)
        for node in component
    }

    component_graph = nx.DiGraph()
    component_graph.add_nodes_from(range(len(components)))

    for start, end in graph.edges:
        start_component = component_by_node[start]
        end_component = component_by_node[end]
        if start_component != end_component:
            component_graph.add_edge(start_component, end_component)

    result: list[str] = []

    for component_index in nx.topological_sort(component_graph):
        for node in node_order:
            if component_by_node[node] == component_index and node in selected:
                result.append(node)

    return result
