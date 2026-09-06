from __future__ import annotations

import networkx as nx

from micro_workflow_manager.system import MicroWorkflow


def direct_incomplete_inputs(workflow: MicroWorkflow, nodes: set[str]) -> set[str]:
    blockers = set()
    for component in workflow.execution_components(list(nodes)):
        for previous in workflow.component_predecessors(set(component)):
            if previous not in nodes and not workflow.node_complete(previous):
                blockers.add(previous)
    return blockers


def descendants_in_order(workflow: MicroWorkflow, node: str) -> list[str]:
    start = workflow.component_for(node)
    result: list[str] = []
    for component in workflow.component_descendants(start):
        result.extend(component)
    return result


def topo_subset(workflow: MicroWorkflow, nodes: set[str]) -> list[str]:
    return component_topological_nodes(workflow, nodes)


def expand_to_components(workflow: MicroWorkflow, nodes: set[str]) -> set[str]:
    expanded: set[str] = set()
    for node in nodes:
        expanded.update(workflow.component_for(node))
    return expanded


def component_topological_nodes(workflow: MicroWorkflow, nodes: set[str] | None = None) -> list[str]:
    selected = set(workflow.graph_obj.nodes) if nodes is None else set(nodes)
    result: list[str] = []
    for component in workflow.execution_components(list(selected)):
        result.extend(node for node in component if node in selected)
    return result
