from __future__ import annotations

from micro_workflow_manager.system import MicroWorkflow

def ask(question: str) -> bool:
    try:
        answer = input(f"{question} [y/N] ").strip().lower()
    except EOFError:
        print("n")
        return False

    return answer in {"y", "yes"}

def print_not_ready(workflow: MicroWorkflow, node: str):
    print(f"{node} is not ready yet.")
    print("Previous nodes not finished:")

    for previous in workflow.graph_obj.predecessors(node):
        status = workflow.storage.get_node_status(previous) or "missing"
        if not workflow.node_complete(previous):
            print(f"  {previous}: {status}")

def is_ready(workflow: MicroWorkflow, node: str) -> bool:
    return workflow.node_ready(node)

def require_node(workflow: MicroWorkflow, node: str):
    if node not in workflow.graph_obj.nodes:
        raise RuntimeError(f"Unknown node: {node}")
