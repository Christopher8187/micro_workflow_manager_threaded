from __future__ import annotations

import ast
from pathlib import Path

from micro_workflow_manager.system import MicroWorkflow

from .files import safe_node_name
from .graph_utils import topo_subset

def autostart_closure(
    workflow: MicroWorkflow,
    graph_file: Path,
    start_nodes: list[str],
) -> list[str]:
    edges = scan_autostarts(graph_file.parent / "node_behavior")
    seen = set(start_nodes)
    found = set()
    queue = list(start_nodes)

    while queue:
        current = queue.pop(0)

        for target in sorted(edges.get(current, set())):
            if not workflow.graph_obj.has_edge(current, target):
                continue

            if target in seen:
                continue

            seen.add(target)
            found.add(target)
            queue.append(target)

    return topo_subset(workflow, found)

def scan_autostarts(directory: Path) -> dict[str, set[str]]:
    result: dict[str, set[str]] = {}

    for path in directory.glob("*.py"):
        if path.name == "__init__.py" or path.name.startswith("_"):
            continue

        tree = ast.parse(path.read_text(encoding="utf-8"))
        from_node = router_name(tree) or path.stem
        node_handles = simple_node_handle_assignments(tree)

        for node in ast.walk(tree):
            target = autostart_target(node, node_handles)
            if target is not None:
                result.setdefault(from_node, set()).add(target)

    return result

def router_name(tree: ast.AST) -> str | None:
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue

        func = node.func
        if isinstance(func, ast.Name) and func.id == "NodeRouter":
            pass
        elif isinstance(func, ast.Attribute) and func.attr == "NodeRouter":
            pass
        else:
            continue

        if node.args and isinstance(node.args[0], ast.Constant) and isinstance(node.args[0].value, str):
            return safe_node_name(node.args[0].value)

    return None

def simple_node_handle_assignments(tree: ast.AST) -> dict[str, str]:
    """Detect simple aliases like: target = ctx.node("next_node").

    This intentionally stays conservative. Dynamic node names are still checked
    at runtime by MicroWorkflow.allowed_run_nodes.
    """
    result: dict[str, str] = {}

    for node in ast.walk(tree):
        target_name = assigned_name(node)
        value = assigned_value(node)

        if target_name is None or value is None:
            continue

        target_node = node_call_target(value)
        if target_node is not None:
            result[target_name] = target_node

    return result

def assigned_name(node: ast.AST) -> str | None:
    if isinstance(node, ast.Assign) and len(node.targets) == 1:
        target = node.targets[0]
        if isinstance(target, ast.Name):
            return target.id

    if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
        return node.target.id

    return None

def assigned_value(node: ast.AST) -> ast.AST | None:
    if isinstance(node, ast.Assign):
        return node.value

    if isinstance(node, ast.AnnAssign):
        return node.value

    return None

def node_call_target(node: ast.AST) -> str | None:
    if not isinstance(node, ast.Call):
        return None

    if not isinstance(node.func, ast.Attribute) or node.func.attr != "node":
        return None

    if not node.args:
        return None

    target = node.args[0]
    if isinstance(target, ast.Constant) and isinstance(target.value, str):
        return safe_node_name(target.value)

    return None

def autostart_target(node: ast.AST, node_handles: dict[str, str]) -> str | None:
    if not isinstance(node, ast.Call):
        return None

    if not isinstance(node.func, ast.Attribute) or node.func.attr != "add":
        return None

    if not any(keyword.arg == "autostart" and is_true(keyword.value) for keyword in node.keywords):
        return None

    source = node.func.value

    direct_target = node_call_target(source)
    if direct_target is not None:
        return direct_target

    if isinstance(source, ast.Name):
        return node_handles.get(source.id)

    return None

def is_true(node: ast.AST) -> bool:
    return isinstance(node, ast.Constant) and node.value is True
