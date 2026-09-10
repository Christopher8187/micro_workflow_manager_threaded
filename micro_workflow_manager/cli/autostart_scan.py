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
        node_inputs = simple_node_input_assignments(tree)

        for node in ast.walk(tree):
            target = autostart_target(node, node_handles, node_inputs)
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

def simple_node_input_assignments(tree: ast.AST) -> dict[str, str]:
    """Detect unshadowed handles built from the public MWF constructor."""
    constructors: set[str] = set()
    result: dict[str, tuple[str, str]] = {}

    for statement in tree.body:
        if _has_wildcard_import(statement):
            constructors.clear()
            result.clear()
            continue

        target_name = assigned_name(statement)
        value = assigned_value(statement)

        if target_name is not None and value is not None:
            target_node = node_input_target(value, constructors)
            constructor = node_input_constructor(value, constructors)
            if target_node is not None and constructor is not None:
                result[target_name] = (target_node, constructor)

        constructors.difference_update(_direct_binding_names(statement))
        constructors.update(_node_input_import_names(statement))

    binding_counts = _binding_counts(tree)
    return {
        name: target[0]
        for name, target in result.items()
        if binding_counts.get(name) == 1
        and binding_counts.get(target[1]) == 1
    }

def _node_input_import_names(node: ast.AST) -> set[str]:
    if (not isinstance(node, ast.ImportFrom) or node.level != 0
            or node.module != "micro_workflow_manager"):
        return set()
    return {
        item.asname or item.name
        for item in node.names
        if item.name == "NodeInputFileSystem"
    }

def _has_wildcard_import(node: ast.AST) -> bool:
    return any(
        isinstance(item, ast.ImportFrom)
        and any(alias.name == "*" for alias in item.names)
        for item in ast.walk(node)
    )

def _direct_binding_names(node: ast.AST) -> set[str]:
    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
        return {node.name}
    if isinstance(node, (ast.Import, ast.ImportFrom)):
        return {
            item.asname or item.name.split(".", 1)[0]
            for item in node.names
            if item.name != "*"
        }
    return {
        item.id
        for item in ast.walk(node)
        if isinstance(item, ast.Name) and isinstance(item.ctx, (ast.Store, ast.Del))
    }

def _binding_counts(tree: ast.AST) -> dict[str, int]:
    counts: dict[str, int] = {}

    def add(name: str | None) -> None:
        if name is not None:
            counts[name] = counts.get(name, 0) + 1

    for node in ast.walk(tree):
        if isinstance(node, ast.Name) and isinstance(node.ctx, (ast.Store, ast.Del)):
            add(node.id)
        elif isinstance(node, ast.arg):
            add(node.arg)
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            add(node.name)
        elif isinstance(node, (ast.Import, ast.ImportFrom)):
            for item in node.names:
                if item.name != "*":
                    add(item.asname or item.name.split(".", 1)[0])
        elif isinstance(node, ast.ExceptHandler):
            add(node.name)
        elif isinstance(node, (ast.MatchAs, ast.MatchStar)):
            add(node.name)
        elif isinstance(node, ast.MatchMapping):
            add(node.rest)

    return counts

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

def node_input_target(node: ast.AST, constructors: set[str]) -> str | None:
    if node_input_constructor(node, constructors) is None:
        return None

    target = node.args[0]
    if isinstance(target, ast.Constant) and isinstance(target.value, str):
        return safe_node_name(target.value)

    return None

def node_input_constructor(node: ast.AST, constructors: set[str]) -> str | None:
    if not isinstance(node, ast.Call):
        return None

    if not isinstance(node.func, ast.Name) or node.func.id not in constructors:
        return None

    if not node.args:
        return None

    return node.func.id

def autostart_target(
    node: ast.AST,
    node_handles: dict[str, str],
    node_inputs: dict[str, str] | None = None,
) -> str | None:
    if not isinstance(node, ast.Call):
        return None

    if not isinstance(node.func, ast.Attribute) or node.func.attr not in (
        "add", "add_job", "add_jobs",
    ):
        return None

    if not any(keyword.arg == "autostart" and is_true(keyword.value) for keyword in node.keywords):
        return None

    source = node.func.value

    if node.func.attr in ("add_job", "add_jobs"):
        if isinstance(source, ast.Name):
            return (node_inputs or {}).get(source.id)
        return None

    direct_target = node_call_target(source)
    if direct_target is not None:
        return direct_target

    if isinstance(source, ast.Name):
        return node_handles.get(source.id)

    return None

def is_true(node: ast.AST) -> bool:
    return isinstance(node, ast.Constant) and node.value is True
