"""Conservatively find literal node handles without importing task modules."""

from __future__ import annotations

import ast
from pathlib import Path

from .autostart_scan import node_call_target, router_name


def scan_static_node_targets(directory: Path) -> dict[str, frozenset[str]]:
    result = {}
    for path in directory.glob("*.py"):
        if path.name == "__init__.py" or path.name.startswith("_"):
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"))
        source = router_name(tree) or path.stem
        targets = {
            target
            for node in ast.walk(tree)
            if (target := _context_node_target(node)) is not None
        }
        if targets:
            result[source] = frozenset(targets)
    return result


def _context_node_target(node: ast.AST) -> str | None:
    """Return only direct literal ``ctx.node(...)`` destinations."""
    if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
        return None
    if not isinstance(node.func.value, ast.Name) or node.func.value.id != "ctx":
        return None
    return node_call_target(node)
