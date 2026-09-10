"""Read literal interrupt declarations without importing task modules."""

from __future__ import annotations

import ast
import hashlib
from dataclasses import dataclass
from pathlib import Path

from .files import safe_node_name
from .interrupt_exports import exported_router_calls
from .interrupt_constructors import router_call_kinds


@dataclass(frozen=True, slots=True)
class InterruptDeclarations:
    values: tuple[tuple[str, bool], ...]
    source_files: tuple[tuple[str, str], ...] = ()

    @property
    def interrupt_nodes(self) -> frozenset[str]:
        return frozenset(node for node, value in self.values if value)

    def value_for(self, node: str) -> bool:
        return dict(self.values).get(node, False)


def _literal_router_name(call: ast.Call, path: Path, kind: str) -> str:
    if kind == "from-file":
        file_values = [keyword.value for keyword in call.keywords if keyword.arg == "file"]
        if call.args and file_values:
            raise RuntimeError(
                f"Interrupt declaration in {path.name} has an ambiguous router file"
            )
        if call.args:
            expression = call.args[0]
        elif len(file_values) == 1:
            expression = file_values[0]
        else:
            raise RuntimeError(
                f"Interrupt declaration in {path.name} requires a router file"
            )
        if isinstance(expression, ast.Name) and expression.id == "__file__":
            return safe_node_name(path.stem)
        if isinstance(expression, ast.Constant) and isinstance(expression.value, str):
            return safe_node_name(Path(expression.value).stem)
        raise RuntimeError(
            f"Interrupt declaration in {path.name} requires __file__ or a literal router file"
        )
    name_values = [keyword.value for keyword in call.keywords if keyword.arg == "name"]
    if call.args and name_values:
        raise RuntimeError(
            f"Interrupt declaration in {path.name} has an ambiguous router node name"
        )
    if call.args:
        expression = call.args[0]
    elif len(name_values) == 1:
        expression = name_values[0]
    else:
        expression = None
    if (
        isinstance(expression, ast.Constant)
        and isinstance(expression.value, str)
    ):
        return safe_node_name(expression.value)
    raise RuntimeError(
        f"Interrupt declaration in {path.name} requires a literal router node name"
    )


def _interrupt_keyword(call: ast.Call, path: Path):
    expanded = any(keyword.arg is None for keyword in call.keywords)
    declared = [keyword.value for keyword in call.keywords if keyword.arg == "interrupt"]
    if expanded:
        raise RuntimeError(
            f"Interrupt declaration in {path.name} cannot use expanded router options"
        )
    if not declared:
        return None
    if len(declared) != 1:
        raise RuntimeError(f"Interrupt declaration in {path.name} is ambiguous")
    expression = declared[0]
    if not isinstance(expression, ast.Constant) or type(expression.value) is not bool:
        raise RuntimeError(
            f"Interrupt declaration in {path.name} must use a literal Boolean"
        )
    return expression.value


def scan_interrupt_declarations(directory: Path) -> InterruptDeclarations:
    declarations: dict[str, bool] = {}
    source_files: list[tuple[str, str]] = []
    for path in sorted(directory.glob("*.py")):
        if path.name == "__init__.py" or path.name.startswith("_"):
            continue
        content = path.read_bytes()
        source_files.append((path.name, hashlib.sha256(content).hexdigest()))
        tree = ast.parse(content.decode("utf-8"), filename=str(path))
        calls = router_call_kinds(tree)
        declared = {call: _interrupt_keyword(call, path) for call in calls}
        exported = exported_router_calls(
            tree, calls, [call for call, value in declared.items() if value is not None], path,
        )
        for call in exported:
            if declared[call] is None:
                continue
            name = _literal_router_name(call, path, calls[call])
            declarations[name] = declarations.get(name, False) or declared[call]
    return InterruptDeclarations(
        tuple(sorted(declarations.items())), tuple(source_files),
    )
