"""Resolve MWF router constructors from exact imports in module statement order."""
from __future__ import annotations

import ast


_PACKAGE = "micro_workflow_manager"
_ROUTER_MODULE = _PACKAGE + ".router"
_CLASS = _ROUTER_MODULE + ".NodeRouter"


def router_call_kinds(tree):
    bindings = {}
    calls = {}

    def resolve(expression):
        if isinstance(expression, ast.Name):
            return bindings.get(expression.id)
        if isinstance(expression, ast.Attribute):
            base = resolve(expression.value)
            if base == _PACKAGE and expression.attr == "router":
                return _ROUTER_MODULE
            if base in {_PACKAGE, _ROUTER_MODULE} and expression.attr == "NodeRouter":
                return _CLASS
            if base == _CLASS and expression.attr == "from_file":
                return _CLASS + ".from_file"
        return None

    def clear_target(target):
        if isinstance(target, ast.Name):
            bindings.pop(target.id, None)
        elif isinstance(target, (ast.Tuple, ast.List)):
            for item in target.elts:
                clear_target(item)
        elif isinstance(target, ast.Attribute):
            base = resolve(target.value)
            if base is not None:
                for name in tuple(bindings):
                    if bindings[name] == base:
                        bindings.pop(name)

    def clear_dynamic(statement):
        if isinstance(statement, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            bindings.pop(statement.name, None)
            return
        if isinstance(statement, ast.Name) and isinstance(statement.ctx, (ast.Store, ast.Del)):
            bindings.pop(statement.id, None)
        for child in ast.iter_child_nodes(statement):
            clear_dynamic(child)

    for statement in tree.body:
        if isinstance(statement, ast.Import):
            for alias in statement.names:
                name = alias.asname or alias.name.split(".")[0]
                bindings.pop(name, None)
                if alias.name in {_PACKAGE, _ROUTER_MODULE}:
                    bindings[name] = alias.name if alias.asname else _PACKAGE
            continue
        if isinstance(statement, ast.ImportFrom):
            if any(alias.name == "*" for alias in statement.names):
                bindings.clear()
                continue
            for alias in statement.names:
                name = alias.asname or alias.name
                bindings.pop(name, None)
                if statement.level == 0:
                    if statement.module in {_PACKAGE, _ROUTER_MODULE} and alias.name == "NodeRouter":
                        bindings[name] = _CLASS
                    elif statement.module == _PACKAGE and alias.name == "router":
                        bindings[name] = _ROUTER_MODULE
            continue
        if isinstance(statement, (ast.Assign, ast.AnnAssign)) and statement.value is not None:
            for expression in ast.walk(statement.value):
                if isinstance(expression, ast.NamedExpr):
                    clear_target(expression.target)
        for node in ast.walk(statement):
            if isinstance(node, ast.Call):
                target = resolve(node.func)
                if target == _CLASS:
                    calls[node] = "constructor"
                elif target == _CLASS + ".from_file":
                    calls[node] = "from-file"
        if isinstance(statement, ast.Assign):
            resolved = resolve(statement.value)
            for target in statement.targets:
                clear_target(target)
                if isinstance(target, ast.Name) and resolved is not None:
                    bindings[target.id] = resolved
        elif isinstance(statement, ast.AnnAssign):
            resolved = resolve(statement.value)
            clear_target(statement.target)
            if isinstance(statement.target, ast.Name) and resolved is not None:
                bindings[statement.target.id] = resolved
        else:
            clear_dynamic(statement)
    return calls
