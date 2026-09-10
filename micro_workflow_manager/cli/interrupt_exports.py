"""Determine module router exports from declarations without executing Python."""
from __future__ import annotations

import ast


_UNKNOWN = object()


def exported_router_calls(tree, router_calls, declared_calls, path):
    """Require explicit interrupt declarations to belong to static exports."""
    bindings = {}
    calls = set(router_calls)

    def fail():
        raise RuntimeError(
            f"Interrupt declarations in {path.name} require static router/routers exports; "
            "export every declared router directly or in a literal router list"
        )

    def value(expression):
        if expression in calls:
            return expression
        if isinstance(expression, ast.Name):
            return bindings.get(expression.id, _UNKNOWN)
        if isinstance(expression, (ast.List, ast.Tuple)):
            result = []
            for item in expression.elts:
                if isinstance(item, ast.Starred):
                    part = value(item.value)
                    if not isinstance(part, tuple):
                        return _UNKNOWN
                    result.extend(part)
                else:
                    result.append(value(item))
            return tuple(result)
        if isinstance(expression, ast.Constant):
            return None
        return _UNKNOWN

    def assign(target, resolved):
        if isinstance(target, ast.Name):
            bindings[target.id] = resolved
        elif isinstance(target, (ast.Tuple, ast.List)):
            if isinstance(resolved, tuple) and len(target.elts) == len(resolved):
                for item, part in zip(target.elts, resolved):
                    assign(item, part)
            else:
                for item in target.elts:
                    assign(item, _UNKNOWN)
        elif isinstance(target, ast.Attribute) and target.attr == "interrupt":
            if not known_unrelated(value(target.value)):
                fail()
        elif isinstance(target, ast.Subscript) and isinstance(target.value, ast.Name):
            bindings[target.value.id] = _UNKNOWN

    def invalidate_dynamic_bindings(statement):
        if isinstance(statement, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            # An undecorated definition is a new local object, so later
            # interrupt-attribute mutation is unrelated to a router. A
            # decorator can replace that object with any value, including an
            # already-created router, and therefore cannot be treated as
            # statically unrelated.
            bindings[statement.name] = (
                _UNKNOWN if statement.decorator_list else None
            )
            return
        if isinstance(statement, ast.Name) and isinstance(statement.ctx, (ast.Store, ast.Del)):
            bindings[statement.id] = _UNKNOWN
        for child in ast.iter_child_nodes(statement):
            invalidate_dynamic_bindings(child)

    def known_unrelated(resolved):
        if resolved is None:
            return True
        if isinstance(resolved, tuple):
            return all(known_unrelated(item) for item in resolved)
        return False

    def expression_effects(expression):
        if expression is None:
            return
        for node in ast.walk(expression):
            if isinstance(node, ast.NamedExpr):
                assign(node.target, _UNKNOWN)
            if not isinstance(node, ast.Call):
                continue
            function = node.func
            name = (function.id if isinstance(function, ast.Name)
                    else function.attr if isinstance(function, ast.Attribute) else None)
            if name in {"setattr", "delattr"} and len(node.args) >= 2:
                target = value(node.args[0])
                attribute = node.args[1]
                may_name_interrupt = (
                    not isinstance(attribute, ast.Constant)
                    or type(attribute.value) is not str
                    or attribute.value == "interrupt"
                )
                if may_name_interrupt and not known_unrelated(target):
                    fail()

    for statement in tree.body:
        if isinstance(statement, (ast.Assign, ast.AnnAssign, ast.Expr)):
            expression_effects(statement.value)
        if isinstance(statement, ast.Assign):
            resolved = value(statement.value)
            for target in statement.targets:
                assign(target, resolved)
        elif isinstance(statement, ast.AnnAssign):
            assign(statement.target, value(statement.value) if statement.value is not None else _UNKNOWN)
        elif isinstance(statement, (ast.Import, ast.ImportFrom)):
            if any(alias.name == "*" for alias in statement.names):
                for name in {*bindings, "router", "routers"}:
                    bindings[name] = _UNKNOWN
                continue
            for alias in statement.names:
                bindings[alias.asname or alias.name.split(".")[0]] = _UNKNOWN
        elif isinstance(statement, ast.Expr):
            expression = statement.value
            if (isinstance(expression, ast.Call) and isinstance(expression.func, ast.Attribute)
                    and isinstance(expression.func.value, ast.Name)
                    and expression.func.attr in {"append", "extend", "insert", "remove", "pop", "clear"}):
                name = expression.func.value.id
                if isinstance(bindings.get(name), tuple):
                    bindings[name] = _UNKNOWN
        else:
            invalidate_dynamic_bindings(statement)

    exported = set()
    single = bindings.get("router")
    if single is _UNKNOWN:
        fail()
    if isinstance(single, ast.Call) and single in calls:
        exported.add(single)
    many = bindings.get("routers")
    if many is _UNKNOWN:
        fail()
    if isinstance(many, tuple):
        for item in many:
            if item is _UNKNOWN:
                fail()
            if isinstance(item, ast.Call) and item in calls:
                exported.add(item)
    elif many is not None:
        fail()
    if not set(declared_calls) <= exported:
        fail()
    return tuple(call for call in router_calls if call in exported)
