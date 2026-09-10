from __future__ import annotations

import ast
import json
from pathlib import Path


from .files import read_config
from .engine import _stored_edges
from .preview import PreviewStorage
from .doctor_native import inspect_native_state
from .project import resolve_configured_graph_path


def _json_problem(path: Path) -> str | None:
    try:
        json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        return f"{path}: {error}"
    return None


def _static_ctx_edges(path: Path) -> set[str]:
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    except (OSError, SyntaxError):
        return set()
    targets: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
            continue
        if node.func.attr != "node" or not node.args:
            continue
        first = node.args[0]
        if isinstance(first, ast.Constant) and isinstance(first.value, str):
            targets.add(first.value)
    return targets


def doctor_command(root: Path) -> int:
    errors: list[str] = []
    warnings: list[str] = []
    checks: list[str] = []

    try:
        config = read_config(root)
        graph_file = resolve_configured_graph_path(root, config)
        checks.append(f"graph file exists: {graph_file.relative_to(root).as_posix()}")
        edges = _stored_edges(config)
        storage = PreviewStorage(root)
    except Exception as error:
        print("MWF doctor found a project error:")
        print(f"  ERROR: {error}")
        return 1

    try:
        return _doctor_report(root, config, graph_file, edges, storage, checks, warnings, errors)
    finally:
        storage.close()


def _doctor_report(root, config, graph_file, edges, storage, checks, warnings, errors):
    connection = storage.connection
    integrity = connection.execute('PRAGMA quick_check').fetchone()[0]
    if integrity == "ok":
        checks.append("SQLite workflow state passed PRAGMA quick_check")
    else:
        errors.append(f"SQLite workflow state failed integrity check: {integrity}")

    stored = config.get("graph_path")
    if isinstance(stored, str) and "\\" in stored:
        warnings.append(
            "stored graph_path uses Windows separators; it is accepted and will be "
            "rewritten with '/' by mwf graph --update"
        )

    graph_nodes = {item for edge in edges for item in edge}
    disk_root = root / "node"
    disk_nodes = {
        path.name for path in disk_root.iterdir() if path.is_dir()
    } if disk_root.is_dir() else set()
    missing_folders = sorted(graph_nodes - disk_nodes)
    stale_folders = sorted(disk_nodes - graph_nodes)
    if missing_folders:
        errors.append("graph nodes missing on disk: " + ", ".join(missing_folders))
    if stale_folders:
        errors.append("stale node folders: " + ", ".join(stale_folders))
    if not missing_folders and not stale_folders:
        checks.append("node folders match the graph")

    behavior_dir = graph_file.parent / "node_behavior"
    router_files = {
        path.stem: path
        for path in behavior_dir.glob("*.py")
        if path.name != "__init__.py" and not path.name.startswith("_")
    } if behavior_dir.is_dir() else {}
    missing_routers = sorted(graph_nodes - set(router_files))
    extra_routers = sorted(set(router_files) - graph_nodes)
    if missing_routers:
        errors.append("graph nodes without node_behavior files: " + ", ".join(missing_routers))
    if extra_routers:
        warnings.append("node_behavior files outside the graph are ignored: " + ", ".join(extra_routers))

    declared = set(edges)
    for source, path in router_files.items():
        for target in sorted(_static_ctx_edges(path)):
            if source in graph_nodes and (source, target) not in declared:
                warnings.append(
                    f"{path.name} contains ctx.node({target!r}) but {source} -> {target} "
                    "is not a declared edge"
                )

    # Only payload/config JSON remains on disk. High-churn framework state is
    # validated by SQLite's integrity check above.
    json_files: list[Path] = []
    if disk_root.is_dir():
        for pattern in (
            "*/schema.json",
            "*/input/**/*.json",
            "*/output/**/*.json",
            "*/jobs/*/input.json",
            "*/jobs/*/output.json",
        ):
            json_files.extend(disk_root.glob(pattern))
    malformed = [problem for path in json_files if (problem := _json_problem(path))]
    errors.extend(f"malformed JSON: {item}" for item in malformed)
    if not malformed:
        checks.append(f"checked {len(json_files)} on-disk JSON payload/config file(s)")

    # Job identity is authoritative in SQLite, while each job input remains a
    # file. Report interrupted half-commits in either direction explicitly.
    for node_name in sorted(graph_nodes | disk_nodes):
        database_ids = {row[0] for row in connection.execute(
            'SELECT job_id FROM jobs WHERE node_name=?', (node_name,),
        )}
        jobs_root = root / "node" / node_name / "jobs"
        disk_ids = {
            int(path.name)
            for path in jobs_root.iterdir()
            if path.is_dir() and path.name.isdigit()
        } if jobs_root.is_dir() else set()
        orphan_payloads = sorted(disk_ids - database_ids)
        missing_payloads = sorted(database_ids - disk_ids)
        missing_inputs = sorted(
            job_id
            for job_id in database_ids & disk_ids
            if not (jobs_root / str(job_id) / "input.json").is_file()
        )
        if orphan_payloads:
            errors.append(
                f"job payload folders without SQLite rows in {node_name}: "
                + ", ".join(map(str, orphan_payloads))
            )
        if missing_payloads:
            errors.append(
                f"SQLite jobs without payload folders in {node_name}: "
                + ", ".join(map(str, missing_payloads))
            )
        if missing_inputs:
            errors.append(
                f"SQLite jobs missing input.json in {node_name}: "
                + ", ".join(map(str, missing_inputs))
            )

    inspect_native_state(connection, graph_nodes, checks, warnings, errors, root=root)

    temp_files = list(root.rglob(".*.tmp"))
    if temp_files:
        warnings.append(f"found {len(temp_files)} temporary files left by interrupted atomic writes")

    lock_count = connection.execute(
        "SELECT COUNT(*) FROM advisory_locks"
    ).fetchone()[0]
    checks.append(f"SQLite advisory lock table is readable ({int(lock_count)} active lease(s))")

    print("MWF doctor")
    for item in checks:
        print(f"  OK: {item}")
    for item in warnings:
        print(f"  WARNING: {item}")
    for item in errors:
        print(f"  ERROR: {item}")
    if not warnings and not errors:
        print("  Healthy: no problems found.")
    return 1 if errors else 0
