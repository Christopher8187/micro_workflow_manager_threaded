from __future__ import annotations

import ast
import json
from pathlib import Path

from .active_run import process_is_alive, run_state_liveness
from .files import read_config
from .migration import migration_plan
from .project import import_file, read_edges, resolve_configured_graph_path


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
    except Exception as error:
        print("MWF doctor found a project error:")
        print(f"  ERROR: {error}")
        return 1

    schema_plan = migration_plan(root)
    if schema_plan["newer"]:
        errors.append(
            "framework metadata uses a newer state schema: "
            + ", ".join(path.relative_to(root).as_posix() for path in schema_plan["newer"])
        )
    if schema_plan["outdated"]:
        warnings.append(
            f"{len(schema_plan['outdated'])} framework metadata file(s) need schema migration; "
            "run mwf migrate --dry-run"
        )

    stored = config.get("graph_path")
    if isinstance(stored, str) and "\\" in stored:
        warnings.append("stored graph_path uses Windows separators; it is accepted and will be rewritten with '/' by mwf graph --update")

    try:
        edges = read_edges(import_file(graph_file))
    except Exception as error:
        errors.append(f"graph.py could not be loaded: {error}")
        edges = []

    graph_nodes = {item for edge in edges for item in edge}
    disk_root = root / "node"
    disk_nodes = {path.name for path in disk_root.iterdir() if path.is_dir()} if disk_root.is_dir() else set()
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
                warnings.append(f"{path.name} contains ctx.node({target!r}) but {source} -> {target} is not a declared edge")

    json_files: list[Path] = []
    if disk_root.is_dir():
        for pattern in ("*/node_state.json", "*/schema.json", "*/job_index.json", "*/jobs/*/job.json", "*/jobs/*/input.json", "*/jobs/*/status.json", "*/jobs/*/execution.json"):
            json_files.extend(disk_root.glob(pattern))
    malformed = [problem for path in json_files if (problem := _json_problem(path))]
    errors.extend(f"malformed JSON: {item}" for item in malformed)
    if not malformed:
        checks.append(f"checked {len(json_files)} state JSON files")

    abandoned_running: list[str] = []
    for status_path in disk_root.glob("*/jobs/*/status.json") if disk_root.is_dir() else []:
        try:
            status_data = json.loads(status_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            # The malformed file was already reported above. Doctor must remain
            # read-only and continue checking the rest of the project.
            continue
        if status_data.get("status") != "running":
            continue
        node = status_path.parents[2].name
        job_id = int(status_path.parent.name) if status_path.parent.name.isdigit() else None
        execution_path = status_path.parent / "execution.json"
        try:
            control = json.loads(execution_path.read_text(encoding="utf-8")) if execution_path.exists() else {}
        except (OSError, json.JSONDecodeError):
            continue
        pid = control.get("active_pid") or status_data.get("pid")
        if not process_is_alive(pid):
            abandoned_running.append(f"{node}/{job_id}")
    if abandoned_running:
        warnings.append(
            "running jobs have no live owner: " + ", ".join(abandoned_running) + "; run mwf recover"
        )

    state_path = root / ".mwf_run.json"
    if state_path.exists():
        problem = _json_problem(state_path)
        if problem:
            errors.append(f"malformed JSON: {problem}")
        else:
            state = json.loads(state_path.read_text(encoding="utf-8"))
            liveness = run_state_liveness(state)
            if state.get("status") == "running" and not liveness["live"]:
                warnings.append(f"stale running sequence: {liveness['reason']}; run mwf recover")
            elif liveness["live"]:
                checks.append("active run ownership is live")

    temp_files = list(root.rglob(".*.tmp"))
    if temp_files:
        warnings.append(f"found {len(temp_files)} temporary files left by interrupted atomic writes")

    lock_dir = root / ".mwf_locks"
    if lock_dir.is_dir():
        checks.append(
            f"found {len(list(lock_dir.glob('*.lock')))} reusable lock files; lock files persist by design and are not treated as abandoned state"
        )

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
