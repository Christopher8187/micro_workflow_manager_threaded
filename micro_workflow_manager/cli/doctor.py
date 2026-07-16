from __future__ import annotations

import ast
import json
from datetime import datetime
from pathlib import Path

from micro_workflow_manager.models import RUNNING
from micro_workflow_manager.paths import run_file, threads_file
from micro_workflow_manager.storage import FileStorage

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
        storage = FileStorage(root)
    except Exception as error:
        print("MWF doctor found a project error:")
        print(f"  ERROR: {error}")
        return 1

    integrity = storage.database_integrity_check()
    if integrity == "ok":
        checks.append("SQLite workflow state passed PRAGMA quick_check")
    else:
        errors.append(f"SQLite workflow state failed integrity check: {integrity}")

    schema_plan = migration_plan(root)
    if schema_plan["malformed"]:
        errors.append(
            "malformed framework metadata: "
            + ", ".join(path.relative_to(root).as_posix() for path in schema_plan["malformed"])
        )
    if schema_plan["newer"]:
        errors.append(
            "framework metadata uses a newer state schema: "
            + ", ".join(path.relative_to(root).as_posix() for path in schema_plan["newer"])
        )
    if schema_plan["outdated"]:
        warnings.append(
            f"{len(schema_plan['outdated'])} framework JSON file(s) need schema migration; "
            "run mwf migrate --dry-run"
        )

    stored = config.get("graph_path")
    if isinstance(stored, str) and "\\" in stored:
        warnings.append(
            "stored graph_path uses Windows separators; it is accepted and will be "
            "rewritten with '/' by mwf graph --update"
        )

    try:
        edges = read_edges(import_file(graph_file))
    except Exception as error:
        errors.append(f"graph.py could not be loaded: {error}")
        edges = []

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
        database_ids = set(storage.list_job_ids(node_name))
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

    abandoned_running: list[str] = []
    overdue_checkpoints: list[str] = []
    now_aware = datetime.now().astimezone()
    for node_name in sorted(graph_nodes | disk_nodes):
        for row in storage.list_jobs(node_name, status=RUNNING):
            job_id = int(row["job_id"])
            control = storage.read_job_control(node_name, job_id)
            pid = control.get("active_pid") or row.get("pid")
            if not process_is_alive(pid):
                abandoned_running.append(f"{node_name}/{job_id}")

            runtime = storage.read_job_runtime(node_name, job_id)
            if runtime.get("state") != "running":
                continue
            deadline_text = runtime.get("checkpoint_deadline_at")
            if not isinstance(deadline_text, str):
                continue
            try:
                deadline = datetime.fromisoformat(deadline_text)
            except ValueError:
                continue
            comparison_now = now_aware if deadline.tzinfo is not None else datetime.now()
            if deadline < comparison_now:
                overdue_checkpoints.append(f"{node_name}/{job_id}")

    if abandoned_running:
        warnings.append(
            "running jobs have no live owner: " + ", ".join(abandoned_running) + "; run mwf recover"
        )
    if overdue_checkpoints:
        warnings.append(
            "checkpoint deadlines are overdue: "
            + ", ".join(overdue_checkpoints)
            + "; inspect the job and verify the active scheduler heartbeat"
        )

    thread_override_path = threads_file(root)
    if thread_override_path.exists():
        problem = _json_problem(thread_override_path)
        if problem:
            errors.append(f"malformed JSON: {problem}")
        else:
            try:
                overrides = storage.read_thread_overrides()
            except Exception as error:
                errors.append(f"invalid runtime thread overrides: {error}")
            else:
                unknown = sorted(set(overrides) - graph_nodes)
                if unknown:
                    warnings.append(
                        "runtime max_threads overrides reference nodes outside the graph: "
                        + ", ".join(unknown)
                    )
                checks.append(f"checked {len(overrides)} runtime max_threads override(s)")

    state_path = run_file(root)
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

    lock_count = storage.db_connection().execute(
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
