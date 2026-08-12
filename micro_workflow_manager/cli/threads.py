from __future__ import annotations

import argparse
import sys
from pathlib import Path

from micro_workflow_manager.runners.threaded import (
    HIGH_RUNTIME_THREAD_WARNING,
    MAX_RUNTIME_THREADS,
)
from micro_workflow_manager.storage import FileStorage

from .active_run import live_active_run
from .layout import ensure_runtime_layout
from .files import find_root, read_config, safe_node_name
from .project import load_workflow


RESET_WORDS = {"reset", "default", "clear"}


def _proportional_api_limits(requested: dict[str, int], total: int) -> dict[str, int]:
    if not requested or total >= sum(requested.values()):
        return requested
    if total < len(requested):
        return {name: 1 for name in requested}
    requested_total = sum(requested.values())
    shares = {
        name: max(1, (total * value) // requested_total)
        for name, value in requested.items()
    }
    used = sum(shares.values())
    order = sorted(
        requested,
        key=lambda name: (-((total * requested[name]) % requested_total), name),
    )
    while used < total:
        changed = False
        for name in order:
            if shares[name] >= requested[name]:
                continue
            shares[name] += 1
            used += 1
            changed = True
            if used == total:
                break
        if not changed:
            break
    while used > total:
        for name in reversed(order):
            if shares[name] > 1:
                shares[name] -= 1
                used -= 1
                if used == total:
                    break
    return shares


def _node_schema(storage: FileStorage, node: str) -> dict:
    # Do not call node_schema_file() here: storage path helpers intentionally
    # materialize directories for normal workflow writes. A read-only control
    # command must never recreate an unknown/stale node folder.
    path = storage.project_dir / "node" / node / "schema.json"
    data = storage.read_json(path, default=None)
    if not isinstance(data, dict):
        raise RuntimeError(
            f"No mounted schema for node {node}. Run 'mwf graph --update' first."
        )
    return data


def _declared_limit(storage: FileStorage, node: str) -> int:
    schema = _node_schema(storage, node)
    value = schema.get("max_threads")
    if type(value) is not int or value < 1:
        raise RuntimeError(f"Node {node} has an invalid declared max_threads value")
    return value


def _effective_runner(root: Path, schema: dict) -> str:
    override = schema.get("runner_override")
    if isinstance(override, str) and override:
        return override
    config = read_config(root)
    value = config.get("runner", "threaded")
    return value if isinstance(value, str) else "threaded"


def _parse_new_limit(spec: str, current: int, *, maximum: int | None) -> int:
    text = spec.strip()
    if not text:
        raise ValueError("thread limit cannot be empty")

    if text[0] in {"+", "-"} and len(text) > 1:
        try:
            delta = int(text)
        except ValueError as error:
            raise ValueError("use an integer, +N, -N, or reset") from error
        value = current + delta
    else:
        try:
            value = int(text)
        except ValueError as error:
            raise ValueError("use an integer, +N, -N, or reset") from error

    if value < 1:
        raise ValueError("effective max_threads must remain at least 1")
    if maximum is not None and value > maximum:
        raise ValueError(f"effective max_threads cannot exceed {maximum}")
    return value


def thread_status(root: Path, storage: FileStorage, node: str) -> dict:
    declared = _declared_limit(storage, node)
    override_state = storage.read_thread_override_state()
    overrides = storage.read_thread_overrides()
    override = overrides.get(node)
    schema = _node_schema(storage, node)
    runner = _effective_runner(root, schema)
    effective = 1 if runner == "direct" else (override or declared)
    active = live_active_run(storage)
    is_active_node = active is not None and node in set(active.get("nodes") or [])
    return {
        "node": node,
        "declared": declared,
        "override": override,
        "effective": effective,
        "runner": runner,
        "active": is_active_node,
        "run": active,
        "override_run_id": override_state.get("run_id"),
    }


def print_thread_status(status: dict) -> None:
    print(f"Node {status['node']}")
    print(f"  runner: {status['runner']}")
    print(f"  declared max_threads: {status['declared']}")
    print(
        "  runtime override: "
        + (str(status["override"]) if status["override"] is not None else "(none)")
    )
    print(f"  effective max_threads: {status['effective']}")
    if status["override"] is not None:
        scope = status.get("override_run_id")
        print(f"  override scope: {'active run ' + scope if scope else 'next run only'}")
    if status["active"]:
        run = status["run"] or {}
        print(
            f"  active run: {run.get('command', 'run')} "
            f"{run.get('start_node', '?')} ({run.get('run_id', '?')})"
        )


def list_thread_statuses(root: Path, storage: FileStorage) -> int:
    node_root = root / "node"
    nodes = sorted(
        path.name
        for path in node_root.iterdir()
        if path.is_dir() and (path / "schema.json").is_file()
    ) if node_root.is_dir() else []
    if not nodes:
        print("No mounted nodes found. Run 'mwf graph --update' first.")
        return 0

    statuses = [thread_status(root, storage, node) for node in nodes]
    api_total = storage.read_api_total_limit()
    if api_total is not None:
        requested = {
            status["node"]: status["effective"]
            for status in statuses
            if status["runner"] == "api"
        }
        allocated = _proportional_api_limits(requested, api_total)
        for status in statuses:
            if status["node"] in allocated:
                status["effective"] = allocated[status["node"]]

    print("Runtime max_threads")
    if api_total is None:
        print("API aggregate admission budget: (none)")
    else:
        print(f"API aggregate admission budget: {api_total} (proportional by node request)")
    print("node                     runner    declared  override  effective")
    print("-----------------------  --------  --------  --------  ---------")
    for status in statuses:
        node = status["node"]
        override = status["override"] if status["override"] is not None else "-"
        print(
            f"{node[:23]:23}  {status['runner'][:8]:8}  "
            f"{status['declared']:8}  {str(override):8}  {status['effective']:9}"
        )
    return 0


def api_total_command(root: Path, value: str) -> int:
    storage = FileStorage(root)
    try:
        with storage.interprocess_lock("active-run-state"):
            active = live_active_run(storage)
            run_id = (active or {}).get("run_id")
            if value.lower() in RESET_WORDS:
                existed = storage.clear_api_total_limit(run_id=run_id)
                print(
                    "Cleared aggregate API admission budget."
                    if existed
                    else "Aggregate API admission budget was already unset."
                )
                return 0

            current = storage.read_api_total_limit() or 1
            requested = _parse_new_limit(value, current, maximum=None)
            storage.set_api_total_limit(requested, run_id=run_id)
        print(f"Updated aggregate API admission budget: {requested}")
        print(
            "MWF allocates the budget proportionally across API nodes in the active run; "
            "per-node max_threads values remain the weights and upper bounds."
        )
        if active is not None:
            print("Active API runners will apply the new budget within about 0.2 seconds.")
        else:
            print("The budget is pending for the next run only and clears when that run finishes.")
        return 0
    finally:
        storage.close_database_connections()


def update_declared_threads(root: Path) -> int:
    """Reload node behavior files and refresh mounted schema concurrency values."""
    storage = FileStorage(root)
    before: dict[str, tuple[int, str]] = {}
    node_root = root / "node"
    if node_root.is_dir():
        for path in node_root.iterdir():
            if path.is_dir() and (path / "schema.json").is_file():
                try:
                    schema = _node_schema(storage, path.name)
                    before[path.name] = (int(schema.get("max_threads", 0)), str(schema.get("runner_override") or ""))
                except (RuntimeError, TypeError, ValueError):
                    pass

    workflow = load_workflow(root, require_synced=True)
    updated = 0
    unchanged = 0
    print("Refreshing declared max_threads from src/node_behavior ...")
    for node_name in sorted(workflow.graph_obj.nodes):
        schema = _node_schema(storage, node_name)
        new_limit = int(schema["max_threads"])
        new_runner = str(schema.get("runner_override") or "")
        old = before.get(node_name)
        if old is None or old != (new_limit, new_runner):
            old_text = "(unmounted)" if old is None else str(old[0])
            runner_text = new_runner or "project default"
            print(f"  {node_name}: {old_text} -> {new_limit} ({runner_text})")
            updated += 1
        else:
            unchanged += 1
    print(f"Updated declarations: {updated}; unchanged: {unchanged}")
    print("Runtime overrides were preserved. Use 'mwf threads NODE reset' to clear one.")
    workflow.storage.close_database_connections()
    return 0

def threads_command(root: Path, node: str | None, value: str | None) -> int:
    storage = FileStorage(root)
    if node is None:
        return list_thread_statuses(root, storage)

    node = safe_node_name(node)
    before = thread_status(root, storage, node)

    if value is None:
        print_thread_status(before)
        return 0

    if value.lower() in RESET_WORDS:
        # Serialize active-run discovery with run startup. Otherwise a threads
        # command can observe "no run", wait behind startup's override lock,
        # and then incorrectly write a next-run override after the run is live.
        with storage.interprocess_lock("active-run-state"):
            active = live_active_run(storage)
            storage.clear_thread_override(node, run_id=(active or {}).get("run_id"))
        after = thread_status(root, storage, node)
        print(f"Cleared runtime max_threads override for {node}.")
        print(f"Effective max_threads: {before['effective']} -> {after['effective']}")
        return 0

    maximum = None if before["runner"] == "api" else MAX_RUNTIME_THREADS
    requested = _parse_new_limit(value, before["effective"], maximum=maximum)
    if requested > HIGH_RUNTIME_THREAD_WARNING:
        if before["runner"] == "api":
            print(
                f"Notice: {requested} is a large cooperative API fiber count. It does not "
                "create one OS thread per job, but provider, socket, memory, and rate limits "
                "still apply.",
                file=sys.stderr,
            )
        else:
            print(
                f"Warning: {requested} is an extreme local concurrency setting for OS workers. "
                "Increase gradually and watch system thread, process, and memory limits.",
                file=sys.stderr,
            )
    with storage.interprocess_lock("active-run-state"):
        active = live_active_run(storage)
        storage.set_thread_override(node, requested, run_id=(active or {}).get("run_id"))
    after = thread_status(root, storage, node)

    print(f"Updated runtime max_threads for {node}: {before['effective']} -> {after['effective']}")
    if after["runner"] in {"threaded", "api"}:
        if after["active"]:
            runner_name = "API" if after["runner"] == "api" else "threaded"
            print(
                f"The active {runner_name} runner will apply the new limit within about "
                "0.2 seconds. Lowering the limit does not cancel jobs already running."
            )
        else:
            print("The override is pending for the next run only and will be cleared when that run finishes.")
    elif after["runner"] == "process":
        print(
            "The process runner reads this override when its node pool is created; "
            "an already-created process pool is not resized live."
        )
    else:
        print("This node uses the direct runner, so its effective concurrency remains 1.")
    return 0


def threads_cli(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        prog="mwf threads",
        description=(
            "View or change run-scoped concurrency controls. Per-node API values are "
            "cooperative fiber counts; --api-total adds a proportional workflow budget."
        ),
    )
    parser.add_argument(
        "node",
        nargs="?",
        help="Node name. Omit to list every mounted node.",
    )
    parser.add_argument(
        "value",
        nargs="?",
        help="Absolute integer, +N, -N, or reset/default/clear.",
    )
    parser.add_argument(
        "--api-total",
        metavar="VALUE",
        help="Set the aggregate API admission budget, or use reset/default/clear.",
    )
    parser.add_argument(
        "--update",
        action="store_true",
        help="Reload node behavior files and refresh declared max_threads/runner values.",
    )
    args = parser.parse_args(argv)

    try:
        root = find_root()
        ensure_runtime_layout(root)
        if args.update:
            if args.node is not None or args.value is not None or args.api_total is not None:
                raise RuntimeError("mwf threads --update does not accept a node or runtime value")
            return update_declared_threads(root)
        if args.api_total is not None:
            if args.node is not None or args.value is not None:
                raise RuntimeError("mwf threads --api-total does not accept a node or node value")
            return api_total_command(root, args.api_total)
        return threads_command(root, args.node, args.value)
    except Exception as error:
        print(f"Error: {error}", file=sys.stderr)
        return 1
