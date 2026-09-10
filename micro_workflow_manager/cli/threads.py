from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from micro_workflow_manager.runners.threaded import (
    HIGH_RUNTIME_THREAD_WARNING,
    MAX_RUNTIME_THREADS,
)
from micro_workflow_manager.storage import FileStorage

from .files import find_root, read_config, safe_node_name
from .preview import PreviewStorage
from .project import load_workflow
from .startup_recovery import recover_before_mutation


RESET_WORDS = {"reset", "default", "clear"}
_UNREAD_API_LIMIT = object()


def _node_schema(root: Path, node: str) -> dict:
    path = root / "node" / node / "schema.json"
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as error:
        raise RuntimeError(
            f"No mounted schema for node {node}. Run 'mwf graph --update' first."
        ) from error
    if not isinstance(data, dict):
        raise RuntimeError(
            f"No mounted schema for node {node}. Run 'mwf graph --update' first."
        )
    return data


def _declared_limit(root: Path, node: str) -> int:
    value = _node_schema(root, node).get("max_threads")
    if type(value) is not int or value < 1:
        raise RuntimeError(f"Node {node} has an invalid declared max_threads value")
    return value


def _effective_runner(root: Path, schema: dict) -> str:
    override = schema.get("runner_override")
    if isinstance(override, str) and override:
        return override
    value = read_config(root).get("runner", "threaded")
    return value if isinstance(value, str) else "threaded"


def _parse_new_limit(spec: str, current: int, *, maximum: int | None) -> int:
    text = spec.strip()
    if not text:
        raise ValueError("thread limit cannot be empty")
    try:
        value = current + int(text) if text[0] in {"+", "-"} and len(text) > 1 else int(text)
    except ValueError as error:
        raise ValueError("use an integer, +N, -N, or reset") from error
    if value < 1:
        raise ValueError("effective max_threads must remain at least 1")
    if maximum is not None and value > maximum:
        raise ValueError(f"effective max_threads cannot exceed {maximum}")
    return value


def thread_status(
    root: Path,
    storage,
    node: str,
    *,
    api_total_limit=_UNREAD_API_LIMIT,
) -> dict:
    declared = _declared_limit(root, node)
    observation = storage.read_thread_override_observation(node)
    schema = _node_schema(root, node)
    runner = _effective_runner(root, schema)
    override = observation["value"]
    effective = 1 if runner == "direct" else (override or declared)
    if api_total_limit is _UNREAD_API_LIMIT:
        api_total_limit = storage.read_api_total_limit()
    return {
        "node": node,
        "declared": declared,
        "override": override,
        "effective": effective,
        "runner": runner,
        "session_id": observation["session_id"],
        "observation": observation,
        "api_total_limit": api_total_limit,
    }


def print_thread_status(status: dict) -> None:
    print(f"Node {status['node']}")
    print(f"  runner: {status['runner']}")
    print(f"  declared max_threads: {status['declared']}")
    print(
        "  runtime override: "
        + (str(status["override"]) if status["override"] is not None else "(none)")
    )
    print(f"  requested max_threads: {status['effective']}")
    if status["runner"] == "api":
        limit = status["api_total_limit"]
        print(f"  project-wide API limit: {limit if limit is not None else '(none)'}")
    if status["override"] is not None:
        owner = status["session_id"]
        print(f"  override scope: {'session ' + owner if owner else 'pending next claimant'}")
    if status["session_id"] is not None:
        print(f"  active session: {status['session_id']}")


def list_thread_statuses(root: Path, storage) -> int:
    node_root = root / "node"
    nodes = sorted(
        path.name
        for path in node_root.iterdir()
        if path.is_dir() and (path / "schema.json").is_file()
    ) if node_root.is_dir() else []
    if not nodes:
        print("No mounted nodes found. Run 'mwf graph --update' first.")
        return 0

    api_total = storage.read_api_total_limit()
    statuses = [
        thread_status(root, storage, node, api_total_limit=api_total)
        for node in nodes
    ]

    print("Runtime max_threads")
    print(
        "Project-wide API limit: (none)" if api_total is None else
        f"Project-wide API limit: {api_total}"
    )
    print("node                     runner    declared  override  requested")
    print("-----------------------  --------  --------  --------  ---------")
    for status in statuses:
        override = status["override"] if status["override"] is not None else "-"
        print(
            f"{status['node'][:23]:23}  {status['runner'][:8]:8}  "
            f"{status['declared']:8}  {str(override):8}  {status['effective']:9}"
        )
    return 0


def _warn_large_limit(status, requested):
    if requested <= HIGH_RUNTIME_THREAD_WARNING:
        return
    if status["runner"] == "api":
        print(
            f"Notice: {requested} is a large cooperative API fiber count. It does not "
            "create one OS thread per job, but provider, socket, memory, and rate limits "
            "still apply.", file=sys.stderr,
        )
    else:
        print(
            f"Warning: {requested} is an extreme local concurrency setting for OS workers. "
            "Increase gradually and watch system thread, process, and memory limits.",
            file=sys.stderr,
        )


def _read_preview_status(root, node=None):
    storage = PreviewStorage(root)
    try:
        if node is None:
            return list_thread_statuses(root, storage)
        return thread_status(root, storage, node)
    finally:
        storage.close()


def threads_command(root: Path, node: str | None, value: str | None) -> int:
    if node is None:
        return _read_preview_status(root)
    node = safe_node_name(node)
    before_recovery = _read_preview_status(root, node)
    if value is None:
        print_thread_status(before_recovery)
        return 0

    recover_before_mutation(root)
    storage = FileStorage(root)
    try:
        before = thread_status(root, storage, node)
        expected = before["observation"]
        if value.lower() in RESET_WORDS:
            storage.clear_thread_override(node, expected=expected)
            after = thread_status(root, storage, node)
            print(f"Cleared runtime max_threads override for {node}.")
            print(f"Requested max_threads: {before['effective']} -> {after['effective']}")
            if before["session_id"] is not None:
                print(f"Updated owning session {before['session_id']}.")
            return 0

        maximum = None if before["runner"] == "api" else MAX_RUNTIME_THREADS
        requested = _parse_new_limit(value, before["effective"], maximum=maximum)
        _warn_large_limit(before, requested)
        storage.set_thread_override(node, requested, expected=expected)
        after = thread_status(root, storage, node)
        print(f"Updated runtime max_threads override for {node}: {after['override']}")
        print(f"Requested max_threads: {before['effective']} -> {after['effective']}")
        if after["session_id"] is not None:
            print(f"Updated owning session {after['session_id']}.")
        else:
            print("The override is pending for the next session that owns this node.")
        return 0
    finally:
        storage.close_database_connections()


def api_total_command(root: Path, value: str) -> int:
    storage = FileStorage(root)
    try:
        if value.lower() in RESET_WORDS:
            existed = storage.clear_api_total_limit()
            print("Cleared aggregate API admission budget." if existed else
                  "Aggregate API admission budget was already unset.")
            return 0
        text = value.strip()
        relative = bool(text) and text[0] in {"+", "-"} and len(text) > 1
        if relative:
            try:
                change = int(text)
            except ValueError as error:
                raise ValueError("use an integer, +N, -N, or reset") from error
            requested = storage.update_api_total_limit(change, relative=True)
        else:
            requested = _parse_new_limit(value, 1, maximum=None)
            requested = storage.update_api_total_limit(requested, relative=False)
        print(f"Updated aggregate API admission budget: {requested}")
        return 0
    finally:
        storage.close_database_connections()


def update_declared_threads(root: Path) -> int:
    storage = FileStorage(root)
    before = {}
    try:
        node_root = root / "node"
        if node_root.is_dir():
            for path in node_root.iterdir():
                if path.is_dir() and (path / "schema.json").is_file():
                    schema = _node_schema(root, path.name)
                    before[path.name] = (
                        int(schema.get("max_threads", 0)),
                        str(schema.get("runner_override") or ""),
                    )
        workflow = load_workflow(root, require_synced=True)
        updated = 0
        unchanged = 0
        print("Refreshing declared max_threads from src/node_behavior ...")
        for node_name in sorted(workflow.graph_obj.nodes):
            schema = _node_schema(root, node_name)
            current = (int(schema["max_threads"]), str(schema.get("runner_override") or ""))
            if before.get(node_name) != current:
                print(f"  {node_name}: {before.get(node_name, ('(unmounted)',))[0]} -> {current[0]}")
                updated += 1
            else:
                unchanged += 1
        print(f"Updated declarations: {updated}; unchanged: {unchanged}")
        print("Native runtime overrides were preserved.")
        workflow.storage.close_database_connections()
        return 0
    finally:
        storage.close_database_connections()


def threads_cli(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        prog="mwf threads",
        description=(
            "View or change native session-scoped node concurrency. The "
            "deprecated --api-total form retains one project-wide API value."
        ),
    )
    parser.add_argument("node", nargs="?", help="Node name; omit to list mounted nodes.")
    parser.add_argument(
        "value", nargs="?", help="Absolute integer, +N, -N, or reset/default/clear.",
    )
    parser.add_argument(
        "--api-total", metavar="VALUE",
        help="Deprecated: set or clear the project-wide aggregate API admission value.",
    )
    parser.add_argument(
        "--update", action="store_true",
        help="Reload declared max_threads and runner values from node behavior files.",
    )
    args = parser.parse_args(argv)
    if args.api_total is not None:
        print(
            "Deprecation warning: mwf threads --api-total is deprecated and remains functional.",
            file=sys.stderr,
        )
    try:
        root = find_root()
        if args.update and (args.node is not None or args.value is not None or args.api_total is not None):
            raise RuntimeError("mwf threads --update does not accept a node or runtime value")
        if args.api_total is not None and (args.node is not None or args.value is not None):
            raise RuntimeError("mwf threads --api-total does not accept a node or node value")
        if args.update or args.api_total is not None:
            recover_before_mutation(root)
        if args.update:
            return update_declared_threads(root)
        if args.api_total is not None:
            return api_total_command(root, args.api_total)
        return threads_command(root, args.node, args.value)
    except Exception as error:
        print(f"Error: {error}", file=sys.stderr)
        return 1
