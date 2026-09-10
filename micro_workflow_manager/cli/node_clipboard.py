from __future__ import annotations

from pathlib import Path

from micro_workflow_manager.project_format import is_link_or_reparse_point, read_native_project_config
from micro_workflow_manager.storage import FileStorage
from micro_workflow_manager.storage.clipboard_snapshot import (
    observe_clipboard_node,
    open_native_snapshot,
    read_project_id,
    require_clipboard_payload,
    require_no_live_clipboard_session,
)
from micro_workflow_manager.storage.clipboard_state import validate_clipboard_support
from micro_workflow_manager.storage.sqlite.preview_snapshot import open_preview_snapshot

from .extras.scaffold import ensure_vscode_settings


SNAPSHOT_NAME = ".mwf-node-state.sqlite3"


def clipboard_root(root: Path) -> Path:
    return root / "clipboard"


def validate_clipboard_request(root: Path, operation: str, node: str) -> None:
    """Validate public source/provenance before startup recovery may mutate state."""
    read_native_project_config(root)
    if operation not in ("copy", "paste"):
        raise ValueError("Unknown clipboard operation")
    if operation == "copy":
        source = root / "node" / node
        if is_link_or_reparse_point(source) or not source.is_dir():
            raise RuntimeError(f"Node folder does not exist or is not ordinary: {source}")
    else:
        source = clipboard_root(root) / node
        snapshot_path = source / SNAPSHOT_NAME
        if (is_link_or_reparse_point(source) or not source.is_dir()
                or is_link_or_reparse_point(snapshot_path) or not snapshot_path.is_file()):
            raise RuntimeError(f"Clipboard does not contain an ordinary native node {node!r}: {source}")
    connection = open_preview_snapshot(root / ".mwf" / "state.sqlite3")
    try:
        current = observe_clipboard_node(connection, node)
        require_no_live_clipboard_session(connection, current)
        if operation == "copy":
            require_clipboard_payload(source, current)
        else:
            saved, saved_observation = open_native_snapshot(
                snapshot_path, node, read_project_id(connection),
            )
            try:
                require_clipboard_payload(source, saved_observation)
                validate_clipboard_support(connection, saved_observation, current)
            finally:
                saved.close()
        if node not in current.component:
            raise RuntimeError("Clipboard node is outside current native membership")
    finally:
        connection.close()


def copy_node_to_clipboard(root: Path, node: str) -> int:
    validate_clipboard_request(root, "copy", node)
    source = root / "node" / node
    if not source.is_dir():
        raise RuntimeError(f"Node folder does not exist: {source}")
    storage = FileStorage(root)
    destination = clipboard_root(root) / node
    print(f"Copying node/{node} to clipboard/{node} ...")
    if destination.exists():
        print(f"Replacing previous clipboard copy: {destination}")
    try:
        result = storage.capture_node_clipboard(node, destination)
    finally:
        storage.close_database_connections()
    file_count = sum(1 for item in destination.rglob("*") if item.is_file())
    print(f"Saved clipboard node: {destination}")
    print(f"  files copied: {file_count}")
    print(f"  native jobs retained: {result['jobs']}")
    print("  Native job identity, supporting execution history, and component result included")
    ensure_vscode_settings(root)
    return 0


def paste_node_from_clipboard(root: Path, node: str) -> int:
    validate_clipboard_request(root, "paste", node)
    source = clipboard_root(root) / node
    if not source.is_dir():
        raise RuntimeError(f"Clipboard does not contain node {node!r}: {source}")
    storage = FileStorage(root)
    snapshot = source / SNAPSHOT_NAME
    if is_link_or_reparse_point(snapshot) or not snapshot.is_file():
        raise RuntimeError("Native clipboard state snapshot is missing or is not an ordinary file")
    print(f"Preparing clipboard/{node} for node/{node} ...")
    try:
        result = storage.restore_node_clipboard(node, source)
    finally:
        storage.close_database_connections()
    destination = root / "node" / node
    file_count = sum(1 for item in destination.rglob("*") if item.is_file())
    print(f"Restored node from clipboard: {destination}")
    print(f"  files pasted: {file_count}")
    print("  Native job identity, supporting execution history, and component result restored")
    print(
        f"  jobs available: {result['jobs']} "
        f"(alignment generation: {result['alignment_generation']})"
    )
    ensure_vscode_settings(root)
    return 0
