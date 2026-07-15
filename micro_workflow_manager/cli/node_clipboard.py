from __future__ import annotations

import shutil
import uuid
from pathlib import Path

from .extras.scaffold import ensure_vscode_settings


def clipboard_root(root: Path) -> Path:
    return root / "clipboard"


def copy_node_to_clipboard(root: Path, node: str) -> int:
    source = root / "node" / node
    if not source.is_dir():
        raise RuntimeError(f"Node folder does not exist: {source}")
    destination = clipboard_root(root) / node
    temporary = clipboard_root(root) / f".{node}.copying-{uuid.uuid4().hex}"
    clipboard_root(root).mkdir(parents=True, exist_ok=True)
    if temporary.exists():
        shutil.rmtree(temporary)
    print(f"Copying node/{node} to clipboard/{node} ...")
    shutil.copytree(source, temporary)
    file_count = sum(1 for item in temporary.rglob("*") if item.is_file())
    if destination.exists():
        print(f"Replacing previous clipboard copy: {destination}")
        shutil.rmtree(destination)
    temporary.replace(destination)
    print(f"Saved clipboard node: {destination}")
    print(f"  files copied: {file_count}")
    ensure_vscode_settings(root)
    return 0


def paste_node_from_clipboard(root: Path, node: str) -> int:
    source = clipboard_root(root) / node
    if not source.is_dir():
        raise RuntimeError(f"Clipboard does not contain node {node!r}: {source}")
    node_root = root / "node"
    destination = node_root / node
    temporary = node_root / f".{node}.pasting-{uuid.uuid4().hex}"
    node_root.mkdir(parents=True, exist_ok=True)
    print(f"Preparing clipboard/{node} for node/{node} ...")
    shutil.copytree(source, temporary)
    file_count = sum(1 for item in temporary.rglob("*") if item.is_file())
    if destination.exists():
        print(f"Removing current node folder: {destination}")
        shutil.rmtree(destination)
    temporary.replace(destination)
    print(f"Restored node from clipboard: {destination}")
    print(f"  files pasted: {file_count}")
    ensure_vscode_settings(root)
    return 0
