from __future__ import annotations

import json
import shutil
from pathlib import Path

from .constants import MWF_FILE

def safe_node_name(name: str) -> str:
    if not name or name in {".", ".."}:
        raise ValueError("Invalid node name")

    if any(part in name for part in ["/", "\\", ".."]):
        raise ValueError(f"Unsafe node name: {name}")

    return name

def safe_node_dir(root: Path, node: str) -> Path:
    safe_node_name(node)
    base = (root / "node").resolve()
    path = (base / node).resolve()

    try:
        path.relative_to(base)
    except ValueError as error:
        raise ValueError(f"Unsafe node path: {path}") from error

    return path

def remove_dir(path: Path):
    if path.exists():
        if not path.is_dir():
            raise ValueError(f"Expected directory: {path}")
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)

def remove_path(path: Path):
    if path.is_dir():
        shutil.rmtree(path)
    elif path.exists():
        path.unlink()

def find_root(start: Path | None = None) -> Path:
    path = (start or Path.cwd()).resolve()

    for folder in [path, *path.parents]:
        if (folder / MWF_FILE).exists():
            return folder

    raise RuntimeError("Not an mwf project. Run: mwf init")

def read_config(root: Path) -> dict:
    path = root / MWF_FILE

    if not path.exists():
        raise RuntimeError("Not an mwf project. Run: mwf init")

    return json.loads(path.read_text(encoding="utf-8"))

def write_json(path: Path, data: dict):
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
