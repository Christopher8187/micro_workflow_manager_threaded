from __future__ import annotations

from pathlib import Path

from micro_workflow_manager.paths import config_file
from micro_workflow_manager.project_format import read_native_project_config


def has_project_marker(root: Path) -> bool:
    # Recognize old markers only to locate the project and explain refusal.
    return config_file(root).is_file() or (root / '.mwf').is_file()


def ensure_runtime_layout(root: Path) -> None:
    """Validate the sole native project format without moving or creating files."""
    read_native_project_config(root)
