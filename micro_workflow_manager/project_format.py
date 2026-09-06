from __future__ import annotations

import json
import stat
from pathlib import Path

from .paths import config_file
from .schema import CURRENT_STATE_SCHEMA_VERSION


PROJECT_FORMAT_VERSION = 5
UNSUPPORTED_PROJECT = (
    "Unsupported MWF project format. Use migration.md to prepare a separate "
    "fresh project; this project has not been migrated."
)


def is_link_or_reparse_point(path: Path) -> bool:
    try:
        entry = path.lstat()
    except FileNotFoundError:
        return False
    return stat.S_ISLNK(entry.st_mode) or bool(
        getattr(entry, 'st_file_attributes', 0)
        & getattr(stat, 'FILE_ATTRIBUTE_REPARSE_POINT', 0)
    )


def require_unchanged_path(path: Path, identity) -> None:
    """Refuse replacement of a path exclusively claimed during creation."""
    try:
        current = path.lstat()
    except OSError as error:
        raise RuntimeError(f'The new project path changed during initialization: {path}') from error
    if (is_link_or_reparse_point(path)
            or (identity.st_dev, identity.st_ino) != (current.st_dev, current.st_ino)):
        raise RuntimeError(f'The new project path changed during initialization: {path}')


def read_native_project_config(root: Path) -> dict:
    """Admit only the current project format without opening SQLite or writing."""
    root = Path(root)
    path = config_file(root)
    if is_link_or_reparse_point(path.parent) or is_link_or_reparse_point(path):
        raise RuntimeError(UNSUPPORTED_PROJECT)
    try:
        config = json.loads(path.read_text(encoding='utf-8'))
    except (OSError, ValueError) as error:
        raise RuntimeError(UNSUPPORTED_PROJECT) from error
    if (
        not isinstance(config, dict)
        or type(config.get('version')) is not int
        or config['version'] != PROJECT_FORMAT_VERSION
        or type(config.get('schema_version')) is not int
        or config['schema_version'] != CURRENT_STATE_SCHEMA_VERSION
    ):
        raise RuntimeError(UNSUPPORTED_PROJECT)
    return config


def new_project_config() -> dict:
    return {
        'version': PROJECT_FORMAT_VERSION,
        'schema_version': CURRENT_STATE_SCHEMA_VERSION,
        'graph_path': None,
        'runner': 'threaded',
        'edges': [],
    }
