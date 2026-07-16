from __future__ import annotations

import json
import shutil
from pathlib import Path

from micro_workflow_manager.paths import (
    LEGACY_CONFIG_NAME,
    LEGACY_LOCKS_NAME,
    LEGACY_RUN_NAME,
    LEGACY_THREADS_NAME,
    config_file,
    locks_dir,
    mwf_dir,
    run_file,
    threads_file,
)


def has_project_marker(root: Path) -> bool:
    legacy = root / LEGACY_CONFIG_NAME
    return config_file(root).is_file() or legacy.is_file()


def ensure_runtime_layout(root: Path) -> bool:
    """Move 0.2.6-and-earlier root state into the consolidated .mwf folder.

    Returns True when at least one legacy path was migrated. The operation is
    deliberately small and idempotent so any 0.3.x command can open an older
    project without requiring a separate manual migration first.
    """

    legacy_config = root / LEGACY_CONFIG_NAME
    target_dir = mwf_dir(root)
    target_config = config_file(root)
    migrated = False

    if legacy_config.is_file():
        try:
            data = json.loads(legacy_config.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            raise RuntimeError(f"Cannot migrate legacy .mwf project file: {error}") from error
        if not isinstance(data, dict):
            raise RuntimeError("Cannot migrate legacy .mwf project file: expected a JSON object")
        data["version"] = 4
        legacy_config.unlink()
        target_dir.mkdir(parents=True, exist_ok=True)
        target_config.write_text(
            json.dumps(data, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        migrated = True
    elif target_dir.exists() and not target_dir.is_dir():
        raise RuntimeError(f"Expected .mwf to be a directory: {target_dir}")

    if target_config.exists():
        target_dir.mkdir(parents=True, exist_ok=True)

    file_moves = [
        (root / LEGACY_RUN_NAME, run_file(root)),
        (root / LEGACY_THREADS_NAME, threads_file(root)),
    ]
    for source, destination in file_moves:
        if not source.exists():
            continue
        destination.parent.mkdir(parents=True, exist_ok=True)
        if destination.exists():
            source.unlink()
        else:
            source.replace(destination)
        migrated = True

    # Lock files have no durable meaning. MWF 0.3.4 stores short advisory
    # leases in SQLite, so legacy lock directories can be removed safely.
    for obsolete_locks in (root / LEGACY_LOCKS_NAME, locks_dir(root)):
        if obsolete_locks.exists():
            if obsolete_locks.is_dir():
                shutil.rmtree(obsolete_locks)
            else:
                obsolete_locks.unlink()
            migrated = True

    return migrated
