from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from micro_workflow_manager.schema import CURRENT_STATE_SCHEMA_VERSION, STATE_SCHEMA_FIELD
from micro_workflow_manager.storage import FileStorage


_METADATA_PATTERNS = (
    ".mwf",
    ".mwf_run.json",
    "node/*/node_state.json",
    "node/*/schema.json",
    "node/*/default_jobs.json",
    "node/*/job_index.json",
    "node/*/jobs/*/job.json",
    "node/*/jobs/*/status.json",
    "node/*/jobs/*/execution.json",
    "node/*/idempotency/*.json",
)


def framework_metadata_files(root: Path) -> list[Path]:
    paths: set[Path] = set()
    for pattern in _METADATA_PATTERNS:
        paths.update(path for path in root.glob(pattern) if path.is_file())
    return sorted(paths)


def state_schema_version(data: Any) -> int | None:
    if not isinstance(data, dict):
        return None
    value = data.get(STATE_SCHEMA_FIELD)
    return value if type(value) is int else None


def migration_plan(root: Path) -> dict[str, list[Path]]:
    outdated: list[Path] = []
    current: list[Path] = []
    newer: list[Path] = []
    malformed: list[Path] = []

    for path in framework_metadata_files(root):
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            malformed.append(path)
            continue
        version = state_schema_version(data)
        if version is None or version < CURRENT_STATE_SCHEMA_VERSION:
            outdated.append(path)
        elif version > CURRENT_STATE_SCHEMA_VERSION:
            newer.append(path)
        else:
            current.append(path)

    return {
        "outdated": outdated,
        "current": current,
        "newer": newer,
        "malformed": malformed,
    }


def migrate_command(root: Path, *, dry_run: bool = False) -> int:
    plan = migration_plan(root)
    if plan["malformed"]:
        names = ", ".join(path.relative_to(root).as_posix() for path in plan["malformed"])
        raise RuntimeError(f"Cannot migrate malformed framework JSON: {names}")
    if plan["newer"]:
        names = ", ".join(path.relative_to(root).as_posix() for path in plan["newer"])
        raise RuntimeError(
            f"State was written by a newer MWF schema: {names}. "
            "Install a compatible newer package instead of downgrading it."
        )

    verb = "Would migrate" if dry_run else "Migrated"
    if not plan["outdated"]:
        print(f"State schema is already current: {CURRENT_STATE_SCHEMA_VERSION}")
        return 0

    if not dry_run:
        storage = FileStorage(root)
        for path in plan["outdated"]:
            data = storage.read_json(path)
            if not isinstance(data, dict):
                raise RuntimeError(f"Framework metadata must be a JSON object: {path}")
            data[STATE_SCHEMA_FIELD] = CURRENT_STATE_SCHEMA_VERSION
            storage.atomic_write_json(path, data)

    print(f"{verb} {len(plan['outdated'])} framework metadata file(s) to schema {CURRENT_STATE_SCHEMA_VERSION}:")
    for path in plan["outdated"]:
        print(f"  {path.relative_to(root).as_posix()}")
    print("User input.json, output.json, returned files, and events.jsonl were not changed.")
    return 0
