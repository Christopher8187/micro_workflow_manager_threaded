from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from micro_workflow_manager.paths import state_database_file
from micro_workflow_manager.schema import CURRENT_STATE_SCHEMA_VERSION, STATE_SCHEMA_FIELD
from micro_workflow_manager.storage import FileStorage


_METADATA_PATTERNS = (
    ".mwf/project.json",
    ".mwf/run.json",
    ".mwf/threads.json",
    "node/*/schema.json",
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
    return {"outdated": outdated, "current": current, "newer": newer, "malformed": malformed}


def _read_only_database_integrity(path: Path) -> str:
    if not path.is_file():
        return "not initialized"
    uri = f"file:{path.resolve().as_posix()}?mode=ro"
    connection = sqlite3.connect(uri, uri=True, timeout=5.0)
    try:
        row = connection.execute("PRAGMA quick_check").fetchone()
        return str(row[0]) if row is not None else "unknown"
    finally:
        connection.close()


def migrate_command(root: Path, *, dry_run: bool = False) -> int:
    plan = migration_plan(root)
    if plan["malformed"]:
        names = ", ".join(path.relative_to(root).as_posix() for path in plan["malformed"])
        raise RuntimeError(f"Cannot migrate malformed framework JSON: {names}")
    if plan["newer"]:
        names = ", ".join(path.relative_to(root).as_posix() for path in plan["newer"])
        raise RuntimeError(
            f"State was written by a newer MWF schema: {names}. Install a compatible newer package instead of downgrading it."
        )

    database_path = state_database_file(root)
    verb = "Would migrate" if dry_run else "Migrated"
    if dry_run:
        if database_path.is_file():
            print(f"SQLite state database: {database_path}")
            print(f"  integrity: {_read_only_database_integrity(database_path)}")
        else:
            print(f"Would initialize SQLite state database: {database_path}")
            print("  legacy job/status/queue/idempotency metadata would be imported once")
    else:
        # Opening storage creates/updates the SQLite schema and imports legacy
        # job/status/queue/idempotency metadata once. User input/output files are
        # never moved into the database.
        storage = FileStorage(root)
        for path in plan["outdated"]:
            data = storage.read_json(path)
            if not isinstance(data, dict):
                raise RuntimeError(f"Framework metadata must be a JSON object: {path}")
            data[STATE_SCHEMA_FIELD] = CURRENT_STATE_SCHEMA_VERSION
            storage.atomic_write_json(path, data)
        print(f"SQLite state database: {storage.state_database_path()}")
        print(f"  integrity: {storage.database_integrity_check()}")

    if plan["outdated"]:
        print(f"{verb} {len(plan['outdated'])} JSON metadata file(s) to schema {CURRENT_STATE_SCHEMA_VERSION}:")
        for path in plan["outdated"]:
            print(f"  {path.relative_to(root).as_posix()}")
    else:
        print(f"State schema is already current: {CURRENT_STATE_SCHEMA_VERSION}")
    print("Job metadata, queue state, events, execution leases, and idempotency keys are stored in SQLite.")
    print("User input.json, output.json, returned files, node input/, and node output/ were not changed.")
    return 0
