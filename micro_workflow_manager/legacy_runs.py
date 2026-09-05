from __future__ import annotations

import json
import stat
from pathlib import Path

from .paths import LEGACY_RUN_NAME, run_file, state_database_file
from .session_liveness import execution_session_liveness


def read_legacy_run_records(root: Path) -> list[tuple[Path, dict]]:
    """Read both exact run paths without choosing one or changing either."""
    records = []
    errors = []
    for path in (root / LEGACY_RUN_NAME, run_file(root)):
        label = path.relative_to(root).as_posix()
        try:
            entry = path.lstat()
        except (FileNotFoundError, NotADirectoryError):
            continue
        except OSError as error:
            errors.append(f"{label}: {error}")
            continue
        reparse = getattr(entry, "st_file_attributes", 0) & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
        if not stat.S_ISREG(entry.st_mode) or reparse:
            errors.append(f"{label}: expected a regular run file without links")
            continue
        try:
            state = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as error:
            errors.append(f"{label}: {error}")
            continue
        if not isinstance(state, dict):
            errors.append(f"{label}: expected a JSON object")
            continue
        records.append((path, state))
    if errors:
        raise RuntimeError("Cannot inspect legacy run records: " + "; ".join(errors))
    return records


def preflight_legacy_storage_creation(root: Path) -> None:
    """Refuse an observed live legacy owner before creating a missing store."""
    records = read_legacy_run_records(root)
    # Existing SQLite completion cannot be inspected here without the separate
    # read-only coordination decision. Do not mistake existence for completion.
    if not records or state_database_file(root).is_file():
        return
    live = [(path, state) for path, state in records if execution_session_liveness(state)["live"]]
    if live:
        names = ", ".join(
            f"{state.get('run_id', '?')} ({path.relative_to(root).as_posix()})"
            for path, state in live
        )
        raise RuntimeError(f"Cannot initialize SQLite during migration while legacy runs are alive: {names}.")
