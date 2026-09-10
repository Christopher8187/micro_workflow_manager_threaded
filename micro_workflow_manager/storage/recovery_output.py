"""Read exact abandoned output identities without changing project files."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import stat

from .input_publication_files import check_local_path


@dataclass(frozen=True, slots=True)
class RecoveryFileObservation:
    relative_path: str
    marker: tuple[int, ...] | None
    content: bytes | None

    def manifest(self):
        return {
            'path': self.relative_path,
            'marker': None if self.marker is None else list(self.marker),
            'sha256': None if self.content is None else hashlib.sha256(self.content).hexdigest(),
        }


def recovery_path(root: Path, relative: str) -> Path:
    if not isinstance(relative, str) or '\\' in relative:
        raise ValueError('Recovery requires an ordinary relative path')
    parts = relative.split('/')
    if any(part in ('', '.', '..') or ':' in part for part in parts):
        raise ValueError('Recovery requires an ordinary relative path')
    path = root.joinpath(*parts)
    check_local_path(root, path)
    return path


def file_marker(value):
    return value.st_dev, value.st_ino, value.st_mode, value.st_size, value.st_mtime_ns


def observe_recovery_file(root: Path, relative: str) -> RecoveryFileObservation:
    path = recovery_path(root, relative)
    try:
        before = path.lstat()
    except FileNotFoundError:
        return RecoveryFileObservation(relative, None, None)
    if not stat.S_ISREG(before.st_mode):
        raise RuntimeError('Recovery output is not an ordinary file: ' + relative)
    try:
        with path.open('rb') as handle:
            opened = os.fstat(handle.fileno())
            content = handle.read()
            finished = os.fstat(handle.fileno())
        check_local_path(root, path)
        after = path.lstat()
    except FileNotFoundError as error:
        raise RuntimeError('Recovery output changed during observation: ' + relative) from error
    marker = file_marker(before)
    if (any(file_marker(item) != marker for item in (opened, finished, after))
            or len(content) != before.st_size):
        raise RuntimeError('Recovery output changed during observation: ' + relative)
    return RecoveryFileObservation(relative, marker, content)


def recovery_terminal_output(observed, generation, execution_id):
    try:
        output = json.loads(observed.content) if observed.content is not None else None
    except (ValueError, UnicodeError):
        return None
    if (not isinstance(output, dict)
            or type(output.get('generation')) is not int
            or output['generation'] != generation
            or output.get('execution_id') != execution_id
            or output.get('status') not in ('done', 'skipped', 'failed', 'cancelled')):
        return None
    return output
