from __future__ import annotations

import os
import shutil
import stat
from dataclasses import dataclass
from pathlib import Path

from micro_workflow_manager.file_helpers import _relative_file_parts
from micro_workflow_manager.project_format import is_link_or_reparse_point


@dataclass(frozen=True)
class InputFileChange:
    path: str
    kind: str
    content: str | bytes | Path | None = None
    overwrite: bool = False
    encoding: str = 'utf-8'
    missing_ok: bool = True


def checked_input_path(storage, receiver, relative):
    parts = _relative_file_parts(relative)
    root = storage.project_dir
    target = root / 'node' / receiver / 'input'
    for part in parts:
        target = target / part
    if not target.is_relative_to(root / 'node' / receiver / 'input'):
        raise ValueError('Unsafe managed input path')
    check_local_path(root, target)
    target = target.resolve()
    check_local_path(root, target)
    return target


def input_relative_path(storage, receiver, path, producer):
    # The raw-node prefix is an exact graph identity. Remaining path segments
    # use the filesystem's existing spelling, including Windows case aliases.
    parts = path.relative_to(storage.project_dir / 'node' / receiver / 'input').parts
    return '/'.join((producer, *parts[1:]))


def check_local_path(root, path):
    if not path.is_relative_to(root):
        raise ValueError(f'Unsafe managed input path: {path}')
    if any(is_link_or_reparse_point(parent) for parent in (path, *path.parents)
           if parent.is_relative_to(root)):
        raise ValueError(f'Unsafe managed input path: {path}')


class StagedInputFiles:
    """Own one locked batch's replacement files and copies of prior bytes."""

    def __init__(self, storage, producer, receiver, operation_id, changes):
        self.storage = storage
        self.receiver = receiver
        self.directory = storage.project_dir / '.mwf' / 'input-publications' / operation_id
        self.entries = []
        self.applied = []
        self.created_directories = []
        self.directory_owned = False
        seen = {}
        for change in changes:
            if not isinstance(change, InputFileChange):
                raise TypeError('Managed input changes must be InputFileChange values')
            relative = '/'.join((producer, *_relative_file_parts(change.path)))
            path = checked_input_path(storage, receiver, relative)
            if path in seen and (change.kind != 'copy' or seen[path] != 'copy'):
                raise ValueError(f'duplicate batch input filename: {relative}')
            seen[path] = change.kind
            if change.kind not in {'text', 'bytes', 'copy', 'append', 'delete'}:
                raise ValueError('Unknown managed input change')
            if change.kind in {'text', 'append'} and not isinstance(change.content, str):
                raise TypeError('Input content must be text')
            if change.kind == 'bytes' and not isinstance(change.content, bytes):
                raise TypeError('Input content must be bytes')
            if change.kind == 'copy':
                source = Path(change.content)
                if not source.is_file():
                    raise FileNotFoundError(f'Input source file does not exist: {source}')
            self.entries.append({'path': path, 'change': change})
        allocated = set()
        for entry in self.entries:
            path, change = entry['path'], entry['change']
            if (change.kind not in {'append', 'delete'} and not change.overwrite
                    and (path.exists() or path in allocated)):
                number = 2
                while True:
                    candidate = path.with_name(f'{path.stem}_{number}{path.suffix}')
                    if candidate not in seen and candidate not in allocated and not candidate.exists():
                        break
                    number += 1
                path = entry['path'] = candidate
            allocated.add(path)
            check_local_path(storage.project_dir, path)
            if path.exists() and not path.is_file():
                raise IsADirectoryError(str(path))
            if change.kind == 'delete' and not change.missing_ok and not path.exists():
                raise FileNotFoundError(str(path))
            entry['existed'] = path.exists()
            entry['original_identity'] = self._identity(path) if entry['existed'] else None
            entry['relative'] = input_relative_path(storage, receiver, path, producer)

    def stage(self):
        check_local_path(self.storage.project_dir, self.directory)
        self.directory.mkdir(parents=True, exist_ok=False)
        self.directory_owned = True
        for position, entry in enumerate(self.entries):
            path, change = entry['path'], entry['change']
            check_local_path(self.storage.project_dir, path)
            backup = entry['backup'] = self.directory / f'{position}.old'
            staged = entry['staged'] = self.directory / f'{position}.new'
            if entry['existed']:
                shutil.copy2(path, backup)
                self._flush(backup)
            if change.kind == 'delete':
                continue
            if change.kind == 'bytes':
                staged.write_bytes(change.content)
            elif change.kind == 'copy':
                shutil.copy2(Path(change.content), staged)
            elif change.kind == 'append':
                if entry['existed']:
                    shutil.copy2(backup, staged)
                with staged.open('a', encoding=change.encoding) as handle:
                    handle.write(change.content)
            else:
                staged.write_text(change.content, encoding=change.encoding)
            self._flush(staged)
            entry['size'] = staged.stat().st_size
            entry['staged_identity'] = self._identity(staged)

    @staticmethod
    def _flush(path):
        mode = stat.S_IMODE(path.stat().st_mode)
        try:
            # Windows requires a writable descriptor for fsync. Change only
            # this private copy, then restore the copied source permissions.
            path.chmod(mode | stat.S_IWRITE)
            with path.open('r+b') as handle:
                os.fsync(handle.fileno())
        finally:
            path.chmod(mode)

    @staticmethod
    def _identity(path):
        state = path.stat()
        return state.st_dev, state.st_ino, state.st_size, state.st_mtime_ns, state.st_mode

    def manifest(self):
        return [{'path': entry['relative'], 'kind': entry['change'].kind,
                 'existed': entry['existed']} for entry in self.entries]

    def publish(self):
        for entry in self.entries:
            path, change = entry['path'], entry['change']
            check_local_path(self.storage.project_dir, path)
            check_local_path(self.storage.project_dir, self.directory)
            if change.kind == 'delete':
                self.applied.append(entry)
                self.storage.retry_fs(lambda: path.unlink(missing_ok=change.missing_ok))
                continue
            missing = []
            parent = path.parent
            while not parent.exists():
                missing.append(parent)
                parent = parent.parent
            for parent in reversed(missing):
                parent.mkdir()
                self.created_directories.append(parent)
            self.applied.append(entry)
            self.storage.retry_fs(lambda: os.replace(entry['staged'], path))

    def restore(self):
        staged_identities = {}
        for entry in self.entries:
            if 'staged_identity' in entry:
                staged_identities.setdefault(entry['path'], set()).add(entry['staged_identity'])
        restored = set()
        for entry in reversed(self.applied):
            path = entry['path']
            if path in restored:
                continue
            restored.add(path)
            check_local_path(self.storage.project_dir, path)
            check_local_path(self.storage.project_dir, self.directory)
            current = self._identity(path) if path.exists() else None
            if current == entry['original_identity']:
                continue
            if current is not None:
                if current not in staged_identities.get(path, ()):
                    raise RuntimeError(f'Managed input changed during restoration: {path}')
                # A newly published copy may carry the source's read-only
                # mode. Only our exact replacement may be made writable.
                path.chmod(stat.S_IMODE(path.stat().st_mode) | stat.S_IWRITE)
            try:
                if entry['existed']:
                    self.storage.atomic_copy_file(entry['backup'], path)
                else:
                    self.storage.retry_fs(lambda: path.unlink(missing_ok=True))
            except BaseException as error:
                try:
                    check_local_path(self.storage.project_dir, path)
                    if current is not None and path.exists() and self._identity(path)[:4] == current[:4]:
                        path.chmod(stat.S_IMODE(current[4]))
                except BaseException as mode_error:
                    error.__notes__ = [*getattr(error, '__notes__', ()), f'Cannot restore input mode: {mode_error}']
                raise
        for path in reversed(self.created_directories):
            check_local_path(self.storage.project_dir, path)
            self.storage.retry_fs(path.rmdir)

    def discard(self):
        check_local_path(self.storage.project_dir, self.directory)
        if self.directory_owned and self.directory.exists():
            # Remove only artifacts allocated by this operation, without a
            # recursive traversal through unexpected entries or links.
            for position in range(len(self.entries)):
                for suffix in ('old', 'new'):
                    path = self.directory / f'{position}.{suffix}'
                    check_local_path(self.storage.project_dir, path)
                    if path.exists():
                        path.chmod(stat.S_IMODE(path.stat().st_mode) | stat.S_IWRITE)
                    path.unlink(missing_ok=True)
            self.directory.rmdir()
