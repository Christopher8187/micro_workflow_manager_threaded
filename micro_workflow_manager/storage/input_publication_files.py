from __future__ import annotations

from .recovery_errors import add_recovery_note

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
    parts = path.relative_to(storage.project_dir / 'node' / receiver / 'input').parts
    return '/'.join((producer, *parts[1:]))


def check_local_path(root, path):
    if not path.is_relative_to(root):
        raise ValueError(f'Unsafe managed input path: {path}')
    if any(is_link_or_reparse_point(parent) for parent in (path, *path.parents)
           if parent.is_relative_to(root)):
        raise ValueError(f'Unsafe managed input path: {path}')


def _capture(root, relative):
    from .recovery_tree import capture_recovery_tree
    return capture_recovery_tree(root, relative)


def _marker(path):
    from .recovery_tree import directory_marker
    return directory_marker(path.lstat())


def _move(source, destination):
    from .recovery_moves import move_without_replacement
    move_without_replacement(source, destination)


class StagedInputFiles:
    """Own one locked batch and its persisted recovery manifest."""

    def __init__(self, storage, producer, receiver, operation_id, changes, owner):
        self.storage = storage
        self.producer = producer
        self.receiver = receiver
        self.operation_id = operation_id
        self.owner = dict(owner)
        self.root = storage.project_dir
        self.relative = '.mwf/input-publications/' + operation_id
        self.directory = self.root / '.mwf' / 'input-publications' / operation_id
        self.entries = []
        self.directories = []
        self.manifest = None
        self._allocated = []
        self._directory_identity = None
        seen, allocated, requested = {}, set(), []
        for change in changes:
            self._validate_change(change)
            relative = '/'.join((producer, *_relative_file_parts(change.path)))
            path = checked_input_path(storage, receiver, relative)
            if path in seen and (change.kind != 'copy' or seen[path] != 'copy'):
                raise ValueError(f'duplicate batch input filename: {relative}')
            seen[path] = change.kind
            requested.append((change, path))
        for change, path in requested:
            if (change.kind not in {'append', 'delete'} and not change.overwrite
                    and (path.exists() or path in allocated)):
                path = self._unused_path(path, seen, allocated)
            allocated.add(path)
            check_local_path(self.root, path)
            if path.exists() and not path.is_file():
                raise IsADirectoryError(str(path))
            if change.kind == 'delete' and not change.missing_ok and not path.exists():
                raise FileNotFoundError(str(path))
            project_path = path.relative_to(self.root).as_posix()
            original = _capture(self.root, project_path)
            self.entries.append({
                'path': path,
                'project_path': project_path,
                'relative': input_relative_path(storage, receiver, path, producer),
                'change': change,
                'original': original,
                'existed': original is not None,
            })
        originals = {}
        for entry in self.entries:
            prior = originals.setdefault(entry['path'], entry['original'])
            if prior != entry['original']:
                raise RuntimeError('Managed input path changed while planning publication')
        self._plan_directories()

    @staticmethod
    def _validate_change(change):
        if not isinstance(change, InputFileChange):
            raise TypeError('Managed input changes must be InputFileChange values')
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

    @staticmethod
    def _unused_path(path, seen, allocated):
        number = 2
        while True:
            candidate = path.with_name(f'{path.stem}_{number}{path.suffix}')
            if candidate not in seen and candidate not in allocated and not candidate.exists():
                return candidate
            number += 1

    def _plan_directories(self):
        input_root = self.root / 'node' / self.receiver / 'input'
        missing = set()
        for entry in self.entries:
            if entry['change'].kind == 'delete':
                continue
            parent = entry['path'].parent
            while not parent.exists():
                if not parent.is_relative_to(input_root):
                    raise RuntimeError('Managed input parent is outside receiver input storage')
                missing.add(parent)
                parent = parent.parent
        self.directories = sorted(missing, key=lambda value: (len(value.parts), value.as_posix()))

    @staticmethod
    def _flush(path):
        mode = stat.S_IMODE(path.stat().st_mode)
        try:
            path.chmod(mode | stat.S_IWRITE)
            with path.open('r+b') as handle:
                os.fsync(handle.fileno())
        finally:
            path.chmod(mode)

    def stage(self):
        check_local_path(self.root, self.directory)
        self.directory.parent.mkdir(exist_ok=True)
        self.directory.mkdir(exist_ok=False)
        self._directory_identity = _marker(self.directory)
        manifest = {
            'version': 1,
            'operation_id': self.operation_id,
            'producer': self.producer,
            'receiver': self.receiver,
            'owner': self.owner,
            'staging': {'path': self.relative, 'marker': _marker(self.directory)},
            'directories': [],
            'entries': [],
        }
        try:
            for position, entry in enumerate(self.entries):
                backup = None
                if entry['original'] is not None:
                    saved = f'{position}.old'
                    path = self.directory / saved
                    shutil.copy2(entry['path'], path)
                    self._flush(path)
                    tree = _capture(self.root, self.relative + '/' + saved)
                    self._allocated.append((path, tree))
                    backup = {'saved': saved, 'tree': tree}
                staged = None
                if entry['change'].kind != 'delete':
                    saved = f'{position}.new'
                    path = self.directory / saved
                    self._write_staged(entry, backup, path)
                    self._flush(path)
                    tree = _capture(self.root, self.relative + '/' + saved)
                    self._allocated.append((path, tree))
                    staged = {'saved': saved, 'tree': tree}
                    entry['size'] = path.stat().st_size
                manifest['entries'].append({
                    'position': position,
                    'path': entry['project_path'],
                    'relative': entry['relative'],
                    'kind': entry['change'].kind,
                    'original': entry['original'],
                    'backup': backup,
                    'staged': staged,
                })
            for position, visible in enumerate(self.directories):
                saved = 'created_' + str(position)
                path = self.directory / saved
                path.mkdir()
                tree = _capture(self.root, self.relative + '/' + saved)
                self._allocated.append((path, tree))
                manifest['directories'].append({
                    'path': visible.relative_to(self.root).as_posix(),
                    'saved': saved,
                    'tree': tree,
                })
        except BaseException as error:
            try:
                self.discard_incomplete()
            except BaseException as cleanup_error:
                add_recovery_note(error, 'Incomplete managed input staging was retained: ' + str(cleanup_error))
            raise
        self.manifest = manifest
        self.cleanup_files().preflight_restore()

    def _write_staged(self, entry, backup, path):
        change = entry['change']
        if change.kind == 'bytes':
            path.write_bytes(change.content)
        elif change.kind == 'copy':
            shutil.copy2(Path(change.content), path)
        elif change.kind == 'append':
            if backup is not None:
                shutil.copy2(self.directory / backup['saved'], path)
            mode = stat.S_IMODE(path.stat().st_mode) if path.exists() else None
            try:
                if mode is not None:
                    path.chmod(mode | stat.S_IWRITE)
                with path.open('a', encoding=change.encoding) as handle:
                    handle.write(change.content)
            finally:
                if mode is not None and path.exists():
                    path.chmod(mode)
        else:
            path.write_text(change.content, encoding=change.encoding)

    def _expected_before(self, position):
        entry = self.entries[position]
        prior = [prior for prior in range(position)
                 if self.entries[prior]['path'] == entry['path']]
        if not prior:
            return entry['original']
        previous = prior[-1]
        if self.entries[previous]['change'].kind == 'delete':
            return None
        return self.manifest['entries'][previous]['staged']['tree']

    @staticmethod
    def _make_writable(path, observed):
        path.chmod(stat.S_IMODE(observed['marker'][2]) | stat.S_IWRITE)

    def publish(self):
        if self.manifest is None:
            raise RuntimeError('Managed input publication was not staged')
        self.cleanup_files().preflight_restore()
        for item in self.manifest['directories']:
            private = self.directory / item['saved']
            visible = self.root.joinpath(*item['path'].split('/'))
            if _capture(self.root, item['path']) is not None:
                raise RuntimeError('Managed input directory appeared before publication: ' + item['path'])
            if _capture(self.root, self.relative + '/' + item['saved']) != item['tree']:
                raise RuntimeError('Managed input private directory changed before publication')
            _move(private, visible)
        for position, entry in enumerate(self.entries):
            expected = self._expected_before(position)
            actual = _capture(self.root, entry['project_path'])
            if actual != expected:
                raise RuntimeError('Managed input changed before publication: ' + entry['project_path'])
            path = entry['path']
            if actual is not None:
                self._make_writable(path, actual)
            if entry['change'].kind == 'delete':
                if actual is not None:
                    self.storage.retry_fs(path.unlink)
            else:
                staged = self.directory / self.manifest['entries'][position]['staged']['saved']
                if actual is None:
                    self.storage.retry_fs(lambda staged=staged, path=path: _move(staged, path))
                else:
                    self.storage.retry_fs(lambda staged=staged, path=path: os.replace(staged, path))
        self.cleanup_files().require_published()

    def cleanup_files(self):
        if self.manifest is None:
            raise RuntimeError('Managed input publication has no complete recovery manifest')
        from .input_cleanup_files import load_input_cleanup_files
        return load_input_cleanup_files(
            self.storage, self.receiver, self.operation_id, self.manifest,
        )

    def restore(self):
        self.cleanup_files().restore()

    def require_restored(self):
        self.cleanup_files().require_restored()

    def discard(self):
        self.cleanup_files().discard()

    def discard_incomplete(self):
        for path, expected in reversed(self._allocated):
            relative = path.relative_to(self.root).as_posix()
            actual = _capture(self.root, relative)
            if actual is None:
                continue
            if actual != expected:
                raise RuntimeError('Incomplete managed input private material changed: ' + path.name)
            if actual['kind'] == 'file':
                path.chmod(stat.S_IMODE(path.stat().st_mode) | stat.S_IWRITE)
                path.unlink()
            elif not any(path.iterdir()):
                path.rmdir()
        if self.directory.exists():
            if _marker(self.directory) != self._directory_identity or any(self.directory.iterdir()):
                raise RuntimeError('Incomplete managed input staging directory requires recovery')
            self.directory.rmdir()
