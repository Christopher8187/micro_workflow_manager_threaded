"""Validate and finish interrupted managed-input filesystem changes."""

from __future__ import annotations

import re
import stat

from .input_publication_files import check_local_path
from .recovery_errors import add_recovery_note
from .recovery_output import recovery_path
from .recovery_tree import (
    capture_recovery_tree,
    directory_marker,
    known_remaining_tree,
    same_original_content,
    validate_tree_record,
    writable_file_mode,
)


_OPERATION = re.compile(r'[0-9a-f]{32}')
_OWNER_FIELDS = {'owner_pid', 'process_identity', 'hostname', 'heartbeat_at'}
_ENTRY_FIELDS = {'position', 'path', 'relative', 'kind', 'original', 'backup', 'staged'}
_PRIVATE_FIELDS = {'saved', 'tree'}
_DIRECTORY_FIELDS = {'path', 'saved', 'tree'}


def _text(value, label):
    if not isinstance(value, str) or not value or value != value.strip():
        raise RuntimeError('Invalid managed input ' + label)
    return value


def _private(value, *, required):
    if value is None:
        if required:
            raise RuntimeError('Missing managed input private file record')
        return
    if not isinstance(value, dict) or set(value) != _PRIVATE_FIELDS:
        raise RuntimeError('Invalid managed input private file record')
    if not re.fullmatch(r'[0-9]+[.](?:old|new)', _text(value['saved'], 'private filename')):
        raise RuntimeError('Invalid managed input private filename')
    validate_tree_record(value['tree'])
    if value['tree'] is None or value['tree']['kind'] != 'file':
        raise RuntimeError('Managed input private material must be a file')


class InputCleanupFiles:
    """Strict persisted view of one managed-input filesystem operation."""

    def __init__(self, storage, receiver, operation_id, manifest):
        self.storage = storage
        self.root = storage.project_dir
        self.receiver = receiver
        self.operation_id = operation_id
        self.manifest = manifest
        self._validate_manifest()
        self.relative = manifest['staging']['path']
        self.directory = recovery_path(self.root, self.relative)

    def _validate_manifest(self):
        if not _OPERATION.fullmatch(self.operation_id):
            raise RuntimeError('Invalid managed input operation identity')
        fields = {'version', 'operation_id', 'producer', 'receiver', 'owner',
                  'staging', 'directories', 'entries'}
        if not isinstance(self.manifest, dict) or set(self.manifest) != fields:
            raise RuntimeError('Invalid managed input cleanup manifest')
        if (type(self.manifest['version']) is not int or self.manifest['version'] != 1
                or self.manifest['operation_id'] != self.operation_id
                or self.manifest['receiver'] != self.receiver):
            raise RuntimeError('Managed input cleanup manifest identity differs from its receipt')
        producer = _text(self.manifest['producer'], 'producer')
        if '/' in producer or '\\' in producer or '..' in producer:
            raise RuntimeError('Invalid managed input producer')
        owner = self.manifest['owner']
        if not isinstance(owner, dict) or set(owner) != _OWNER_FIELDS:
            raise RuntimeError('Invalid managed input operation owner')
        if type(owner['owner_pid']) is not int or owner['owner_pid'] <= 0:
            raise RuntimeError('Invalid managed input operation process')
        for name in ('process_identity', 'hostname', 'heartbeat_at'):
            _text(owner[name], 'operation ' + name)
        staging = self.manifest['staging']
        if not isinstance(staging, dict) or set(staging) != {'path', 'marker'}:
            raise RuntimeError('Invalid managed input staging record')
        if staging['path'] != '.mwf/input-publications/' + self.operation_id:
            raise RuntimeError('Invalid managed input staging path')
        marker = staging['marker']
        if (not isinstance(marker, list) or len(marker) != 3
                or any(type(item) is not int for item in marker) or not stat.S_ISDIR(marker[2])):
            raise RuntimeError('Invalid managed input staging identity')
        self._validate_entries()
        self._validate_directories()

    def _validate_entries(self):
        entries = self.manifest['entries']
        if not isinstance(entries, list) or not entries:
            raise RuntimeError('Managed input cleanup requires file entries')
        private, originals = set(), {}
        prefix = f'node/{self.receiver}/input/'
        producer_prefix = self.manifest['producer'] + '/'
        for position, entry in enumerate(entries):
            if not isinstance(entry, dict) or set(entry) != _ENTRY_FIELDS:
                raise RuntimeError('Invalid managed input cleanup entry')
            if type(entry['position']) is not int or entry['position'] != position:
                raise RuntimeError('Invalid managed input cleanup position')
            path = _text(entry['path'], 'path')
            relative = _text(entry['relative'], 'receiver path')
            physical = recovery_path(self.root, path)
            declared = recovery_path(self.root, prefix + relative)
            if physical != declared or not relative.startswith(producer_prefix):
                raise RuntimeError('Managed input cleanup path differs from its receiver path')
            if entry['kind'] not in {'text', 'bytes', 'copy', 'append', 'delete'}:
                raise RuntimeError('Invalid managed input cleanup kind')
            validate_tree_record(entry['original'])
            if entry['original'] is not None and entry['original']['kind'] != 'file':
                raise RuntimeError('Managed input original must be an ordinary file')
            _private(entry['backup'], required=entry['original'] is not None)
            _private(entry['staged'], required=entry['kind'] != 'delete')
            if (entry['backup'] is not None
                    and entry['backup']['saved'] != f'{position}.old'):
                raise RuntimeError('Managed input backup position is inconsistent')
            if (entry['staged'] is not None
                    and entry['staged']['saved'] != f'{position}.new'):
                raise RuntimeError('Managed input staged position is inconsistent')
            if (entry['backup'] is not None
                    and not same_original_content(entry['backup']['tree'], entry['original'])):
                raise RuntimeError('Managed input backup differs from its original')
            for item in (entry['backup'], entry['staged']):
                if item is not None:
                    if item['saved'] in private:
                        raise RuntimeError('Duplicate managed input private filename')
                    private.add(item['saved'])
            prior = originals.setdefault(path, entry['original'])
            if prior != entry['original']:
                raise RuntimeError('Duplicate managed input path has inconsistent original identity')
        for members in self._by_path().values():
            if len(members) > 1 and any(item['kind'] != 'copy' for item in members):
                raise RuntimeError('Only copy changes may repeat a managed input path')

    def _validate_directories(self):
        directories = self.manifest['directories']
        if not isinstance(directories, list):
            raise RuntimeError('Invalid managed input replacement directories')
        paths = set()
        prefix = f'node/{self.receiver}/input/'
        for position, item in enumerate(directories):
            if not isinstance(item, dict) or set(item) != _DIRECTORY_FIELDS:
                raise RuntimeError('Invalid managed input replacement directory')
            if (not item['path'].startswith(prefix)
                    or item['saved'] != 'created_' + str(position)):
                raise RuntimeError('Invalid managed input replacement directory identity')
            recovery_path(self.root, item['path'])
            validate_tree_record(item['tree'])
            if item['tree'] is None or item['tree']['kind'] != 'directory' or item['tree']['children']:
                raise RuntimeError('Managed input replacement directory must be recorded empty')
            if item['path'] in paths:
                raise RuntimeError('Duplicate managed input replacement directory')
            paths.add(item['path'])
        depths = [item['path'].count('/') for item in directories]
        if depths != sorted(depths):
            raise RuntimeError('Managed input replacement directories are out of order')
        targets = [entry['path'] for entry in self.manifest['entries']]
        if any(not any(path.startswith(item['path'] + '/') for path in targets) for item in directories):
            raise RuntimeError('Managed input replacement directory has no managed target')

    def _by_path(self):
        result = {}
        for entry in self.manifest['entries']:
            result.setdefault(entry['path'], []).append(entry)
        return result

    def _capture(self, relative):
        return capture_recovery_tree(self.root, relative)

    def _known_private(self, *, cleanup):
        try:
            state = self.directory.lstat()
        except FileNotFoundError:
            return False
        check_local_path(self.root, self.directory)
        if directory_marker(state) != self.manifest['staging']['marker']:
            raise RuntimeError('Managed input staging directory identity changed')
        known = {}
        for entry in self.manifest['entries']:
            for item in (entry['backup'], entry['staged']):
                if item is not None:
                    known[item['saved']] = item['tree']
        for item in self.manifest['directories']:
            known[item['saved']] = item['tree']
        if {item.name for item in self.directory.iterdir()} - known.keys():
            raise RuntimeError('Managed input staging contains unexpected private entries')
        for name, expected in known.items():
            actual = self._capture(self.relative + '/' + name)
            if actual is not None and not (known_remaining_tree(actual, expected) if cleanup else actual == expected):
                raise RuntimeError('Managed input private material changed: ' + name)
        return True

    def _require_backups(self):
        for entry in self.manifest['entries']:
            item = entry['backup']
            if item is not None and self._capture(self.relative + '/' + item['saved']) != item['tree']:
                raise RuntimeError('Managed input backup is missing or changed: ' + entry['path'])

    def _prefixes(self):
        possible = []
        entries = self.manifest['entries']
        for prefix in range(len(entries) + 1):
            matches = True
            for position, entry in enumerate(entries):
                item = entry['staged']
                if item is None:
                    continue
                present = self._capture(self.relative + '/' + item['saved']) is not None
                if present != (position >= prefix):
                    matches = False
                    break
            if matches:
                possible.append(prefix)
        return possible

    @staticmethod
    def _restored(actual, original):
        return actual is None if original is None else same_original_content(actual, original)

    @staticmethod
    def _recoverable(actual, expected):
        if expected is None:
            return actual is None
        return actual == expected or known_remaining_tree(actual, expected)

    def _original_or_recoverable(self, actual, original):
        return self._restored(actual, original) or self._recoverable(actual, original)

    def _forward(self, path, prefix):
        entries = self._by_path()[path]
        applied = [item for item in entries if item['position'] < prefix]
        if not applied:
            return entries[0]['original']
        last = applied[-1]
        return None if last['kind'] == 'delete' else last['staged']['tree']

    def _require_directories(self):
        allowed = {item['path']: set() for item in self.manifest['directories']}
        for item in self.manifest['directories']:
            parent = item['path'].rsplit('/', 1)[0]
            if parent in allowed:
                allowed[parent].add(item['path'].rsplit('/', 1)[1])
        for entry in self.manifest['entries']:
            parent = entry['path'].rsplit('/', 1)[0]
            if parent in allowed:
                allowed[parent].add(entry['path'].rsplit('/', 1)[1])
        for item in self.manifest['directories']:
            private = self._capture(self.relative + '/' + item['saved'])
            path = recovery_path(self.root, item['path'])
            try:
                state = path.lstat()
            except FileNotFoundError:
                visible = False
            else:
                check_local_path(self.root, path)
                visible = True
                if directory_marker(state) != item['tree']['marker']:
                    raise RuntimeError('Managed input replacement directory changed: ' + item['path'])
                if {child.name for child in path.iterdir()} - allowed[item['path']]:
                    raise RuntimeError('Managed input replacement directory contains unexpected material')
            if private is not None and visible:
                raise RuntimeError('Managed input replacement directory exists twice')

    def preflight_restore(self):
        if not self._known_private(cleanup=True):
            raise RuntimeError('Managed input staging directory is missing')
        self._require_backups()
        self._require_directories()
        prefixes = self._prefixes()
        if not prefixes:
            raise RuntimeError('Managed input staged publication order is damaged')
        actual = {path: self._capture(path) for path in self._by_path()}
        valid = any(all(self._recoverable(actual[path], self._forward(path, prefix))
                            or self._original_or_recoverable(actual[path], entries[0]['original'])
                        for path, entries in self._by_path().items())
                    for prefix in prefixes)
        if not valid:
            raise RuntimeError('Managed input visible material changed before restoration')

    def _restore_visible_mode(self, path, target, actual, writable, error):
        try:
            if self._capture(path) != writable:
                raise RuntimeError('Managed input changed after failed restoration: ' + path)
            target.chmod(stat.S_IMODE(actual['marker'][2]))
            if self._capture(path) != actual:
                raise RuntimeError('Managed input mode was not restored: ' + path)
        except BaseException as mode_error:
            add_recovery_note(error, 'Managed input visible mode could not be restored: ' + str(mode_error))

    def restore(self):
        self.preflight_restore()
        if not hasattr(self.storage, 'atomic_copy_file') or not hasattr(self.storage, 'retry_fs'):
            raise RuntimeError('Managed input restoration requires live storage')
        for path, entries in reversed(tuple(self._by_path().items())):
            original = entries[0]['original']
            actual = self._capture(path)
            target = recovery_path(self.root, path)
            if self._original_or_recoverable(actual, original):
                if actual is not None and actual['marker'][2] != original['marker'][2]:
                    target.chmod(stat.S_IMODE(original['marker'][2]))
                continue
            replacements = [item['staged']['tree'] for item in entries if item['staged'] is not None]
            if actual is not None and not any(known_remaining_tree(actual, item) for item in replacements):
                raise RuntimeError('Managed input changed during restoration: ' + path)
            writable = None
            mode_changed = False
            try:
                if actual is not None:
                    writable = dict(actual, marker=list(actual['marker']))
                    writable['marker'][2] = writable_file_mode(actual['marker'][2])
                    mode_changed = True
                    target.chmod(stat.S_IMODE(writable['marker'][2]))
                    if self._capture(path) != writable:
                        raise RuntimeError('Managed input changed while making it writable: ' + path)
                if original is None:
                    self.storage.retry_fs(lambda target=target: target.unlink(missing_ok=True))
                else:
                    backup = entries[0]['backup']
                    if self._capture(self.relative + '/' + backup['saved']) != backup['tree']:
                        raise RuntimeError('Managed input backup changed before restoration: ' + path)
                    self.storage.atomic_copy_file(self.directory / backup['saved'], target)
            except BaseException as error:
                if mode_changed:
                    self._restore_visible_mode(path, target, actual, writable, error)
                raise
        for item in reversed(self.manifest['directories']):
            path = recovery_path(self.root, item['path'])
            try:
                state = path.lstat()
            except FileNotFoundError:
                continue
            if directory_marker(state) != item['tree']['marker'] or any(path.iterdir()):
                raise RuntimeError('Managed input replacement directory is not empty: ' + item['path'])
            self.storage.retry_fs(path.rmdir)
        self.require_restored()

    def require_restored(self):
        for path, entries in self._by_path().items():
            if not self._restored(self._capture(path), entries[0]['original']):
                raise RuntimeError('Managed input restoration did not preserve the original: ' + path)
        for item in self.manifest['directories']:
            if self._capture(item['path']) is not None:
                raise RuntimeError('Managed input restoration retained a replacement directory: ' + item['path'])

    def require_published(self):
        if not self._known_private(cleanup=True):
            raise RuntimeError('Managed input staging directory is missing')
        self._require_directories()
        for item in self.manifest['directories']:
            if self._capture(self.relative + '/' + item['saved']) is not None:
                raise RuntimeError('Managed input replacement directory was not published: ' + item['path'])
        for entry in self.manifest['entries']:
            item = entry['staged']
            if item is not None and self._capture(self.relative + '/' + item['saved']) is not None:
                raise RuntimeError('Managed input staged file was not published: ' + entry['path'])
        for path in self._by_path():
            if self._capture(path) != self._forward(path, len(self.manifest['entries'])):
                raise RuntimeError('Managed input publication is incomplete: ' + path)

    def preflight_discard(self):
        self._known_private(cleanup=True)

    def discard(self):
        self.preflight_discard()
        if not hasattr(self.storage, 'retry_fs'):
            raise RuntimeError('Managed input cleanup requires live storage')
        if not self.directory.exists():
            return
        for entry in self.manifest['entries']:
            for item in (entry['backup'], entry['staged']):
                if item is None:
                    continue
                path = self.directory / item['saved']
                if path.exists():
                    actual = self._capture(self.relative + '/' + item['saved'])
                    if not known_remaining_tree(actual, item['tree']):
                        raise RuntimeError('Managed input private file changed before cleanup')
                    path.chmod(stat.S_IMODE(path.stat().st_mode) | stat.S_IWRITE)
                    self.storage.retry_fs(lambda path=path: path.unlink(missing_ok=True))
        for item in reversed(self.manifest['directories']):
            path = self.directory / item['saved']
            if path.exists():
                actual = self._capture(self.relative + '/' + item['saved'])
                if actual != item['tree'] or any(path.iterdir()):
                    raise RuntimeError('Managed input private directory is not empty')
                self.storage.retry_fs(path.rmdir)
        self.storage.retry_fs(self.directory.rmdir)


def load_input_cleanup_files(storage, receiver, operation_id, manifest):
    return InputCleanupFiles(storage, receiver, operation_id, manifest)
