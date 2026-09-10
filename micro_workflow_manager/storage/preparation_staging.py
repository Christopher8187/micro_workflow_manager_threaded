"""Persist and restore complete component-preparation file identities."""

from __future__ import annotations

import stat

from .recovery_errors import add_recovery_note
from .recovery_output import recovery_path
from .recovery_moves import move_without_replacement
from .recovery_tree import capture_recovery_tree, directory_marker, known_remaining_tree, validate_tree_record


class PreparationStaging:
    def __init__(self, root, manifest):
        self.root, self.manifest = root, manifest
        self.relative = manifest['staging']['path']
        self.directory = recovery_path(root, self.relative)
        self.paths = manifest['paths']
        self.replacements = manifest['replacements']
        self._validate_manifest()

    def _validate_manifest(self):
        if not self.relative.startswith('.mwf/preparation-trash/') or len(self.relative.split('/')) != 3:
            raise RuntimeError('Invalid preparation staging path')
        import re
        if not re.fullmatch(r'[0-9a-f]{32}', self.relative.split('/')[-1]):
            raise RuntimeError('Invalid preparation operation identity')
        marker = self.manifest['staging']['marker']
        if (not isinstance(marker, list) or len(marker) != 3
                or any(type(value) is not int for value in marker) or not stat.S_ISDIR(marker[2])):
            raise RuntimeError('Invalid preparation staging directory identity')
        if (len({item['path'] for item in self.replacements}) != len(self.replacements)
                or self.manifest['directories'] != [item['path'] for item in self.replacements]):
            raise RuntimeError('Invalid preparation replacement directories')
        names, paths = set(), set()
        for item in self.paths:
            path, saved = item['path'], item['saved']
            recovery_path(self.root, path)
            if not path.startswith('node/') or path in paths or not re.fullmatch(r'[0-9]+', saved):
                raise RuntimeError('Invalid preparation original path')
            if type(item['existed']) is not bool or item['existed'] != (item['original'] is not None):
                raise RuntimeError('Invalid preparation original existence')
            validate_tree_record(item['original'])
            names.add(saved)
            paths.add(path)
        for item in self.replacements:
            if item['path'] not in paths or not re.fullmatch(r'created_[0-9]+', item['saved']):
                raise RuntimeError('Invalid preparation replacement path')
            validate_tree_record(item['tree'])
            if item['tree']['kind'] != 'directory' or item['tree']['children']:
                raise RuntimeError('Preparation replacement must be an empty recorded directory')
            if item['saved'] in names:
                raise RuntimeError('Duplicate preparation staging name')
            names.add(item['saved'])
        if len({item['saved'] for item in self.paths}) != len(self.paths):
            raise RuntimeError('Duplicate preparation original name')
        for first in paths:
            if any(first.startswith(other + '/') for other in paths if first != other):
                raise RuntimeError('Overlapping preparation original paths')

    def _saved(self, item):
        return capture_recovery_tree(self.root, self.relative + '/' + item['saved'])

    def _known_private(self):
        known = {item['saved']: item['original'] for item in self.paths}
        known.update({item['saved']: item['tree'] for item in self.replacements})
        return known

    def require_private(self, *, cleanup=False):
        status = recovery_path(self.root, self.relative).lstat()
        if directory_marker(status) != self.manifest['staging']['marker']:
            raise RuntimeError('Preparation staging directory identity changed')
        known = self._known_private()
        if {path.name for path in self.directory.iterdir()} - known.keys():
            raise RuntimeError('Preparation staging contains unexpected private entries')
        for name, expected in known.items():
            actual = capture_recovery_tree(self.root, self.relative + '/' + name)
            if actual is not None and not (known_remaining_tree(actual, expected) if cleanup else actual == expected):
                raise RuntimeError('Preparation saved material changed: ' + name)

    def stage(self):
        self.require_private()
        for item in self.paths:
            visible = capture_recovery_tree(self.root, item['path'])
            if visible != item['original'] or self._saved(item) is not None:
                raise RuntimeError('Preparation original changed before staging: ' + item['path'])
        for item in self.paths:
            if item['existed']:
                if capture_recovery_tree(self.root, item['path']) != item['original']:
                    raise RuntimeError('Preparation original changed before move')
                move_without_replacement(recovery_path(self.root, item['path']), self.directory / item['saved'])
        for item in self.replacements:
            if capture_recovery_tree(self.root, item['path']) is not None:
                raise RuntimeError('Preparation output replacement appeared before publication')
            move_without_replacement(self.directory / item['saved'], recovery_path(self.root, item['path']))

    def restoration_steps(self):
        self.require_private()
        replacements = {item['path']: item for item in self.replacements}
        steps = []
        for item in self.paths:
            visible = capture_recovery_tree(self.root, item['path'])
            saved = self._saved(item)
            if visible == item['original'] and saved is None:
                continue
            replacement = replacements.get(item['path'])
            if visible is not None:
                if replacement is None or visible != replacement['tree'] or self._saved(replacement) is not None:
                    raise RuntimeError('Preparation restoration target changed: ' + item['path'])
            if saved != item['original']:
                raise RuntimeError('Preparation original is missing or changed: ' + item['path'])
            steps.append((item, visible is not None))
        return steps

    def _detach_replacement(self, replacement, target):
        saved = self.directory / replacement['saved']
        move_without_replacement(target, saved)
        try:
            if self._saved(replacement) != replacement['tree']:
                raise RuntimeError('Preparation replacement changed before removal: ' + replacement['path'])
        except BaseException as error:
            try:
                move_without_replacement(saved, target)
            except BaseException as return_error:
                add_recovery_note(error, 'Changed preparation replacement could not be returned: ' + str(return_error))
            raise

    def _detach_replacements(self, steps):
        detached = []
        try:
            for item, remove_replacement in reversed(steps):
                if not remove_replacement:
                    continue
                replacement = next(value for value in self.replacements if value['path'] == item['path'])
                if capture_recovery_tree(self.root, item['path']) != replacement['tree']:
                    raise RuntimeError('Preparation replacement changed before removal')
                target = recovery_path(self.root, item['path'])
                self._detach_replacement(replacement, target)
                detached.append((replacement, target))
        except BaseException as error:
            for replacement, target in reversed(detached):
                try:
                    if self._saved(replacement) != replacement['tree']:
                        raise RuntimeError('Detached preparation replacement changed')
                    move_without_replacement(self.directory / replacement['saved'], target)
                except BaseException as return_error:
                    add_recovery_note(error, 'Preparation replacement remains private: ' + str(return_error))
            raise

    def restore(self):
        steps = self.restoration_steps()
        self._detach_replacements(steps)
        for item, _ in reversed(steps):
            target = recovery_path(self.root, item['path'])
            if item['original'] is not None:
                if self._saved(item) != item['original']:
                    raise RuntimeError('Preparation original changed before restoration')
                move_without_replacement(self.directory / item['saved'], target)
        self.require_restored()

    def require_restored(self):
        for item in self.paths:
            if capture_recovery_tree(self.root, item['path']) != item['original']:
                raise RuntimeError('Preparation restoration did not preserve the original: ' + item['path'])

    def _remove_recorded_tree(self, relative, expected):
        actual = capture_recovery_tree(self.root, relative)
        if actual is None:
            return
        if not known_remaining_tree(actual, expected):
            raise RuntimeError('Preparation saved material changed before cleanup: ' + relative)
        path = recovery_path(self.root, relative)
        if actual['kind'] == 'file':
            path.chmod(stat.S_IMODE(path.stat().st_mode) | stat.S_IWRITE)
            actual = capture_recovery_tree(self.root, relative)
            if not known_remaining_tree(actual, expected):
                raise RuntimeError('Preparation saved file changed before cleanup: ' + relative)
            path.unlink()
            return
        for name in sorted(actual['children']):
            self._remove_recorded_tree(relative + '/' + name, expected['children'][name])
        actual = capture_recovery_tree(self.root, relative)
        if actual is None:
            return
        if not known_remaining_tree(actual, expected) or actual['children']:
            raise RuntimeError('Preparation saved directory changed before cleanup: ' + relative)
        path.rmdir()

    def discard(self):
        self.require_private(cleanup=True)
        for path in self.directory.rglob('*'):
            recovery_path(self.root, path.relative_to(self.root).as_posix())
        self.require_private(cleanup=True)
        for name, expected in self._known_private().items():
            self._remove_recorded_tree(self.relative + '/' + name, expected)
        self.require_private(cleanup=True)
        self.directory.rmdir()


def _record_created_directory(path, created):
    path.mkdir(exist_ok=False)
    created.append((path, None))
    marker = directory_marker(path.lstat())
    created[-1] = (path, marker)
    return marker


def _clean_failed_allocation(created, error):
    for path, marker in reversed(created):
        try:
            if marker is None or directory_marker(path.lstat()) != marker:
                raise RuntimeError('created directory identity changed')
            path.rmdir()
        except FileNotFoundError:
            continue
        except BaseException as cleanup_error:
            add_recovery_note(error, 'Partial preparation staging remains at ' + str(path) + ': ' + str(cleanup_error))


def allocate_preparation_staging(root, operation_id, paths, directories, receivers):
    relative = '.mwf/preparation-trash/' + operation_id
    directory = recovery_path(root, relative)
    directory.parent.mkdir(parents=True, exist_ok=True)
    created = []
    try:
        marker = _record_created_directory(directory, created)
        manifest = {'receivers': sorted(receivers),
                    'staging': {'path': relative, 'marker': marker},
                    'paths': [], 'directories': [], 'replacements': []}
        for position, path in enumerate(paths):
            name = path.relative_to(root).as_posix()
            original = capture_recovery_tree(root, name)
            manifest['paths'].append(dict(path=name, saved=str(position), existed=original is not None,
                                          original=original))
        for position, path in enumerate(sorted(directories)):
            saved = 'created_' + str(position)
            _record_created_directory(directory / saved, created)
            name = path.relative_to(root).as_posix()
            manifest['directories'].append(name)
            manifest['replacements'].append(dict(path=name, saved=saved,
                tree=capture_recovery_tree(root, relative + '/' + saved)))
        return PreparationStaging(root, manifest)
    except BaseException as error:
        _clean_failed_allocation(created, error)
        raise
