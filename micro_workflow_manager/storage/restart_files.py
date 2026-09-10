"""Stage and recover exact manual-restart outputs beside their jobs."""

from __future__ import annotations

import os
import re
from .recovery_errors import add_recovery_note
from .recovery_output import recovery_path
from .recovery_tree import (
    capture_recovery_tree,
    known_remaining_tree,
    validate_tree_record,
    writable_file_mode,
)


def _move(source, destination):
    from .execution_restart import _move_restart_output

    return _move_restart_output(source, destination)


def _discard(path):
    from .execution_restart import _discard_restart_output

    return _discard_restart_output(path)


class RestartFiles:
    """Own the canonical and private output paths in one restart receipt."""

    def __init__(self, root, operation_id, targets):
        self.root = root
        self.operation_id = operation_id
        self.targets = tuple(targets)
        self._validate()

    @classmethod
    def capture(cls, storage, operation_id, targets):
        records = []
        for target in targets:
            node, job_id = target['node'], target['job_id']
            output = storage.output_file(node, job_id)
            relative = output.relative_to(storage.project_dir).as_posix()
            saved = output.with_name(f'.restart-{operation_id}-output.json')
            saved_relative = saved.relative_to(storage.project_dir).as_posix()
            original = capture_recovery_tree(storage.project_dir, relative)
            if capture_recovery_tree(storage.project_dir, saved_relative) is not None:
                raise RuntimeError('Restart private output already exists: ' + saved_relative)
            records.append({
                'node': node,
                'job_id': job_id,
                'target': target,
                'output': {'path': relative, 'saved': saved_relative, 'original': original},
            })
        return cls(storage.project_dir, operation_id, records)

    def _validate(self):
        if not re.fullmatch(r'[0-9a-f]{32}', self.operation_id):
            raise RuntimeError('Invalid restart operation identity')
        addresses, ordered, paths = set(), [], set()
        for item in self.targets:
            if not isinstance(item, dict) or set(item) != {'node', 'job_id', 'target', 'output'}:
                raise RuntimeError('Invalid restart target manifest')
            node, job_id = item['node'], item['job_id']
            if (not isinstance(node, str) or not node or type(job_id) is not int or job_id < 1
                    or not isinstance(item['target'], dict)
                    or item['target'].get('node') != node or item['target'].get('job_id') != job_id):
                raise RuntimeError('Invalid restart target address')
            output = item['output']
            expected = f'node/{node}/jobs/{job_id}/output.json'
            saved = f'node/{node}/jobs/{job_id}/.restart-{self.operation_id}-output.json'
            if (not isinstance(output, dict) or set(output) != {'path', 'saved', 'original'}
                    or output['path'] != expected or output['saved'] != saved):
                raise RuntimeError('Restart output belongs to another job')
            validate_tree_record(output['original'])
            address = node, job_id
            if address in addresses or expected in paths or saved in paths:
                raise RuntimeError('Duplicate restart output target')
            addresses.add(address)
            ordered.append(address)
            paths.update((expected, saved))
            recovery_path(self.root, expected)
            recovery_path(self.root, saved)
        if ordered != sorted(ordered):
            raise RuntimeError('Invalid restart target ordering')

    def manifest(self):
        return {'version': 1, 'operation_id': self.operation_id, 'targets': list(self.targets)}

    def _visible(self, item):
        return capture_recovery_tree(self.root, item['output']['path'])

    def _saved(self, item):
        return capture_recovery_tree(self.root, item['output']['saved'])

    def require_originals(self):
        for item in self.targets:
            if (self._visible(item) != item['output']['original']
                    or self._saved(item) is not None):
                raise RuntimeError('Restart output changed before staging: ' + item['output']['path'])

    def stage(self):
        self.require_originals()
        for item in self.targets:
            if item['output']['original'] is None:
                continue
            if (self._visible(item) != item['output']['original']
                    or self._saved(item) is not None):
                raise RuntimeError('Restart output changed during staging: ' + item['output']['path'])
            _move(recovery_path(self.root, item['output']['path']),
                  recovery_path(self.root, item['output']['saved']))
        self.require_staged()

    def require_staged(self):
        for item in self.targets:
            original = item['output']['original']
            if original is None:
                if self._visible(item) is not None or self._saved(item) is not None:
                    raise RuntimeError('Restart missing output changed during staging')
            elif self._visible(item) is not None or self._saved(item) != original:
                raise RuntimeError('Restart output was not staged exactly: ' + item['output']['path'])

    def restoration_steps(self):
        steps = []
        for item in self.targets:
            original = item['output']['original']
            visible, saved = self._visible(item), self._saved(item)
            if visible == original and saved is None:
                continue
            if original is None or visible is not None or saved != original:
                raise RuntimeError('Restart restoration files changed: ' + item['output']['path'])
            steps.append(item)
        return steps

    def restore(self):
        steps = self.restoration_steps()
        restored, failures = [], []
        for item in reversed(steps):
            try:
                if self._visible(item) is not None or self._saved(item) != item['output']['original']:
                    raise RuntimeError('Restart restoration target changed: ' + item['output']['path'])
                _move(recovery_path(self.root, item['output']['saved']),
                      recovery_path(self.root, item['output']['path']))
                restored.append(item)
            except BaseException as error:
                failures.append((item, error))
        if failures:
            first_item, first_error = failures[0]
            error = RuntimeError(
                'Restart output restoration failed; retained at '
                + str(recovery_path(self.root, first_item['output']['saved'])) + ': ' + str(first_error),
            )
            for item, failure in failures[1:]:
                add_recovery_note(
                    error,
                    'Restart output restoration failed; retained at '
                    + str(recovery_path(self.root, item['output']['saved'])) + ': ' + str(failure),
                )
            for item in reversed(restored):
                try:
                    if (self._visible(item) == item['output']['original']
                            and self._saved(item) is None):
                        _move(recovery_path(self.root, item['output']['path']),
                              recovery_path(self.root, item['output']['saved']))
                except BaseException as rollback_error:
                    add_recovery_note(
                        error,
                        'Restart restoration rollback requires recovery: ' + str(rollback_error),
                    )
            raise error from first_error
        self.require_restored()

    def require_restored(self):
        for item in self.targets:
            if self._visible(item) != item['output']['original'] or self._saved(item) is not None:
                raise RuntimeError('Restart did not restore the original output: ' + item['output']['path'])

    def has_private(self):
        return any(self._saved(item) is not None for item in self.targets)

    def has_private_path(self):
        return any(os.path.lexists(recovery_path(self.root, item['output']['saved']))
                   for item in self.targets)

    def require_private(self, *, cleanup=False):
        for item in self.targets:
            actual, expected = self._saved(item), item['output']['original']
            if actual is not None and not (known_remaining_tree(actual, expected) if cleanup else actual == expected):
                raise RuntimeError('Restart private output changed: ' + item['output']['saved'])

    def _remove_recorded_tree(self, relative, expected):
        actual = capture_recovery_tree(self.root, relative)
        if actual is None:
            return
        if not known_remaining_tree(actual, expected):
            raise RuntimeError('Restart private output changed before cleanup: ' + relative)
        path = recovery_path(self.root, relative)
        if actual['kind'] == 'file':
            path.chmod(writable_file_mode(actual['marker'][2]))
            actual = capture_recovery_tree(self.root, relative)
            if not known_remaining_tree(actual, expected):
                raise RuntimeError('Restart private file changed before cleanup: ' + relative)
            _discard(path)
            return
        for name in sorted(actual['children']):
            self._remove_recorded_tree(relative + '/' + name, expected['children'][name])
        actual = capture_recovery_tree(self.root, relative)
        if actual is None:
            return
        if not known_remaining_tree(actual, expected) or actual['children']:
            raise RuntimeError('Restart private directory changed before cleanup: ' + relative)
        _discard(path)

    def discard(self):
        self.require_private(cleanup=True)
        for item in self.targets:
            original = item['output']['original']
            if original is not None:
                self._remove_recorded_tree(item['output']['saved'], original)
        self.require_private(cleanup=True)
