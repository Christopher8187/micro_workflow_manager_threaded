"""Stage observed outputs and restore them without replacing later files."""

from __future__ import annotations

from contextlib import contextmanager
import logging
import os
import stat

from .recovery_errors import add_recovery_note
from .recovery_moves import move_without_replacement
from .recovery_output import observe_recovery_file, recovery_path
from .recovery_tree import writable_file_mode


def require_recovery_outputs_unchanged(root, plan):
    for job in plan.jobs:
        if observe_recovery_file(root, job.output.relative_path).manifest() != job.output.manifest():
            raise RuntimeError('Recovery output changed: ' + job.output.relative_path)


class StagedRecoveryFiles:
    def __init__(self, root, plan, operation_id):
        self.root, self.plan, self.operation_id = root, plan, operation_id
        self.relative = '.mwf/recovery-trash/' + operation_id
        self.directory = recovery_path(root, self.relative)
        self.discard_relative = self.relative + '/discard'
        self.discard_directory = recovery_path(root, self.discard_relative)
        self.entries = tuple((job.output, str(position)) for position, job in enumerate(plan.jobs)
                             if job.terminal_status is None and job.output.marker is not None)
        self.identity = None
        self.discard_identity = None

    @classmethod
    def reopen(cls, root, plan, operation_id, manifest):
        files = cls(root, plan, operation_id)
        identity = manifest.get('identity') if isinstance(manifest, dict) else None
        discard = manifest.get('discard') if isinstance(manifest, dict) else None
        if (not isinstance(identity, list) or len(identity) != 3
                or any(type(item) is not int for item in identity) or not stat.S_ISDIR(identity[2])):
            raise RuntimeError('Invalid recovery staging identity')
        if (not isinstance(discard, dict) or set(discard) != {'directory', 'identity'}
                or discard['directory'] != files.discard_relative
                or not isinstance(discard['identity'], list) or len(discard['identity']) != 3
                or any(type(item) is not int for item in discard['identity'])
                or not stat.S_ISDIR(discard['identity'][2])):
            raise RuntimeError('Invalid recovery discard identity')
        files.identity = tuple(identity)
        files.discard_identity = tuple(discard['identity'])
        if manifest != files.manifest():
            raise RuntimeError('Recovery file manifest disagrees with its observation')
        return files

    def allocate(self):
        recovery_path(self.root, '.mwf/recovery-trash').mkdir(exist_ok=True)
        self.directory.mkdir(exist_ok=False)
        try:
            self.discard_directory.mkdir(exist_ok=False)
        except BaseException:
            self.directory.rmdir()
            raise
        status = self.directory.lstat()
        discarded = self.discard_directory.lstat()
        self.identity = status.st_dev, status.st_ino, status.st_mode
        self.discard_identity = discarded.st_dev, discarded.st_ino, discarded.st_mode

    def manifest(self):
        return {
            'directory': self.relative,
            'identity': None if self.identity is None else list(self.identity),
            'discard': {
                'directory': self.discard_relative,
                'identity': None if self.discard_identity is None else list(self.discard_identity),
            },
            'paths': [dict(original=original.manifest(), saved=saved)
                      for original, saved in self.entries],
        }

    def _require_directory(self, *, allow_removed_discard=False):
        directory = recovery_path(self.root, self.relative)
        status = directory.lstat()
        if (status.st_dev, status.st_ino, status.st_mode) != self.identity:
            raise RuntimeError('Recovery staging directory changed: ' + self.relative)
        discard = recovery_path(self.root, self.discard_relative)
        try:
            discarded = discard.lstat()
        except FileNotFoundError as error:
            if allow_removed_discard and not any(directory.iterdir()):
                return False
            raise RuntimeError('Recovery discard directory disappeared: '
                               + self.discard_relative) from error
        if (discarded.st_dev, discarded.st_ino, discarded.st_mode) != self.discard_identity:
            raise RuntimeError('Recovery discard directory changed: ' + self.discard_relative)
        saved_names = {saved for _, saved in self.entries}
        if {path.name for path in directory.iterdir()} - (saved_names | {'discard'}):
            raise RuntimeError('Recovery staging contains unexpected files: ' + self.relative)
        if {path.name for path in discard.iterdir()} - saved_names:
            raise RuntimeError('Recovery discard contains unexpected files: ' + self.discard_relative)
        return True

    def _saved(self, saved):
        return observe_recovery_file(self.root, self.relative + '/' + saved)

    def _discarded(self, saved):
        return observe_recovery_file(self.root, self.discard_relative + '/' + saved)

    def _require_discard_empty(self):
        self._require_directory()
        if any(self._discarded(saved).marker is not None for _, saved in self.entries):
            raise RuntimeError('Recovery discard area contains detached files: ' + self.discard_relative)

    @staticmethod
    def _same_file(actual, original, *, cleanup=False):
        marker = actual.marker
        if marker is not None and cleanup and original.marker is not None:
            expected = original.marker
            if marker[2] == writable_file_mode(expected[2]):
                marker = (*marker[:2], expected[2], *marker[3:])
        return marker == original.marker and actual.manifest()['sha256'] == original.manifest()['sha256']

    def stage(self):
        self._require_directory()
        self._require_discard_empty()
        require_recovery_outputs_unchanged(self.root, self.plan)
        for original, saved in self.entries:
            self._require_directory()
            if (self._saved(saved).marker is not None
                    or not self._same_file(observe_recovery_file(self.root, original.relative_path), original)):
                raise RuntimeError('Recovery staging files changed: ' + original.relative_path)
            os.replace(recovery_path(self.root, original.relative_path), self.directory / saved)
        self.require_staged()

    def _require_unmoved_outputs(self):
        staged_paths = {original.relative_path for original, _ in self.entries}
        for job in self.plan.jobs:
            if (job.output.relative_path not in staged_paths
                    and observe_recovery_file(self.root, job.output.relative_path).manifest()
                    != job.output.manifest()):
                raise RuntimeError('Recovery output changed: ' + job.output.relative_path)

    def require_staged(self):
        self._require_directory()
        self._require_discard_empty()
        for original, saved in self.entries:
            if (not self._same_file(self._saved(saved), original)
                    or observe_recovery_file(self.root, original.relative_path).marker is not None):
                raise RuntimeError('Recovery staged output changed: ' + original.relative_path)
        self._require_unmoved_outputs()

    def restoration_steps(self):
        self._require_directory()
        self._require_discard_empty()
        self._require_unmoved_outputs()
        pending = []
        for original, saved in self.entries:
            visible = observe_recovery_file(self.root, original.relative_path)
            private = self._saved(saved)
            if self._same_file(visible, original) and private.marker is None:
                continue
            if visible.marker is not None or not self._same_file(private, original):
                raise RuntimeError('Recovery restoration files changed: ' + original.relative_path)
            pending.append((original, saved))
        return pending

    def _before_restore_entry(self, original, saved):
        pass

    def _before_discard_entry(self, original, saved):
        pass

    def _after_discard_identity_read(self, original, saved):
        pass

    def _after_discard_detach(self, original, saved):
        pass

    def _after_discard_directory_remove(self):
        pass

    def restore(self):
        pending = self.restoration_steps()
        for original, saved in reversed(pending):
            self._before_restore_entry(original, saved)
            self._require_directory()
            if (observe_recovery_file(self.root, original.relative_path).marker is not None
                    or not self._same_file(self._saved(saved), original)):
                raise RuntimeError('Recovery restoration files changed: ' + original.relative_path)
            move_without_replacement(self.directory / saved,
                                     recovery_path(self.root, original.relative_path))
        self.require_restored()

    def require_restored(self):
        self._require_directory()
        self._require_discard_empty()
        require_recovery_outputs_unchanged(self.root, self.plan)
        if any(self._saved(saved).marker is not None for _, saved in self.entries):
            raise RuntimeError('Recovery restoration still has saved originals')

    def preflight_discard(self):
        self._require_directory()
        for original, saved in self.entries:
            source, detached = self._saved(saved), self._discarded(saved)
            if source.marker is not None and not self._same_file(source, original, cleanup=True):
                raise RuntimeError('Recovery cleanup file changed: ' + saved)
            if detached.marker is not None and not self._same_file(detached, original, cleanup=True):
                raise RuntimeError('Recovery detached cleanup file changed: ' + saved)
            if source.marker is not None and detached.marker is not None:
                raise RuntimeError('Recovery cleanup file exists in two locations: ' + saved)

    def _return_changed_detached(self, saved):
        detached = recovery_path(self.root, self.discard_relative + '/' + saved)
        source = recovery_path(self.root, self.relative + '/' + saved)
        try:
            if observe_recovery_file(self.root, self.relative + '/' + saved).marker is None:
                move_without_replacement(detached, source)
        except OSError:
            pass
        raise RuntimeError('Recovery cleanup file changed during detachment: ' + saved)

    def _delete_detached(self, original, saved):
        path = recovery_path(self.root, self.discard_relative + '/' + saved)
        actual = self._discarded(saved)
        if not self._same_file(actual, original, cleanup=True):
            raise RuntimeError('Recovery detached cleanup file changed: ' + saved)
        self._after_discard_detach(original, saved)
        actual = self._discarded(saved)
        if not self._same_file(actual, original, cleanup=True):
            raise RuntimeError('Recovery detached cleanup file changed: ' + saved)
        if os.name == 'nt' and not actual.marker[2] & stat.S_IWRITE:
            path.chmod(actual.marker[2] | stat.S_IWRITE)
            actual = self._discarded(saved)
            if not self._same_file(actual, original, cleanup=True):
                raise RuntimeError('Recovery detached cleanup file changed: ' + saved)
        path.unlink()

    def discard(self):
        if not self._require_directory(allow_removed_discard=True):
            self.directory.rmdir()
            try:
                self.directory.parent.rmdir()
            except OSError:
                pass
            return
        self.preflight_discard()
        for original, saved in self.entries:
            self._before_discard_entry(original, saved)
            self.preflight_discard()
            source, detached = self._saved(saved), self._discarded(saved)
            if source.marker is None and detached.marker is None:
                continue
            if source.marker is not None:
                if detached.marker is not None or not self._same_file(source, original, cleanup=True):
                    raise RuntimeError('Recovery cleanup file changed: ' + saved)
                self._after_discard_identity_read(original, saved)
                move_without_replacement(
                    recovery_path(self.root, self.relative + '/' + saved),
                    recovery_path(self.root, self.discard_relative + '/' + saved),
                )
                detached = self._discarded(saved)
                if not self._same_file(detached, original, cleanup=True):
                    self._return_changed_detached(saved)
            self._delete_detached(original, saved)
        self._require_directory()
        if any(self._saved(saved).marker is not None or self._discarded(saved).marker is not None
               for _, saved in self.entries):
            raise RuntimeError('Recovery cleanup still has saved files')
        self.discard_directory.rmdir()
        self._after_discard_directory_remove()
        self.directory.rmdir()
        try:
            self.directory.parent.rmdir()
        except OSError:
            pass


@contextmanager
def stage_session_recovery_files(storage, plan, receipt):
    files = StagedRecoveryFiles(storage.project_dir, plan, receipt.operation_id)
    receipt.files = files
    files.allocate()
    try:
        receipt.prepare(files.manifest())
        files.stage()
        yield files
    except BaseException as error:
        try:
            state = receipt.state()
        except BaseException as state_error:
            add_recovery_note(
                error,
                'Recovery receipt state unavailable; files retained: ' + str(state_error),
            )
            raise error
        try:
            if state != 'committed':
                files.restore()
                if state == 'prepared':
                    receipt.abort()
            files.discard()
        except BaseException as cleanup_error:
            add_recovery_note(
                error,
                'Recovery files remain at ' + str(files.directory) + ': ' + str(cleanup_error),
            )
        raise
    else:
        try:
            files.discard()
        except (OSError, RuntimeError) as error:
            logging.getLogger(__name__).warning(
                'Recovery committed; files remain at %s: %s',
                files.directory,
                error,
            )
