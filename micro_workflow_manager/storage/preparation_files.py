"""Reversible filesystem staging for a single component preparation."""

from __future__ import annotations

from .recovery_errors import add_recovery_note

import logging
from contextlib import contextmanager
from pathlib import Path
from uuid import uuid4

from micro_workflow_manager.project_format import is_link_or_reparse_point
from .preparation_staging import allocate_preparation_staging


def _contains_link(root, path):
    return any(is_link_or_reparse_point(part) for part in (path, *path.parents) if part.is_relative_to(root))


@contextmanager
def stage_preparation_files(root: Path, preparations, *, inputs=(), receipt=None):
    root = root.resolve()
    targets, directories = set(), set()
    for plan in preparations:
        node = root / 'node' / plan.node
        output, jobs = node / 'output', node / 'jobs'
        if plan.clear_output:
            if output.exists() and not output.is_dir():
                raise ValueError(f'Expected directory: {output}')
            targets.add(output)
            directories.add(output)
        targets.update(jobs / str(job_id) for job_id in plan.delete_ids)
        targets.update(jobs / str(job_id) / 'output.json' for job_id in plan.reset_ids)
    targets.update(root / 'node' / item.receiver / 'input' / item.relative for item in inputs)
    for path in targets | directories:
        if _contains_link(root, path) or not path.resolve().is_relative_to(root / 'node'):
            raise ValueError(f'Unsafe preparation path: {path}')
    paths = [path for path in sorted(targets) if not any(parent in targets for parent in path.parents)]
    receivers = {plan.node for plan in preparations} | {item.receiver for item in inputs}
    operation_id = uuid4().hex if receipt is None else receipt.operation_id
    files = allocate_preparation_staging(root, operation_id, paths, directories, receivers)
    try:
        if receipt is not None:
            receipt.files = files
            receipt.prepare(files.manifest)
        files.stage()
        yield
    except BaseException as error:
        state = None
        if receipt is not None:
            try:
                state = receipt.state()
            except BaseException as observation_error:
                add_recovery_note(error, f'Preparation files require recovery at {files.directory}: {observation_error}')
                raise error
        if state != 'committed':
            try:
                files.restore()
                if state == 'prepared':
                    receipt.abort()
            except BaseException as restoration_error:
                add_recovery_note(error, f'Preparation files require recovery at {files.directory}: {restoration_error}')
                raise error
        _discard_preparation_files(files, original_error=error)
        raise
    else:
        _discard_preparation_files(files)
    finally:
        try:
            files.directory.parent.rmdir()
        except OSError:
            pass


def _discard_preparation_files(files, *, original_error=None):
    try:
        files.discard()
    except BaseException as error:
        if original_error is not None:
            add_recovery_note(original_error, f'Preparation files remain at {files.directory}: {error}')
        elif not isinstance(error, (OSError, RuntimeError)):
            raise
        logging.getLogger(__name__).warning('Preparation temporary files remain at %s: %s', files.directory, error)
