"""Reversible filesystem staging for a single component preparation."""

from __future__ import annotations

import logging
import os
import shutil
from contextlib import contextmanager
from pathlib import Path
from uuid import uuid4

from micro_workflow_manager.project_format import is_link_or_reparse_point


def _contains_link(root, path):
    return any(is_link_or_reparse_point(part) for part in (path, *path.parents) if part.is_relative_to(root))


@contextmanager
def stage_preparation_files(root: Path, preparations):
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
    for path in targets | directories:
        if _contains_link(root, path) or not path.resolve().is_relative_to(root / 'node'):
            raise ValueError(f'Unsafe preparation path: {path}')
    paths = [path for path in sorted(targets) if not any(parent in targets for parent in path.parents)]
    staging_parent = root / '.mwf' / 'preparation-trash'
    if _contains_link(root, staging_parent) or not staging_parent.resolve().is_relative_to(root):
        raise ValueError(f'Unsafe preparation staging directory: {staging_parent}')
    staging = staging_parent / uuid4().hex
    moved, created = [], []
    try:
        for path in paths:
            if path.exists():
                staging.mkdir(parents=True, exist_ok=True)
                saved = staging / str(len(moved))
                path.rename(saved)
                moved.append((path, saved))
        for path in sorted(directories):
            if not path.exists():
                path.mkdir()
                created.append(path)
        yield
    except BaseException as error:
        restoration_errors = []
        for path in reversed(created):
            try:
                path.rmdir()
            except OSError as restore_error:
                restoration_errors.append(restore_error)
        for path, saved in reversed(moved):
            try:
                if os.path.lexists(path):
                    raise OSError(f'Preparation restoration target changed: {path}')
                saved.rename(path)
            except OSError as restore_error:
                restoration_errors.append(restore_error)
        if restoration_errors:
            note = f'Preparation files require recovery at {staging}: {restoration_errors!r}'
            error.__notes__ = [*getattr(error, '__notes__', ()), note]
            raise
        if staging.exists():
            staging.rmdir()
        raise
    else:
        if staging.exists():
            try:
                shutil.rmtree(staging)
            except OSError as error:
                logging.getLogger(__name__).warning(
                    'Prepared work committed; temporary files remain at %s: %s', staging, error,
                )
    finally:
        try:
            staging_parent.rmdir()
        except OSError:
            pass
