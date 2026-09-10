"""Refuse an operation whose existing file lock is still held."""

from contextlib import contextmanager
import os
import re

from .recovery_output import recovery_path


class OperationStillActive(RuntimeError):
    """A bounded operation-lock probe found a current owner."""


@contextmanager
def operation_lock(storage, namespace, operation_id):
    if not re.fullmatch(r'[a-z][a-z-]*', namespace) or not re.fullmatch(r'[0-9a-f]{32}', operation_id):
        raise ValueError('Invalid operation lock identity')
    relative = '.mwf/' + namespace + '/' + operation_id + '.lock'
    path = recovery_path(storage.project_dir, relative)
    with _bounded_file_lock(storage, path, operation_id):
        yield


@contextmanager
def _bounded_file_lock(storage, path, identity):
    operation_id = identity
    path.parent.mkdir(exist_ok=True)
    local = storage.thread_lock_for(path)
    if not local.acquire(blocking=False):
        raise OperationStillActive('Operation is still active: ' + operation_id)
    try:
        with path.open('a+b') as handle:
            if os.name == 'nt':
                import msvcrt
                handle.seek(0, os.SEEK_END)
                if handle.tell() == 0:
                    handle.write(b'0')
                    handle.flush()
                handle.seek(0)
                try:
                    msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
                except OSError as error:
                    raise OperationStillActive('Operation is still active: ' + operation_id) from error
                try:
                    yield
                finally:
                    handle.seek(0)
                    msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl
                try:
                    fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                except OSError as error:
                    raise OperationStillActive('Operation is still active: ' + operation_id) from error
                try:
                    yield
                finally:
                    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
    finally:
        local.release()


@contextmanager
def recovery_job_fence(storage, node, job_id):
    path = storage.filesystem_lock_file('execution-fences', storage.job_execution_lock_name(node, job_id))
    recovery_path(storage.project_dir, path.relative_to(storage.project_dir).as_posix())
    with _bounded_file_lock(storage, path, f'{node}/{job_id}'):
        yield


@contextmanager
def recovery_advisory_lock(storage, name):
    path = storage.state_database_path().parent / 'logical-locks' / name
    local = storage.thread_lock_for(path)
    if not local.acquire(blocking=False):
        raise RuntimeError('Recovery resource is still active: ' + name)
    try:
        with storage.interprocess_lock(name, timeout=0):
            yield
    finally:
        local.release()


@contextmanager
def session_recovery_lock(storage, session_id):
    import hashlib
    identity = hashlib.sha256(session_id.encode('utf-8')).hexdigest()[:32]
    with operation_lock(storage, 'session-recovery-operations', identity):
        yield
