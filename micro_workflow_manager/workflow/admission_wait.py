"""Resolve one admission writer outcome before acting on an interrupted wait."""

from __future__ import annotations

from dataclasses import dataclass

from ..errors import safe_exception_repr


@dataclass(frozen=True)
class AdmissionWaitResult:
    value: object
    interruption: BaseException | None


@dataclass(frozen=True)
class _DrainedAdmission:
    value: object | None
    interruption: BaseException | None
    writer_error: BaseException | None

    def retained_error(self):
        if self.interruption is None:
            return self.writer_error
        if self.writer_error is not None:
            note = 'Admission writer also failed: ' + safe_exception_repr(self.writer_error)
            self.interruption.__notes__ = [
                *getattr(self.interruption, '__notes__', ()), note,
            ]
        return self.interruption


def _record_interruption(interruption, error):
    if interruption is None:
        return error
    note = 'Additional admission wait interruption: ' + safe_exception_repr(error)
    interruption.__notes__ = [*getattr(interruption, '__notes__', ()), note]
    return interruption


def _drain_admission_future(future):
    """Drain the real Future without treating cancellation as rollback."""
    interruption = None
    while True:
        try:
            return _DrainedAdmission(future.result(), interruption, None)
        except BaseException as delivered_error:
            if not future.done():
                interruption = _record_interruption(interruption, delivered_error)
                continue
            try:
                writer_error = future.exception()
            except BaseException as state_error:
                writer_error = state_error
            if writer_error is None:
                # A wrapper or signal interrupted delivery after the Future had
                # already retained a successful result. Read it again.
                interruption = _record_interruption(interruption, delivered_error)
                continue
            if delivered_error is not writer_error:
                interruption = _record_interruption(interruption, delivered_error)
            return _DrainedAdmission(None, interruption, writer_error)


def resolve_admission_future(future, readback):
    """Return durable admission state and a deferred original error.

    A mutation Future may fail after SQLite COMMIT when state-change
    notification fails. Only an exact readback can distinguish that outcome
    from a rolled-back writer operation.
    """
    drained = _drain_admission_future(future)
    if drained.writer_error is None:
        return AdmissionWaitResult(drained.value, drained.interruption)
    retained_error = drained.retained_error()
    try:
        durable = readback()
    except BaseException as read_error:
        note = 'Admission outcome readback failed: ' + safe_exception_repr(read_error)
        retained_error.__notes__ = [*getattr(retained_error, '__notes__', ()), note]
        raise retained_error from read_error
    if durable is None:
        if drained.interruption is not None:
            raise retained_error from drained.writer_error
        raise retained_error
    return AdmissionWaitResult(durable, retained_error)
