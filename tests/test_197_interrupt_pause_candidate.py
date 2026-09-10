"""Keep empty interrupt-pause polling cheap without weakening strict reads."""

from __future__ import annotations

import sqlite3
from types import SimpleNamespace

import pytest

from micro_workflow_manager.context import JobContext
from micro_workflow_manager.errors import JobRestartedError
from micro_workflow_manager.interrupt_cooperation import wait_for_interrupt_pauses
from micro_workflow_manager.storage.interrupt_coordination import (
    InterruptCoordinationStorageMixin,
)
from micro_workflow_manager.workflow.api_admission import _PermitController


IDENTITY = ("main-session", "A", 7, 3, "execution-7")


class _RawCandidateStorage(InterruptCoordinationStorageMixin):
    def __init__(self):
        self.connection = sqlite3.connect(":memory:")
        self.connection.execute(
            "CREATE TABLE interrupt_paused_executions(execution_id TEXT NOT NULL)"
        )

    @staticmethod
    def _session_text(value, field):
        assert value
        assert field == "execution_id"

    def db_connection(self):
        return self.connection


def test_raw_pause_row_is_always_a_strict_validation_candidate():
    storage = _RawCandidateStorage()
    try:
        storage.connection.execute(
            "INSERT INTO interrupt_paused_executions(execution_id) VALUES(?)",
            ("old-or-active-execution",),
        )
        assert storage._has_interrupt_pause_candidate("old-or-active-execution") is True
        assert storage._has_interrupt_pause_candidate("unrelated-execution") is False
    finally:
        storage.connection.close()


class _ObservedStorage:
    def __init__(self, *, candidate, strict=()):
        self.candidate = candidate
        self.strict = strict
        self.calls = []

    def _has_interrupt_pause_candidate(self, execution_id):
        self.calls.append(("candidate", execution_id))
        return self.candidate

    def read_active_interrupt_pauses(self, *identity):
        self.calls.append(("strict", identity))
        if isinstance(self.strict, BaseException):
            raise self.strict
        return self.strict


class _ObservedSystem:
    def __init__(self, storage, *, stale=False):
        self.storage = storage
        self.stale = stale
        self.execution_checks = []

    def execution_claim_context(self, node_name):
        assert node_name == "A"
        return "main-session", ("A",)

    def check_job_execution(self, node_name, job_id, generation, execution_id):
        identity = (node_name, job_id, generation, execution_id)
        self.execution_checks.append(identity)
        if self.stale:
            raise JobRestartedError("stale generation")


def _controller(storage, *, stale=False):
    system = _ObservedSystem(storage, stale=stale)
    return _PermitController(
        system=system,
        session_id=IDENTITY[0],
        node_name=IDENTITY[1],
        job_id=IDENTITY[2],
        generation=IDENTITY[3],
        execution_id=IDENTITY[4],
    ), system


def _context(storage, *, stale=False):
    system = _ObservedSystem(storage, stale=stale)
    context = JobContext(
        system,
        "A",
        SimpleNamespace(job_id=7, params={}),
        "work",
        1,
        1,
        execution_generation=3,
        execution_id="execution-7",
    )
    return context, system


@pytest.mark.parametrize("caller", ["permit", "checkpoint"])
def test_no_candidate_skips_the_strict_owner_observation(caller):
    storage = _ObservedStorage(candidate=False, strict=AssertionError(
        "strict current-owner observation reached the empty pause path"
    ))
    if caller == "permit":
        controller, _ = _controller(storage)
        assert controller._pause_children() == ()
    else:
        context, _ = _context(storage)
        wait_for_interrupt_pauses(context)
    assert storage.calls == [("candidate", "execution-7")]


@pytest.mark.parametrize("caller", ["permit", "checkpoint"])
@pytest.mark.parametrize(
    "strict",
    [(), JobRestartedError("candidate owner changed")],
    ids=["settled-row", "changed-owner"],
)
def test_every_candidate_still_uses_the_strict_reader(caller, strict):
    storage = _ObservedStorage(candidate=True, strict=strict)

    def invoke():
        if caller == "permit":
            controller, _ = _controller(storage)
            return controller._pause_children()
        context, _ = _context(storage)
        return wait_for_interrupt_pauses(context)

    if isinstance(strict, BaseException):
        with pytest.raises(JobRestartedError, match="candidate owner changed"):
            invoke()
    else:
        assert invoke() in (None, ())
    assert storage.calls == [
        ("candidate", "execution-7"),
        ("strict", IDENTITY),
    ]


@pytest.mark.parametrize("caller", ["permit", "checkpoint"])
def test_stale_generation_refuses_before_the_negative_candidate_probe(caller):
    storage = _ObservedStorage(candidate=False)
    if caller == "permit":
        controller, system = _controller(storage, stale=True)
        action = controller.acquire
    else:
        context, system = _context(storage, stale=True)
        action = context._check_execution
    with pytest.raises(JobRestartedError, match="stale generation"):
        action()
    assert system.execution_checks == [("A", 7, 3, "execution-7")]
    assert storage.calls == []


class _StrictReaderStorage(InterruptCoordinationStorageMixin):
    @staticmethod
    def _session_text(value, field):
        assert value
        assert field in {"session_id", "execution_id"}

    @staticmethod
    def validate_node_name(value):
        return value

    @staticmethod
    def validate_job_id(value):
        return value

    @staticmethod
    def db_connection():
        return object()

    @staticmethod
    def _read_job_owner_observation(connection, node_name, job_id):
        raise JobRestartedError("strict reader kept its owner validation")


def test_public_strict_reader_keeps_current_owner_validation():
    storage = _StrictReaderStorage()
    with pytest.raises(JobRestartedError, match="strict reader kept its owner validation"):
        storage.read_active_interrupt_pauses(*IDENTITY)
