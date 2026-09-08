from __future__ import annotations

import os
import socket
import threading
import time

import pytest

from micro_workflow_manager import MicroWorkflow
from micro_workflow_manager.models import DONE, FAILED, Job, now
from micro_workflow_manager.processes import process_identity


def test_output_backed_terminal_reconciliation_is_idempotent(tmp_path, request):
    from tests.test_086_native_owned_restart import _close

    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'A')])
    storage = workflow.storage
    request.addfinalizer(lambda: _close(storage))
    snapshot = workflow.topology.snapshot()
    storage.register_component_topology(snapshot)
    session_id = 'native-output-recovery'
    storage.create_execution_session(
        session_id, session_kind='main', command='run', start_component=('A',),
        selected_components=[('A',)], started_at=now(), hostname=socket.gethostname(),
        pid=os.getpid(), process_identity=process_identity(os.getpid()),
        expected_shape=snapshot.shape_json,
    )
    storage.reserve_execution_components(session_id, expected_shape=snapshot.shape_json)
    storage.create_job(Job(node_name="A", job_id=1, params={}))
    generation, execution_id = storage.claim_job_execution(
        "A",
        1,
        started_at=now(),
        session_id=session_id,
        component=('A',),
    )
    storage.write_output(
        "A",
        1,
        {
            "status": DONE,
            "generation": generation,
            "result_repr": "'complete'",
        },
    )

    assert storage.get_job_status("A", 1) == "running"
    assert storage.reconcile_terminal_outputs(["A"]) == 1
    assert storage.get_job_status("A", 1) == DONE

    output = storage.output_file('A', 1).read_bytes()
    events = storage.read_job_events('A', 1)
    owner = storage.read_job_current_owner('A', 1)

    # The ordinary job finalizer may wake after recovery committed the same
    # lease. Treat that matching terminal state as success rather than a stale
    # execution error.
    storage.finalize_job_execution(
        "A",
        1,
        generation,
        execution_id,
        DONE,
        started_at=now(),
        finished_at=now(),
        duration_seconds=0.0,
        generation=generation,
        execution_id=execution_id,
    )
    assert storage.get_job_status("A", 1) == DONE
    assert storage.reconcile_terminal_outputs(['A']) == 0
    assert storage.output_file('A', 1).read_bytes() == output
    assert storage.read_job_events('A', 1) == events
    assert storage.read_job_current_owner('A', 1) == owner
    assert storage.read_job_control('A', 1)['active_execution_id'] is None
    assert owner['session_id'] == session_id
    decision = storage.decide_execution_session_exit(session_id, outcome='done', finished_at=now())
    assert decision['restarts'] == {} and decision['released'] == 1
    assert storage.get_execution_session(session_id)['outcome'] == 'done'
    assert storage.get_component_reservation(('A',)) is None


@pytest.mark.parametrize('second_status', [DONE, FAILED])
@pytest.mark.parametrize('first_scoped', [True, False])
def test_grouped_terminal_submissions_have_one_durable_result(tmp_path, monkeypatch, request, second_status, first_scoped):
    from concurrent.futures import ThreadPoolExecutor
    from micro_workflow_manager.errors import JobRestartedError
    from micro_workflow_manager.storage.execution_terminal import TerminalOwnerExpectation
    from tests.test_086_native_owned_restart import _close

    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'A')])
    storage = workflow.storage
    request.addfinalizer(lambda: _close(storage))
    snapshot = workflow.topology.snapshot()
    storage.register_component_topology(snapshot)
    session_id = 'native-grouped-terminal'
    storage.create_execution_session(
        session_id, session_kind='main', command='run', start_component=('A',),
        selected_components=[('A',)], started_at=now(), hostname=socket.gethostname(),
        pid=os.getpid(), process_identity=process_identity(os.getpid()),
        expected_shape=snapshot.shape_json,
    )
    storage.reserve_execution_components(session_id, expected_shape=snapshot.shape_json)
    storage.create_job(Job(node_name='A', job_id=1, params={}))
    generation, execution = storage.claim_job_execution(
        'A', 1, started_at=now(), session_id=session_id, component=('A',),
    )
    owner = storage.read_job_current_owner('A', 1)
    expectation = TerminalOwnerExpectation(session_id, ('A',), owner['job_instance_id'])
    storage.write_output('A', 1, {'status': DONE, 'generation': generation, 'result_repr': 'completed'})
    output = storage.output_file('A', 1).read_bytes()
    entered, release = threading.Event(), threading.Event()
    first_queued, both_queued = threading.Event(), threading.Event()
    submissions = []
    submit = storage.submit_grouped_db_mutation

    def record_submission(*args, wait=True, **kwargs):
        future = submit(*args, wait=False, **kwargs)
        submissions.append(future)
        if len(submissions) == 1:
            first_queued.set()
        if len(submissions) == 2:
            both_queued.set()
        return future.result() if wait else future

    monkeypatch.setattr(storage, 'submit_grouped_db_mutation', record_submission)

    def hold_writer(connection):
        entered.set()
        assert release.wait(30)

    gate = storage.submit_db_mutation(hold_writer, wait=False, priority=0)
    pool = ThreadPoolExecutor(max_workers=2)
    try:
        assert entered.wait(20)
        first = pool.submit(
            storage.finalize_job_execution, 'A', 1, generation, execution, DONE,
            expected_owner=expectation if first_scoped else None, submission='first',
        )
        assert first_queued.wait(20)
        second = pool.submit(
            storage.finalize_job_execution, 'A', 1, generation, execution, second_status,
            expected_owner=None if first_scoped else expectation, submission='second',
        )
        assert both_queued.wait(20)
        release.set()
        gate.result(timeout=20)
        first.result(timeout=20)
        if second_status == DONE:
            second.result(timeout=20)
        else:
            with pytest.raises(JobRestartedError):
                second.result(timeout=20)
        assert storage.get_job_status('A', 1) == DONE
        assert storage.read_job_control('A', 1)['active_execution_id'] is None
        terminal = [event for event in storage.read_job_events('A', 1)
                    if event['event'] in {DONE, FAILED}]
        assert len(terminal) == 1
        assert terminal[0]['event'] == DONE and terminal[0]['submission'] == 'first'
        assert storage.read_job_current_owner('A', 1) == owner
        assert storage.output_file('A', 1).read_bytes() == output
        decision = storage.decide_execution_session_exit(session_id, outcome='done', finished_at=now())
        assert decision['released'] == 1
    finally:
        release.set()
        pool.shutdown(wait=True)
        gate.result(timeout=20)


def test_component_failure_joins_started_jobs_without_recovery_scan(tmp_path, monkeypatch):
    from concurrent.futures import Future

    workflow = MicroWorkflow(tmp_path, runner="api")
    workflow.graph([("A", "B"), ("B", "A")])
    a_started = threading.Event()
    b_failed = threading.Event()
    release_a: Future[None] = Future()

    @workflow.task("A", runner="api", max_threads=1)
    def run_a(ctx):
        a_started.set()
        release_a.result()
        return "finished after sibling failure"

    @workflow.task("B", runner="api", max_threads=1)
    def run_b(ctx):
        assert a_started.wait(2)
        b_failed.set()
        raise RuntimeError("stop the component")

    workflow.add_jobs(None, "A", [{}])
    workflow.add_jobs(None, "B", [{}])
    workflow.active_job_restart_enabled = True

    recovery_calls = []
    original_reconcile = workflow.storage.reconcile_terminal_outputs

    def record_reconcile(*args, **kwargs):
        recovery_calls.append((args, kwargs))
        return original_reconcile(*args, **kwargs)

    monkeypatch.setattr(workflow.storage, "reconcile_terminal_outputs", record_reconcile)

    error = []

    def run_component():
        try:
            workflow.run_node("A", ignore_readiness=True)
        except BaseException as exc:
            error.append(exc)

    worker = threading.Thread(target=run_component)
    worker.start()
    assert b_failed.wait(3)

    # The first failure has stopped admission, but the already-started A job is
    # still owned by the active component and must be allowed to finish.
    time.sleep(0.05)
    assert worker.is_alive()
    assert workflow.storage.get_job_status("A", 1) == "running"

    release_a.set_result(None)
    worker.join(timeout=5)
    assert not worker.is_alive()
    assert error
    assert "stop the component" in workflow.storage.read_json(workflow.storage.output_file("B", 1))["error"]
    assert workflow.storage.get_job_status("A", 1) == DONE
    assert workflow.storage.get_job_status("B", 1) == FAILED
    assert workflow.storage.get_node_status("A") == FAILED
    assert workflow.storage.get_node_status("B") == FAILED
    assert recovery_calls == []
