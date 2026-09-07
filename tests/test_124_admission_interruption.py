"""Public regressions for interrupted native session admission waits."""

from __future__ import annotations

from threading import Event

import pytest

from micro_workflow_manager import MicroWorkflow
from micro_workflow_manager.workflow.execution_session import execution_session
from micro_workflow_manager.workflow.sample_admission import SampleRequest
from tests.test_090_component_session_settlement import _close


def _workflow_with_job(tmp_path):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'A')])

    @workflow.task('A')
    def work(ctx, role=None):
        return role

    workflow.add_job(None, 'A', job_id=1)
    return workflow


def _interrupt_completed_result(monkeypatch, storage, method_name, interrupted, *, sample_only=False):
    original = getattr(storage, method_name)
    raised = []

    def submit(*args, **kwargs):
        pending = original(*args, **kwargs)
        if kwargs.get('_wait') is not False:
            return pending
        if sample_only and kwargs.get('_reserved_sample_builder') is None:
            return pending
        result = pending.result

        def deliver(*result_args, **result_kwargs):
            value = result(*result_args, **result_kwargs)
            if not raised:
                raised.append(interrupted)
                raise interrupted
            return value

        pending.result = deliver
        return pending

    monkeypatch.setattr(storage, method_name, submit)
    return raised


@pytest.mark.parametrize('boundary', ['session', 'reservation'])
def test_interrupted_ordinary_admission_uses_committed_ownership_for_cleanup(
    tmp_path, monkeypatch, boundary,
):
    workflow = _workflow_with_job(tmp_path)
    storage = workflow.storage
    expected_instance = storage.read_job_instance_id('A', 1)
    interrupted = KeyboardInterrupt('ordinary admission delivery interrupted')
    method = 'create_execution_session' if boundary == 'session' else 'reserve_execution_components'
    raised = _interrupt_completed_result(monkeypatch, storage, method, interrupted)
    entered = []
    try:
        with pytest.raises(KeyboardInterrupt) as observed:
            with execution_session(
                workflow, command='run jobs', start_node='A', nodes=['A'], selected_jobs=[1],
            ):
                entered.append(True)

        assert observed.value is interrupted
        assert raised == [interrupted]
        assert entered == []
        session, = storage.list_execution_sessions()
        assert (session['status'], session['outcome']) == ('terminal', 'failed')
        assert session['selected_components'] == [('A',)]
        assert session['selected_jobs'] == [('A', 1)]
        assert storage._read_session_job_roots(storage.db_connection(), session['session_id']) == [
            ('A', 1, expected_instance),
        ]
        assert storage.get_component_reservation(('A',)) is None
        assert storage.get_live_main_session() is None
        assert storage.get_job_status('A', 1) == 'queued'
        assert storage.read_job_current_owner('A', 1) is None
    finally:
        _close(storage)


def test_interrupted_combined_sample_admission_cleans_its_committed_reservation_and_roots(
    tmp_path, monkeypatch,
):
    workflow = _workflow_with_job(tmp_path)
    storage = workflow.storage
    expected_instance = storage.read_job_instance_id('A', 1)
    interrupted = KeyboardInterrupt('sample admission delivery interrupted')
    raised = _interrupt_completed_result(
        monkeypatch, storage, 'create_execution_session', interrupted, sample_only=True,
    )
    entered = []
    try:
        with pytest.raises(KeyboardInterrupt) as observed:
            with execution_session(
                workflow, command='run sample', start_node='A', nodes=['A'],
                sample_request=SampleRequest(('100%',), 'fixed-seed'),
            ):
                entered.append(True)

        assert observed.value is interrupted
        assert raised == [interrupted]
        assert entered == []
        session, = storage.list_execution_sessions()
        assert (session['status'], session['outcome']) == ('terminal', 'failed')
        assert session['selection_kind'] == 'jobs'
        assert session['selected_components'] == [('A',)]
        assert session['selected_jobs'] == [('A', 1)]
        assert storage._read_session_job_roots(storage.db_connection(), session['session_id']) == [
            ('A', 1, expected_instance),
        ]
        assert session['details']['selection']['kind'] == 'sample'
        assert session['details']['selection']['members']['A']['selected_job_ids'] == [1]
        assert storage.get_component_reservation(('A',)) is None
        assert storage.get_live_main_session() is None
        assert storage.get_job_status('A', 1) == 'queued'
        assert storage.read_job_current_owner('A', 1) is None
    finally:
        _close(storage)


@pytest.mark.parametrize('boundary', ['session', 'reservation'])
def test_interruption_stays_primary_when_the_admission_writer_rejects_the_transaction(
    tmp_path, monkeypatch, boundary,
):
    workflow = _workflow_with_job(tmp_path)
    storage = workflow.storage
    expected_instance = storage.read_job_instance_id('A', 1)
    table = 'execution_sessions' if boundary == 'session' else 'component_reservations'
    storage.submit_db_mutation(lambda connection: connection.execute(
        f"CREATE TRIGGER refuse_admission BEFORE INSERT ON {table} BEGIN "
        "SELECT RAISE(ABORT, 'admission refused'); END"
    ))
    interrupted = KeyboardInterrupt('failed admission delivery interrupted')
    method = 'create_execution_session' if boundary == 'session' else 'reserve_execution_components'
    original = getattr(storage, method)
    raised = []

    def submit(*args, **kwargs):
        pending = original(*args, **kwargs)
        if kwargs.get('_wait') is not False:
            return pending
        result = pending.result

        def deliver(*result_args, **result_kwargs):
            if not raised:
                try:
                    result(*result_args, **result_kwargs)
                except BaseException:
                    pass
                raised.append(interrupted)
                raise interrupted
            return result(*result_args, **result_kwargs)

        pending.result = deliver
        return pending

    monkeypatch.setattr(storage, method, submit)
    try:
        with pytest.raises(KeyboardInterrupt) as observed:
            with execution_session(
                workflow, command='run jobs', start_node='A', nodes=['A'], selected_jobs=[1],
            ):
                raise AssertionError('refused admission entered its body')

        assert observed.value is interrupted
        assert raised == [interrupted]
        assert any('admission refused' in note for note in interrupted.__notes__)
        if boundary == 'session':
            assert storage.list_execution_sessions() == []
        else:
            session, = storage.list_execution_sessions()
            assert (session['status'], session['outcome']) == ('terminal', 'failed')
            assert storage._read_session_job_roots(storage.db_connection(), session['session_id']) == [
                ('A', 1, expected_instance),
            ]
        assert storage.get_component_reservation(('A',)) is None
        assert storage.get_job_status('A', 1) == 'queued'
    finally:
        _close(storage)


@pytest.mark.parametrize('boundary', ['session', 'reservation', 'sample'])
def test_inflight_admission_wait_drains_commit_before_cleaning_exact_ownership(
    tmp_path, monkeypatch, boundary,
):
    workflow = _workflow_with_job(tmp_path)
    storage = workflow.storage
    expected_instance = storage.read_job_instance_id('A', 1)
    entered, release = Event(), Event()
    interrupted = KeyboardInterrupt('in-flight admission wait interrupted')
    method_name = 'reserve_execution_components' if boundary == 'reservation' else 'create_execution_session'
    original_method = getattr(storage, method_name)

    def method(*args, **kwargs):
        original_submit = storage.submit_db_mutation

        def submit(operation, *submit_args, **submit_kwargs):
            def gated(connection):
                entered.set()
                assert release.wait(15)
                return operation(connection)

            return original_submit(gated, *submit_args, **submit_kwargs)

        storage.submit_db_mutation = submit
        try:
            pending = original_method(*args, **kwargs)
        finally:
            storage.submit_db_mutation = original_submit
        assert kwargs.get('_wait') is False
        assert entered.wait(15)
        result = pending.result
        raised = []

        def deliver(*result_args, **result_kwargs):
            if not raised:
                raised.append(interrupted)
                release.set()
                raise interrupted
            return result(*result_args, **result_kwargs)

        pending.result = deliver
        return pending

    monkeypatch.setattr(storage, method_name, method)
    request = SampleRequest(('100%',), 'in-flight-seed') if boundary == 'sample' else None
    selected = None if request else [1]
    try:
        with pytest.raises(KeyboardInterrupt) as observed:
            with execution_session(
                workflow, command='run sample' if request else 'run jobs',
                start_node='A', nodes=['A'], selected_jobs=selected,
                sample_request=request,
            ):
                raise AssertionError('interrupted admission entered its body')

        assert observed.value is interrupted
        assert release.is_set()
        session, = storage.list_execution_sessions()
        assert (session['status'], session['outcome']) == ('terminal', 'failed')
        assert session['selected_jobs'] == [('A', 1)]
        assert storage._read_session_job_roots(storage.db_connection(), session['session_id']) == [
            ('A', 1, expected_instance),
        ]
        if request is not None:
            assert session['details']['selection']['kind'] == 'sample'
        assert storage.get_component_reservation(('A',)) is None
        assert storage.get_live_main_session() is None
        assert storage.get_job_status('A', 1) == 'queued'
        assert storage.read_job_current_owner('A', 1) is None
    finally:
        release.set()
        _close(storage)


def _fail_target_notification(
    monkeypatch, storage, method_name, notification_error, *, sample_only=False,
):
    original_method = getattr(storage, method_name)
    original_notify = storage.notify_state_change
    armed = []
    failed = []

    def notify():
        if armed and not failed:
            failed.append(notification_error)
            armed.clear()
            raise notification_error
        return original_notify()

    def method(*args, **kwargs):
        if kwargs.get('_wait') is not False:
            return original_method(*args, **kwargs)
        if sample_only and kwargs.get('_reserved_sample_builder') is None:
            return original_method(*args, **kwargs)
        original_submit = storage.submit_db_mutation

        def submit(operation, *submit_args, **submit_kwargs):
            def arm_after_operation(connection):
                value = operation(connection)
                armed.append(True)
                return value

            return original_submit(arm_after_operation, *submit_args, **submit_kwargs)

        storage.submit_db_mutation = submit
        try:
            return original_method(*args, **kwargs)
        finally:
            storage.submit_db_mutation = original_submit

    monkeypatch.setattr(storage, 'notify_state_change', notify)
    monkeypatch.setattr(storage, method_name, method)
    return failed


@pytest.mark.parametrize('boundary', ['session', 'reservation', 'sample'])
def test_postcommit_notification_failure_reads_durable_admission_before_cleanup(
    tmp_path, monkeypatch, boundary,
):
    workflow = _workflow_with_job(tmp_path)
    storage = workflow.storage
    expected_instance = storage.read_job_instance_id('A', 1)
    notification_error = OSError(f'{boundary} admission notification failed')
    method = 'reserve_execution_components' if boundary == 'reservation' else 'create_execution_session'
    failed = _fail_target_notification(
        monkeypatch, storage, method, notification_error, sample_only=boundary == 'sample',
    )
    sample_request = SampleRequest(('100%',), 'notification-seed') if boundary == 'sample' else None
    try:
        with pytest.raises(OSError) as observed:
            with execution_session(
                workflow, command='run sample' if sample_request else 'run jobs',
                start_node='A', nodes=['A'],
                selected_jobs=None if sample_request else [1],
                sample_request=sample_request,
            ):
                raise AssertionError('failed admission notification entered its body')

        assert observed.value is notification_error
        assert failed == [notification_error]
        session, = storage.list_execution_sessions()
        assert (session['status'], session['outcome']) == ('terminal', 'failed')
        assert session['selected_components'] == [('A',)]
        assert session['selected_jobs'] == [('A', 1)]
        assert storage._read_session_job_roots(storage.db_connection(), session['session_id']) == [
            ('A', 1, expected_instance),
        ]
        if sample_request is not None:
            assert session['selection_kind'] == 'jobs'
            assert session['details']['selection']['kind'] == 'sample'
            assert session['details']['selection']['members']['A']['selected_job_ids'] == [1]
        assert storage.get_component_reservation(('A',)) is None
        assert storage.get_live_main_session() is None
        assert storage.get_job_status('A', 1) == 'queued'
        assert storage.read_job_current_owner('A', 1) is None
    finally:
        _close(storage)
