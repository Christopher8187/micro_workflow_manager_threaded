from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from threading import Barrier, Event, Thread, current_thread

import pytest

from micro_workflow_manager import MicroWorkflow, NodeRouter
from micro_workflow_manager.storage import FileStorage
from tests.test_086_native_owned_restart import _close, _snapshot
from tests.test_active_job_restart import wait_until


@pytest.mark.parametrize('damage', ['none', 'before_scan', 'after_capture', 'after_reservation'])
def test_output_reconciliation_validates_native_claim_for_recovery(tmp_path, monkeypatch, damage):
    workflow = MicroWorkflow(tmp_path, runner='threaded', persist_graph=False)
    workflow.graph([('A', 'A')])
    storage = workflow.storage
    router = NodeRouter('A', runner='threaded', max_threads=1)
    publication_error = OSError('terminal publication failed after output was saved')
    failed, damaged = [], []

    @router.task
    def work(ctx):
        return 'completed handler output'

    workflow.include_routers(router)
    workflow.add_job(None, 'A')
    finalize = storage.finalize_job_execution

    def fail_terminal_publication(node, job_id, *args, **extra):
        if not failed:
            failed.append(storage.output_file(node, job_id).read_bytes())
            raise publication_error
        return finalize(node, job_id, *args, **extra)

    monkeypatch.setattr(storage, 'finalize_job_execution', fail_terminal_publication)
    list_jobs = storage.list_jobs

    def damage_owner_before_cleanup(node, status=None):
        if damage == 'before_scan' and status == 'running' and failed and not damaged:
            storage.submit_db_mutation(lambda connection: connection.execute(
                "UPDATE jobs SET active_execution_id='missing-native-owner' "
                "WHERE node_name='A' AND job_id=1",
            ).rowcount)
            damaged.append(storage.read_job_control('A', 1))
        return list_jobs(node, status)

    monkeypatch.setattr(storage, 'list_jobs', damage_owner_before_cleanup)
    submit = storage.submit_grouped_db_mutation

    def damage_owner_before_terminal_submission(*args, **kwargs):
        if damage in {'after_capture', 'after_reservation'} and args[0] == ('terminal',) and failed and not damaged:
            owner = storage.read_job_current_owner('A', 1)
            if damage == 'after_capture':
                changed = storage.submit_db_mutation(lambda connection: connection.execute(
                    'UPDATE job_execution_owners SET job_instance_id=? WHERE execution_id=?',
                    ('0' * 32, owner['execution_id']),
                ).rowcount)
            else:
                changed = storage.submit_db_mutation(lambda connection: connection.execute(
                    'DELETE FROM component_reservations WHERE session_id=?',
                    (owner['session_id'],),
                ).rowcount)
            assert changed == 1
            damaged.append(storage.read_job_control('A', 1))
        return submit(*args, **kwargs)

    monkeypatch.setattr(storage, 'submit_grouped_db_mutation', damage_owner_before_terminal_submission)
    try:
        with pytest.raises(OSError) as caught:
            workflow.run_component({'A'})
        assert caught.value is publication_error
        sessions = storage.list_execution_sessions()
        assert storage.output_file('A', 1).read_bytes() == failed[0]
        if damage == 'none':
            assert not damaged
            assert len(sessions) == 1 and sessions[0]['outcome'] == 'failed'
            assert storage.get_job_status('A', 1) == 'done'
            assert storage.read_job_control('A', 1)['active_execution_id'] is None
            assert storage.get_component_reservation(('A',)) is None
            assert any(event['event'] == 'done' and event.get('recovered_from_output') is True
                       for event in storage.read_job_events('A', 1))
            return
        assert len(damaged) == 1
        assert len(sessions) == 1 and sessions[0]['status'] == 'running'
        reservation = storage.get_component_reservation(('A',))
        if damage == 'after_reservation':
            assert reservation is None
        else:
            assert reservation['session_id'] == sessions[0]['session_id']
        assert storage.get_job_status('A', 1) == 'running'
        assert storage.read_job_control('A', 1) == damaged[0]
        assert not any(event['event'] in {'done', 'failed', 'cancelled', 'skipped'}
                       for event in storage.read_job_events('A', 1))
        assert any('still owns active job A/1' in note for note in getattr(caught.value, '__notes__', ()))
        with pytest.raises(RuntimeError, match='already active|requires recovery'):
            workflow.run_component({'A'})
        assert len(storage.list_execution_sessions()) == 1
    finally:
        _close(storage)


@pytest.mark.parametrize('runner', ['threaded', 'direct', 'api'])
@pytest.mark.parametrize('restart_order', ['settlement', 'first_scan', 'reconciliation'])
def test_output_failure_cleanup_keeps_accepted_restart(tmp_path, monkeypatch, runner, restart_order):
    workflow = MicroWorkflow(tmp_path, runner=runner, persist_graph=False)
    workflow.graph([('A', 'A')])
    storage = workflow.storage
    router = NodeRouter('A', runner=runner, max_threads=1)
    calls, restarted = [], []
    output_failed = Event()
    output_error = OSError('output write failed before terminal publication')

    @router.task
    def work(ctx):
        calls.append((ctx.job_id, ctx.execution_generation))
        return ctx.job_id

    workflow.include_routers(router)
    workflow.add_job(None, 'A')
    workflow.add_job(None, 'A')
    write_output = storage.write_output

    def fail_first_output(node, job_id, output):
        if node == 'A' and job_id == 1 and output['generation'] == 0:
            output_failed.set()
            raise output_error
        return write_output(node, job_id, output)

    monkeypatch.setattr(storage, 'write_output', fail_first_output)

    def request_restart():
        command = subprocess.run(
            [sys.executable, '-m', 'micro_workflow_manager', 'restart', 'A', 'job', '1'],
            cwd=tmp_path, env=dict(os.environ, PYTHONPATH=str(Path(__file__).resolve().parents[1])),
            capture_output=True, text=True, timeout=20,
        )
        assert command.returncode == 0, command.stdout + command.stderr
        restarted.append(storage.read_job_owner_observation('A', 1))

    def restart_during_cleanup(node, job_id, extra):
        if (restart_order != 'settlement' or not extra.get('recovered_after_component_abort')
                or restarted):
            return
        assert node == 'A' and job_id == 1
        request_restart()

    list_jobs = storage.list_jobs

    def restart_before_running_scan(node, status=None):
        if restart_order == 'first_scan' and status == 'running' and output_failed.is_set() and not restarted:
            request_restart()
        return list_jobs(node, status)

    monkeypatch.setattr(storage, 'list_jobs', restart_before_running_scan)
    reconcile = storage.reconcile_terminal_outputs

    def restart_before_reconciliation(*args, **kwargs):
        if restart_order == 'reconciliation' and not restarted:
            request_restart()
        return reconcile(*args, **kwargs)

    monkeypatch.setattr(storage, 'reconcile_terminal_outputs', restart_before_reconciliation)

    set_status = storage.set_job_status

    def restart_before_status_write(node, job_id, status, **extra):
        restart_during_cleanup(node, job_id, extra)
        return set_status(node, job_id, status, **extra)

    monkeypatch.setattr(storage, 'set_job_status', restart_before_status_write)
    finalize = storage.finalize_job_execution

    def restart_before_terminal_write(node, job_id, *args, **extra):
        restart_during_cleanup(node, job_id, extra)
        return finalize(node, job_id, *args, **extra)

    monkeypatch.setattr(storage, 'finalize_job_execution', restart_before_terminal_write)
    try:
        with pytest.raises(OSError) as caught:
            workflow.run_component({'A'})
        assert caught.value is output_error
        assert len(restarted) == 1
        accepted = restarted[0]
        assert accepted['generation'] == 1 and accepted['status'] == 'queued'
        assert accepted['active_execution_id'] is None
        assert calls == [(1, 0), (1, 1)]
        assert storage.get_job_status('A', 1) == 'done'
        assert storage.get_job_status('A', 2) == 'queued'
        assert storage.read_job_current_owner('A', 2) is None
        owner = storage.read_job_current_owner('A', 1)
        assert owner['generation'] == 1
        assert owner['session_id'] == accepted['owner']['session_id']
        assert owner['job_instance_id'] == accepted['owner']['job_instance_id']
        assert owner['execution_id'] != accepted['owner']['execution_id']
        assert storage.get_execution_session(owner['session_id'])['outcome'] == 'failed'
        assert storage.get_component_reservation(('A',)) is None
        assert storage.list_jobs('A', status='running') == []
    finally:
        _close(storage)


def test_api_two_lanes_retain_multiple_restarted_preclaims(tmp_path, monkeypatch):
    monkeypatch.setenv('MWF_API_STARTUP_STRATEGY', 'lanes:2')
    workflow = MicroWorkflow(tmp_path, runner='api', persist_graph=False)
    workflow.graph([('A', 'A')])
    storage = workflow.storage
    router = NodeRouter('A', runner='api', max_threads=6)
    startup, failure = Barrier(2), Barrier(2)
    at_decision, release_decision = Event(), Event()
    calls, result = [], {}
    handler_errors = {2: ValueError('first API lane failed'), 4: ValueError('second API lane failed')}

    @router.task
    def work(ctx):
        calls.append((ctx.job_id, ctx.execution_generation, current_thread().name))
        if ctx.execution_generation == 0:
            if ctx.job_id in (1, 4):
                startup.wait(timeout=20)
            if ctx.job_id in (2, 4):
                failure.wait(timeout=20)
                raise handler_errors[ctx.job_id]
        return ctx.job_id

    workflow.include_routers(router)
    for _ in range(7):
        workflow.add_job(None, 'A')

    # Each mutation still uses the real writer. Completing it before its Future
    # is observed keeps the unstarted remainder of each claimed burst visible.
    for name in ('submit_db_mutation', 'submit_grouped_db_mutation'):
        submit = getattr(storage, name)

        def complete_before_observation(*args, _submit=submit, wait=True, **kwargs):
            future = _submit(*args, wait=False, **kwargs)
            completed = Event()
            future.add_done_callback(lambda _: completed.set())
            assert completed.wait(20)
            return future.result() if wait else future

        monkeypatch.setattr(storage, name, complete_before_observation)

    decide = storage.decide_execution_session_exit

    def pause_failed_decision(*args, **kwargs):
        if kwargs.get('outcome') == 'failed' and not at_decision.is_set():
            at_decision.set()
            assert release_decision.wait(30)
        return decide(*args, **kwargs)

    monkeypatch.setattr(storage, 'decide_execution_session_exit', pause_failed_decision)

    def run():
        try:
            result['value'] = workflow.run_component({'A'})
        except BaseException as error:
            result['error'] = error

    thread = Thread(target=run, name='api-multiple-lane-restarts', daemon=True)
    thread.start()
    try:
        assert at_decision.wait(25), result
        assert sorted((job_id, generation) for job_id, generation, _ in calls) == [(1, 0), (2, 0), (4, 0)]
        pumps = {name for _, _, name in calls}
        assert len(pumps) == 2 and all(name.startswith('mwf-api-start-A_') for name in pumps)
        owners = {job_id: storage.read_job_current_owner('A', job_id) for job_id in range(1, 7)}
        session_id = owners[1]['session_id']
        assert all(owner['session_id'] == session_id for owner in owners.values())
        assert storage.get_job_status('A', 1) == 'done'
        assert storage.get_job_status('A', 2) == storage.get_job_status('A', 4) == 'failed'
        peer_output = storage.output_file('A', 1).read_bytes()
        peer_events = storage.read_job_events('A', 1)
        for job_id in (3, 5, 6):
            assert storage.get_job_status('A', job_id) == 'queued'
            assert storage.read_job_control('A', job_id)['active_execution_id'] is None
            assert not storage.output_file('A', job_id).exists()
            assert any(event.get('reason') == 'preclaimed burst was not started'
                       for event in storage.read_job_events('A', job_id))
            workflow.cancel_job('A', job_id)
            assert storage.read_job_current_owner('A', job_id) == owners[job_id]
        assert storage.get_job_status('A', 7) == 'queued'
        assert storage.read_job_current_owner('A', 7) is None
        command = subprocess.run(
            [sys.executable, '-m', 'micro_workflow_manager', 'restart', 'A', 'jobs', '3', '5-6'],
            cwd=tmp_path, env=dict(os.environ, PYTHONPATH=str(Path(__file__).resolve().parents[1])),
            capture_output=True, text=True, timeout=20,
        )
        assert command.returncode == 0, command.stdout + command.stderr
        for job_id in (3, 5, 6):
            assert storage.current_job_generation('A', job_id) == 1
            assert storage.read_job_current_owner('A', job_id) == owners[job_id]
            assert storage.get_job_status('A', job_id) == 'queued'
            assert storage.read_job_control('A', job_id)['active_execution_id'] is None
        release_decision.set()
        thread.join(timeout=20)
        assert not thread.is_alive(), result
        assert result['error'].__cause__ in handler_errors.values()
        assert sorted((job_id, generation) for job_id, generation, _ in calls) == [
            (1, 0), (2, 0), (3, 1), (4, 0), (5, 1), (6, 1),
        ]
        assert storage.output_file('A', 1).read_bytes() == peer_output
        assert storage.read_job_events('A', 1) == peer_events
        for job_id in (1, 2, 4):
            assert storage.read_job_control('A', job_id)['active_execution_id'] is None
            assert storage.read_job_current_owner('A', job_id) == owners[job_id]
        for job_id in (3, 5, 6):
            assert storage.get_job_status('A', job_id) == 'done'
            assert storage.read_job_control('A', job_id)['active_execution_id'] is None
            replacement = storage.read_job_current_owner('A', job_id)
            assert replacement['session_id'] == session_id
            assert replacement['job_instance_id'] == owners[job_id]['job_instance_id']
            assert replacement['generation'] == 1
            assert replacement['execution_id'] != owners[job_id]['execution_id']
        assert storage.get_job_status('A', 2) == storage.get_job_status('A', 4) == 'failed'
        assert storage.get_job_status('A', 7) == 'queued'
        assert storage.read_job_current_owner('A', 7) is None
        assert storage.list_jobs('A', status='running') == []
        assert storage.get_execution_session(session_id)['outcome'] == 'failed'
        assert storage.get_component_reservation(('A',)) is None
    finally:
        release_decision.set()
        startup.abort()
        failure.abort()
        thread.join(timeout=20)
        assert not thread.is_alive(), result
        _close(storage)


@pytest.mark.parametrize('global_runner', ['api', 'direct'])
@pytest.mark.parametrize('release_failure', [False, True, 'both', 'both_restart', 'both_damaged',
                                           'both_damaged_reservation', 'restart_during_settlement',
                                           'restart_before_release', 'restart_after_settlement',
                                           'notification_failure', 'restart_after_release'])
def test_api_abandoned_burst_keeps_last_owner_and_runs_after_restart(tmp_path, monkeypatch, release_failure, global_runner):
    workflow = MicroWorkflow(tmp_path, runner=global_runner, persist_graph=False)
    workflow.graph([('A', 'A')])
    storage = workflow.storage
    router = NodeRouter('A', runner='api', max_threads=2)
    calls, result = [], {}
    at_decision, release_decision = Event(), Event()
    handler_error = ValueError('first API item fails before yielding')
    failed_releases = []
    settlement_restarts = []
    events_at_restart = []
    release_results = []
    failed_notifications = []
    settlement_race = release_failure in ('restart_before_release', 'restart_during_settlement',
                                         'restart_after_settlement', 'restart_after_release')
    cleanup_fails = release_failure in ('both', 'both_restart', 'both_damaged', 'both_damaged_reservation')
    damaged_owner = release_failure in ('both_damaged', 'both_damaged_reservation')

    @router.task
    def work(ctx):
        calls.append((ctx.job_id, ctx.execution_generation))
        if ctx.job_id == 1 and ctx.execution_generation == 0:
            raise handler_error
        return ctx.job_id

    workflow.include_routers(router)
    workflow.add_job(None, 'A')
    workflow.add_job(None, 'A')
    if settlement_race:
        workflow.add_job(None, 'A')
    decide = storage.decide_execution_session_exit

    # Model a writer that finishes each awaited mutation before the API fiber
    # observes its Future. Every operation still uses the real storage writer.
    for name in ('submit_db_mutation', 'submit_grouped_db_mutation'):
        submit = getattr(storage, name)

        def complete_before_observation(*args, _submit=submit, wait=True, **kwargs):
            future = _submit(*args, wait=False, **kwargs)
            completed = Event()
            future.add_done_callback(lambda _: completed.set())
            assert completed.wait(20)
            return future.result() if wait else future

        monkeypatch.setattr(storage, name, complete_before_observation)

    release_unstarted = storage.release_unstarted_job_execution

    def restart_abandoned_job():
        command = subprocess.run(
            [sys.executable, '-m', 'micro_workflow_manager', 'restart', 'A', 'job', '2'],
            cwd=tmp_path, env=dict(os.environ, PYTHONPATH=str(Path(__file__).resolve().parents[1])),
            capture_output=True, text=True, timeout=20,
        )
        assert command.returncode == 0, command.stdout + command.stderr
        settlement_restarts.append(storage.read_job_owner_observation('A', 2))
        events_at_restart.append(storage.read_job_events('A', 2))

    def fail_unstarted_release(node, job_id, *args, **kwargs):
        if release_failure == 'restart_before_release' and node == 'A' and job_id == 2:
            restart_abandoned_job()
            released = release_unstarted(node, job_id, *args, **kwargs)
            release_results.append(released)
            return released
        if release_failure not in (False, 'notification_failure', 'restart_after_release') and node == 'A' and job_id == 2:
            failed_releases.append(job_id)
            raise OSError('unstarted release write failed')
        released = release_unstarted(node, job_id, *args, **kwargs)
        if release_failure == 'restart_after_release':
            release_results.append(released)
        return released

    monkeypatch.setattr(storage, 'release_unstarted_job_execution', fail_unstarted_release)
    notify_queue_change = storage.notify_queue_change

    def fail_released_queue_notification(node=None):
        if (release_failure == 'notification_failure' and node == 'A' and not failed_notifications
                and any(event.get('reason') == 'preclaimed burst was not started'
                        for event in storage.read_job_events('A', 2))):
            failed_notifications.append(True)
            raise OSError('queue wake failed after release committed')
        return notify_queue_change(node)

    monkeypatch.setattr(storage, 'notify_queue_change', fail_released_queue_notification)
    set_job_status = storage.set_job_status

    def before_abandoned_settlement(node, job_id):
        if cleanup_fails and node == 'A' and job_id == 2:
            raise OSError('abandoned claim settlement write failed')
        if release_failure == 'restart_during_settlement' and node == 'A' and job_id == 2:
            restart_abandoned_job()

    def fail_abandoned_settlement(node, job_id, status, **kwargs):
        before_abandoned_settlement(node, job_id)
        return set_job_status(node, job_id, status, **kwargs)

    monkeypatch.setattr(storage, 'set_job_status', fail_abandoned_settlement)
    finalize = storage.finalize_job_execution

    def fail_exact_abandoned_settlement(node, job_id, *args, **kwargs):
        if kwargs.get('recovered_after_component_abort'):
            before_abandoned_settlement(node, job_id)
        value = finalize(node, job_id, *args, **kwargs)
        if (release_failure == 'restart_after_settlement' and node == 'A' and job_id == 2
                and kwargs.get('recovered_after_component_abort')):
            restart_abandoned_job()
        return value

    monkeypatch.setattr(storage, 'finalize_job_execution', fail_exact_abandoned_settlement)

    def pause_failed_decision(*args, **kwargs):
        if kwargs.get('outcome') == 'failed' and not at_decision.is_set():
            at_decision.set()
            assert release_decision.wait(30)
        return decide(*args, **kwargs)

    monkeypatch.setattr(storage, 'decide_execution_session_exit', pause_failed_decision)

    def run():
        try:
            result['value'] = workflow.run_component({'A'})
        except BaseException as error:
            result['error'] = error

    thread = Thread(target=run, name='api-burst-abandonment', daemon=True)
    thread.start()
    try:
        assert at_decision.wait(25), result
        assert calls == [(1, 0)]
        owner = storage.read_job_current_owner('A', 1)
        abandoned_owner = storage.read_job_current_owner('A', 2)
        assert abandoned_owner['session_id'] == owner['session_id']
        assert abandoned_owner['generation'] == 0
        if release_failure == 'restart_after_release':
            assert release_results == [True]
            assert storage.get_job_status('A', 2) == 'queued'
            workflow.cancel_job('A', 2)
            assert storage.read_job_current_owner('A', 2) == abandoned_owner
            restart_abandoned_job()
        if settlement_race:
            assert len(settlement_restarts) == 1
            assert storage.read_job_events('A', 2) == events_at_restart[0]
            if release_failure == 'restart_before_release':
                assert release_results == [False]
            assert storage.get_job_status('A', 2) == 'queued'
            assert storage.current_job_generation('A', 2) == 1
            release_decision.set()
            thread.join(timeout=20)
            assert not thread.is_alive(), result
            assert result['error'].__cause__ is handler_error
            assert calls == [(1, 0), (2, 1)]
            assert storage.get_job_status('A', 1) == 'failed'
            assert storage.get_job_status('A', 2) == 'done'
            assert storage.get_job_status('A', 3) == 'queued'
            assert storage.read_job_current_owner('A', 3) is None
            replacement = storage.read_job_current_owner('A', 2)
            assert replacement['session_id'] == owner['session_id']
            assert replacement['job_instance_id'] == abandoned_owner['job_instance_id']
            assert replacement['generation'] == 1
            assert replacement['execution_id'] != abandoned_owner['execution_id']
            assert storage.get_execution_session(owner['session_id'])['outcome'] == 'failed'
            assert storage.get_component_reservation(('A',)) is None
            assert storage.list_jobs('A', status='running') == []
            return
        if release_failure not in (False, 'notification_failure'):
            assert failed_releases == [2]
            assert storage.get_job_status('A', 2) == ('running' if cleanup_fails else 'failed')
            if damaged_owner:
                # Simulate a damaged native active-owner pointer after the
                # cleanup scan, before the real terminal writer decision.
                storage.submit_db_mutation(lambda connection: connection.execute(
                    "UPDATE jobs SET active_execution_id='missing-execution-owner' "
                    "WHERE node_name='A' AND job_id=2",
                ).rowcount)
                if release_failure == 'both_damaged_reservation':
                    storage.submit_db_mutation(lambda connection: connection.execute(
                        'DELETE FROM component_reservations WHERE session_id=?', (owner['session_id'],),
                    ).rowcount)
            if release_failure == 'both_restart':
                command = subprocess.run(
                    [sys.executable, '-m', 'micro_workflow_manager', 'restart', 'A', 'job', '1'],
                    cwd=tmp_path, env=dict(os.environ, PYTHONPATH=str(Path(__file__).resolve().parents[1])),
                    capture_output=True, text=True, timeout=20,
                )
                assert command.returncode == 0, command.stdout + command.stderr
            release_decision.set()
            thread.join(timeout=20)
            assert not thread.is_alive(), result
            assert result['error'].__cause__ is handler_error
            assert calls == [(1, 0)]
            session = storage.get_execution_session(owner['session_id'])
            if session['status'] == 'terminal':
                assert session['outcome'] == 'failed'
                assert storage.get_component_reservation(('A',)) is None
                assert storage.list_jobs('A', status='running') == []
                assert storage.read_job_control('A', 2)['active_execution_id'] is None
            else:
                assert cleanup_fails
                if release_failure == 'both_damaged_reservation':
                    assert storage.get_component_reservation(('A',)) is None
                else:
                    assert storage.get_component_reservation(('A',))['session_id'] == owner['session_id']
                assert any('still owns active job A/2' in note
                           for note in getattr(result['error'], '__notes__', ()))
                Event().wait(2.2)
                assert storage.get_execution_session(owner['session_id'])['heartbeat_at'] == session['heartbeat_at']
                with pytest.raises(RuntimeError, match='already active|requires recovery'):
                    workflow.run_component({'A'})
                assert len(storage.list_execution_sessions()) == 1
            if damaged_owner:
                assert storage.read_job_control('A', 2)['active_execution_id'] == 'missing-execution-owner'
            else:
                assert storage.read_job_current_owner('A', 2) == abandoned_owner
            return
        assert storage.get_job_status('A', 2) == 'queued'
        assert failed_notifications == ([True] if release_failure == 'notification_failure' else [])
        assert storage.read_job_control('A', 2)['active_execution_id'] is None
        assert not storage.output_file('A', 2).exists()
        assert len([event for event in storage.read_job_events('A', 2) if event['event'] == 'started']) == 1
        assert any(event['event'] == 'queued'
                   and event.get('reason') == 'preclaimed burst was not started'
                   for event in storage.read_job_events('A', 2))
        command = subprocess.run(
            [sys.executable, '-m', 'micro_workflow_manager', 'restart', 'A', 'job', '1'],
            cwd=tmp_path, env=dict(os.environ, PYTHONPATH=str(Path(__file__).resolve().parents[1])),
            capture_output=True, text=True, timeout=20,
        )
        assert command.returncode == 0, command.stdout + command.stderr
        release_decision.set()
        thread.join(timeout=20)
        assert not thread.is_alive(), result
        assert result == {'value': ['A']}
        assert calls == [(1, 0), (1, 1), (2, 0)]
        for job_id in (1, 2):
            assert storage.get_job_status('A', job_id) == 'done'
            assert storage.read_job_control('A', job_id)['active_execution_id'] is None
            assert storage.read_job_current_owner('A', job_id)['session_id'] == owner['session_id']
        assert storage.read_job_current_owner('A', 2)['execution_id'] != abandoned_owner['execution_id']
        assert storage.get_execution_session(owner['session_id'])['outcome'] == 'done'
        assert storage.get_component_reservation(('A',)) is None
    finally:
        release_decision.set()
        thread.join(timeout=20)
        _close(storage)


@pytest.mark.parametrize('order', ['restart_first', 'terminal_first'])
def test_direct_workflow_continues_without_replaying_completed_nodes(tmp_path, monkeypatch, order):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('P', 'A'), ('A', 'C')])
    storage = workflow.storage
    calls, result = [], {}
    at_decision, release_decision = Event(), Event()

    def work(ctx):
        calls.append((ctx.current_node, ctx.job_id, ctx.execution_generation))
        if ctx.current_node == 'A' and ctx.job_id == 1 and ctx.execution_generation == 0:
            raise ValueError('direct workflow attempt fails')
        return ctx.current_node

    for name in ('P', 'A', 'C'):
        router = NodeRouter(name, runner='direct')
        router.task(work)
        workflow.include_routers(router)
        workflow.add_job(None, name)
    workflow.add_job(None, 'A')
    decide = storage.decide_execution_session_exit

    def pause_failed_decision(*args, **kwargs):
        if kwargs.get('outcome') == 'failed' and not at_decision.is_set():
            at_decision.set()
            assert release_decision.wait(30)
        return decide(*args, **kwargs)

    monkeypatch.setattr(storage, 'decide_execution_session_exit', pause_failed_decision)

    def run():
        try:
            result['value'] = workflow.run()
        except BaseException as error:
            result['error'] = error

    thread = Thread(target=run, name='direct-dag-continuation', daemon=True)
    thread.start()
    try:
        assert at_decision.wait(25), result
        assert calls == [('P', 1, 0), ('A', 1, 0)]
        owner = storage.read_job_current_owner('A', 1)
        peer_output = storage.output_file('P', 1).read_bytes()
        peer_events = storage.read_job_events('P', 1)
        if order == 'terminal_first':
            release_decision.set()
            thread.join(timeout=20)
            assert not thread.is_alive(), result
        command = subprocess.run(
            [sys.executable, '-m', 'micro_workflow_manager', 'restart', 'A', 'job', '1'],
            cwd=tmp_path, env=dict(os.environ, PYTHONPATH=str(Path(__file__).resolve().parents[1])),
            capture_output=True, text=True, timeout=20,
        )
        if order == 'terminal_first':
            assert command.returncode == 1, command.stdout + command.stderr
            assert 'error' in result
            assert storage.current_job_generation('A', 1) == 0
            assert storage.get_job_status('A', 2) == storage.get_job_status('C', 1) == 'queued'
        else:
            assert command.returncode == 0, command.stdout + command.stderr
            release_decision.set()
            thread.join(timeout=20)
            assert not thread.is_alive(), result
            assert 'error' not in result, result
            assert result['value'] == ['P', 'A', 'C']
            assert calls == [
                ('P', 1, 0), ('A', 1, 0), ('A', 1, 1), ('A', 2, 0), ('C', 1, 0),
            ]
            assert storage.read_job_current_owner('A', 1)['session_id'] == owner['session_id']
            assert storage.read_job_current_owner('C', 1)['session_id'] == owner['session_id']
        assert storage.output_file('P', 1).read_bytes() == peer_output
        assert storage.read_job_events('P', 1) == peer_events
        sessions = storage.list_execution_sessions()
        assert len(sessions) == 1 and sessions[0]['status'] == 'terminal'
        assert sessions[0]['outcome'] == ('done' if order == 'restart_first' else 'failed')
        for component in workflow.execution_components():
            assert storage.get_component_reservation(component) is None
    finally:
        release_decision.set()
        thread.join(timeout=20)
        assert not thread.is_alive(), result
        _close(storage)


@pytest.mark.parametrize('fault', ['submission', 'readiness', 'submission_cleanup'])
def test_dag_submission_failure_joins_and_publishes_started_component_outcomes(tmp_path, monkeypatch, fault):
    from micro_workflow_manager.workflow import dag_scheduler

    workflow = MicroWorkflow(tmp_path, runner='threaded', persist_graph=False)
    workflow.graph([('A', 'D'), ('B', 'E'), ('C', 'F')])
    storage = workflow.storage
    a, b, c = (NodeRouter(name, runner='threaded', max_threads=1) for name in ('A', 'B', 'C'))
    started, release = Event(), Event()
    calls = []
    infrastructure_error = OSError('DAG worker submission failed')

    @a.task
    def failing(ctx):
        calls.append(('A', ctx.job_id))
        started.set()
        assert release.wait(20)
        raise ValueError('already started A fails while the scheduler unwinds')

    @b.task
    def completed_peer(ctx):
        calls.append(('B', ctx.job_id))
        return 'completed peer survives scheduler failure'

    @c.task
    def unsubmitted(ctx):
        calls.append(('C', ctx.job_id))
        return 'must remain queued'

    workflow.include_routers(a, b, c)
    for name in ('A', 'B', 'C', 'A'):
        workflow.add_job(None, name)
    executor_type = dag_scheduler.ThreadPoolExecutor

    class FailingSubmissionExecutor(executor_type):
        def submit(self, function, *args, **kwargs):
            self.submitted = getattr(self, 'submitted', 0) + 1
            if fault.startswith('submission') and self.submitted == 3:
                assert started.wait(20)
                wait_until(lambda: storage.get_job_status('B', 1) == 'done', timeout=20)
                release.set()
                raise infrastructure_error
            return super().submit(function, *args, **kwargs)

    monkeypatch.setattr(dag_scheduler, 'ThreadPoolExecutor', FailingSubmissionExecutor)
    restore_admission = workflow.set_active_api_admission_nodes

    def fail_final_restoration(nodes):
        restore_admission(nodes)
        if fault == 'submission_cleanup' and release.is_set():
            raise OSError('secondary admission restoration failure')

    monkeypatch.setattr(workflow, 'set_active_api_admission_nodes', fail_final_restoration)

    def ready_check(name):
        if fault == 'readiness' and name == 'C':
            if storage.get_job_status('B', 1) != 'done':
                return False
            assert started.wait(20)
            release.set()
            raise infrastructure_error
        return workflow.node_ready(name)

    try:
        with pytest.raises(OSError) as failure:
            workflow.run_concurrently(nodes=['A', 'B', 'C'], ready_check=ready_check)
        assert failure.value is infrastructure_error
        assert sorted(calls) == [('A', 1), ('B', 1)]
        assert storage.get_job_status('A', 1) == 'failed'
        assert storage.get_job_status('B', 1) == 'done'
        assert storage.get_job_status('A', 2) == storage.get_job_status('C', 1) == 'queued'
        assert storage.read_job_current_owner('A', 2) is None
        assert storage.read_job_current_owner('C', 1) is None
        assert storage.get_node_status('A') == 'failed'
        assert storage.get_node_status('B') == 'done'
        sessions = storage.list_execution_sessions()
        assert len(sessions) == 1
        assert sessions[0]['status'] == 'terminal'
        assert sessions[0]['outcome'] == 'failed'
        for name in ('A', 'B', 'C'):
            assert storage.get_component_reservation((name,)) is None
            assert storage.list_jobs(name, status='running') == []
        assert storage.read_job_current_owner('A', 1)['session_id'] == sessions[0]['session_id']
        assert storage.read_job_current_owner('B', 1)['session_id'] == sessions[0]['session_id']
    finally:
        release.set()
        _close(storage)


@pytest.mark.parametrize('refuse_after', [False, True])
@pytest.mark.parametrize('runner', ['threaded', 'direct', 'process', 'direct_process'])
def test_cli_runfrom_continues_the_same_selection_and_reaches_descendants(tmp_path, refuse_after, runner):
    global_runner, node_runner = ('direct', 'process') if runner == 'direct_process' else (runner, runner)
    source = tmp_path / 'src'
    behaviors = source / 'node_behavior'
    behaviors.mkdir(parents=True)
    edges = [('A', 'B'), ('B', 'A'), ('B', 'C')] if runner == 'threaded' else [('B', 'A'), ('A', 'C')]
    (source / 'graph.py').write_text(f'''
import time
from pathlib import Path
from micro_workflow_manager.storage import FileStorage
EDGES = {edges!r}
root = Path(__file__).resolve().parent.parent
original = FileStorage.decide_execution_session_exit

def pause_failed_decision(storage, *args, **kwargs):
    checked, release = root / 'dag-exit-checked', root / 'dag-exit-release'
    if kwargs.get('outcome') == 'failed' and not checked.exists():
        checked.write_text('checked', encoding='utf-8')
        deadline = time.monotonic() + 40
        while not release.exists():
            assert time.monotonic() < deadline
            time.sleep(0.01)
    return original(storage, *args, **kwargs)
FileStorage.decide_execution_session_exit = pause_failed_decision
''', encoding='utf-8')
    (behaviors / 'A.py').write_text('''
import time
from micro_workflow_manager import NodeRouter
router = NodeRouter('A', runner='threaded', max_threads=1)
router.create_job(number=2, params={})
@router.task
def work(ctx):
    if ctx.job_id == 1 and ctx.execution_generation == 0:
        deadline = time.monotonic() + 20
        while ctx.system.storage.get_job_status('B', 1) != 'done':
            assert time.monotonic() < deadline
            time.sleep(0.01)
        raise ValueError('CLI component attempt failed')
    return (ctx.job_id, ctx.execution_generation)
'''.replace("runner='threaded'", f'runner={node_runner!r}'), encoding='utf-8')
    for name in ('B', 'C'):
        (behaviors / f'{name}.py').write_text(
            'from micro_workflow_manager import NodeRouter\n'
            f'router = NodeRouter({name!r}, runner={node_runner!r})\n'
            'router.create_job(params={})\n'
            '@router.task\n'
            'def work(ctx):\n'
            '    return (ctx.current_node, ctx.execution_generation)\n', encoding='utf-8',
        )
    environment = dict(os.environ, PYTHONPATH=str(Path(__file__).resolve().parents[1]))
    command_prefix = [sys.executable, '-m', 'micro_workflow_manager']
    for args in (['init'], ['graph', 'src/graph.py', '--runner', global_runner]):
        setup = subprocess.run(command_prefix + args, cwd=tmp_path, env=environment,
                               capture_output=True, text=True, timeout=25)
        assert setup.returncode == 0, setup.stdout + setup.stderr
    storage = FileStorage(tmp_path)
    arguments = ['runfrom', 'A' if runner == 'threaded' else 'B'] + (
        ['refuseafter', 'B' if runner == 'threaded' else 'A'] if refuse_after else []
    )
    process = subprocess.Popen(command_prefix + arguments, cwd=tmp_path, env=environment,
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    release = tmp_path / 'dag-exit-release'
    try:
        wait_until(lambda: (tmp_path / 'dag-exit-checked').exists() or process.poll() is not None, timeout=25)
        assert process.poll() is None, process.communicate(timeout=5)
        owner = storage.read_job_current_owner('A', 1)
        session = storage.get_execution_session(owner['session_id'])
        assert session['status'] == 'running'
        assert session['selected_components'] == ([('A', 'B'), ('C',)] if runner == 'threaded' else [('B',), ('A',), ('C',)])
        assert storage.get_job_status('A', 1) == 'failed'
        assert storage.get_job_status('A', 2) == storage.get_job_status('C', 1) == 'queued'
        assert storage.get_job_status('B', 1) == 'done'
        peer_output = storage.output_file('B', 1).read_bytes()
        peer_events = storage.read_job_events('B', 1)
        restart = subprocess.run(command_prefix + ['restart', 'A', 'job', '1'],
                                 cwd=tmp_path, env=environment, capture_output=True, text=True, timeout=25)
        assert restart.returncode == 0, restart.stdout + restart.stderr
        release.write_text('release', encoding='utf-8')
        stdout, stderr = process.communicate(timeout=25)
        assert process.returncode == 0, stdout + stderr
        assert stdout.count('Ran:\n') == 1
        assert sorted(stdout.split('Ran:\n')[1].split()) == (['A', 'B'] if refuse_after else ['A', 'B', 'C'])
        assert storage.output_file('B', 1).read_bytes() == peer_output
        assert storage.read_job_events('B', 1) == peer_events
        for name, job_id in (('A', 1), ('A', 2), ('B', 1)):
            assert storage.get_job_status(name, job_id) == 'done'
            assert storage.read_job_current_owner(name, job_id)['session_id'] == owner['session_id']
        if refuse_after:
            assert storage.get_job_status('C', 1) == 'queued'
            assert storage.read_job_current_owner('C', 1) is None
            assert 'Refused further Hoeflein-component admission' in stdout
        else:
            assert storage.get_job_status('C', 1) == 'done'
            assert storage.read_job_current_owner('C', 1)['session_id'] == owner['session_id']
        replacement = storage.read_job_current_owner('A', 1)
        assert replacement['generation'] == owner['generation'] + 1
        assert replacement['job_instance_id'] == owner['job_instance_id']
        sessions = storage.list_execution_sessions()
        assert len(sessions) == 1 and sessions[0]['outcome'] == 'done'
        assert sessions[0]['selected_components'] == session['selected_components']
        assert sessions[0]['details'] == session['details']
        for component in session['selected_components']:
            assert storage.get_component_reservation(component) is None
    finally:
        release.write_text('release', encoding='utf-8')
        if process.poll() is None:
            try:
                process.communicate(timeout=25)
            except subprocess.TimeoutExpired:
                process.kill()
                process.communicate(timeout=10)
                raise
        _close(storage)


@pytest.mark.parametrize('entry, order', [
    (entry, order)
    for entry in ('run_component', 'run_node', 'run')
    for order in ('restart_first', 'terminal_first', 'restart_superseded', 'cancelled_after_decision',
                  'setup_error_after_decision', 'persistent_setup_failure', 'later_request_after_setup_failure')
] + [('run_independent', 'restart_first'), ('run_concurrently', 'restart_first'),
     ('api_component', 'restart_first'), ('api_component', 'terminal_first'),
     ('api_run', 'restart_first'), ('api_run', 'terminal_first')])
def test_component_exit_retains_accepted_restart_and_completed_peer(tmp_path, monkeypatch, order, entry):
    runner = 'api' if entry.startswith('api_') else 'threaded'
    workflow = MicroWorkflow(tmp_path, runner=runner, persist_graph=False)
    is_dag = entry in {'run', 'run_independent', 'run_concurrently', 'api_run'}
    independent_branch = entry in {'run_independent', 'api_run'}
    workflow.graph([('A', 'B'), ('B', 'A')] + ([('B', 'C')] if is_dag else [])
                   + ([('X', 'X')] if independent_branch else []))
    a, b = NodeRouter('A', runner=runner, max_threads=1), NodeRouter('B', runner=runner)
    starts, result = [], {}
    ordinary_starts = []
    at_decision, release_decision = Event(), Event()
    storage = workflow.storage
    ready_checks = []

    @a.task
    def first(ctx):
        if ctx.job_id == 2:
            ordinary_starts.append(ctx.execution_generation)
            return 'ordinary work after every component failure is repaired'
        starts.append(('A', ctx.execution_generation))
        if ctx.execution_generation == 0:
            wait_until(lambda: storage.get_job_status('B', 1) == 'done', timeout=20)
            if independent_branch:
                wait_until(lambda: storage.get_job_status('X', 1) == 'done', timeout=20)
            raise ValueError('component member failed before exit')
        return 'replacement completed'

    @b.task
    def peer(ctx):
        starts.append(('B', ctx.execution_generation))
        print('peer output must be retained')
        return 'peer completed'

    workflow.include_routers(a, b)
    if is_dag:
        c = NodeRouter('C', runner=runner)

        @c.task
        def descendant(ctx):
            if entry == 'run_concurrently':
                assert ('C', 1) in ready_checks
            starts.append(('C', ctx.execution_generation))
            return 'descendant after component recovery'

        workflow.include_routers(c)
        workflow.add_job(None, 'C')
    if independent_branch:
        x = NodeRouter('X', runner=runner)

        @x.task
        def independent(ctx):
            starts.append(('X', ctx.execution_generation))
            return 'independent completed component'

        workflow.include_routers(x)
        workflow.add_job(None, 'X')
    workflow.add_job(None, 'A')
    workflow.add_job(None, 'B')
    workflow.add_job(None, 'A')
    decide = storage.decide_execution_session_exit
    claim_job = storage.claim_job_execution
    superseded = []
    cancelled_state = []
    environment = dict(os.environ, PYTHONPATH=str(Path(__file__).resolve().parents[1]))
    make_runner = workflow.make_runner
    preparation_failed = []
    release_setup_failure = Event()
    later_request = []
    infrastructure_error = OSError('one-shot replacement source preparation failed')

    def fail_replacement_preparation(node, **kwargs):
        if (order in {'setup_error_after_decision', 'persistent_setup_failure', 'later_request_after_setup_failure'} and node.name == 'A'
                and storage.current_job_generation('A', 1) == 1
                and (not preparation_failed or (order in {'persistent_setup_failure', 'later_request_after_setup_failure'} and not release_setup_failure.is_set()))):
            preparation_failed.append(True)
            raise infrastructure_error
        return make_runner(node, **kwargs)

    monkeypatch.setattr(workflow, 'make_runner', fail_replacement_preparation)

    def supersede_before_claim(node, job_id, **kwargs):
        expected = kwargs.get('expected_restart')
        if (order == 'restart_superseded' and node == 'A' and job_id == 1
                and expected is not None and expected['generation'] == 1 and not superseded):
            superseded.append(True)
            workflow.cancel_job('A', 1)
            newer = subprocess.run(
                [sys.executable, '-m', 'micro_workflow_manager', 'restart', 'A', 'job', '1'],
                cwd=tmp_path, env=environment, capture_output=True, text=True, timeout=20,
            )
            assert newer.returncode == 0, newer.stdout + newer.stderr
        return claim_job(node, job_id, **kwargs)

    monkeypatch.setattr(storage, 'claim_job_execution', supersede_before_claim)

    def pause_failed_decision(*args, **kwargs):
        if kwargs.get('outcome') == 'failed' and not at_decision.is_set():
            at_decision.set()
            assert release_decision.wait(30)
        if (order == 'later_request_after_setup_failure' and kwargs.get('exhausted_restarts')
                and not later_request):
            later_request.append(True)
            workflow.cancel_job('A', 1)
            newer = subprocess.run(
                [sys.executable, '-m', 'micro_workflow_manager', 'restart', 'A', 'job', '1'],
                cwd=tmp_path, env=environment, capture_output=True, text=True, timeout=20,
            )
            assert newer.returncode == 0, newer.stdout + newer.stderr
        decision = decide(*args, **kwargs)
        if order == 'cancelled_after_decision' and decision['restarts'] and not cancelled_state:
            workflow.cancel_job('A', 1)
            cancelled_state.append((storage.read_job_control('A', 1), storage.read_job_events('A', 1),
                                    _snapshot(storage)[1]))
        return decision

    monkeypatch.setattr(storage, 'decide_execution_session_exit', pause_failed_decision)

    def run():
        try:
            if entry in {'run_component', 'api_component'}:
                result['value'] = workflow.run_component({'A', 'B'})
            elif entry == 'run_node':
                result['value'] = workflow.run_node('B')
            elif entry == 'run_concurrently':
                def ready_check(node):
                    ready_checks.append((node, storage.current_job_generation('A', 1)))
                    return workflow.node_ready(node)
                result['value'] = workflow.run_concurrently(ready_check=ready_check)
            else:
                result['value'] = workflow.run()
        except BaseException as error:
            result['error'] = error

    thread = Thread(target=run, name='native-component-exit', daemon=True)
    thread.start()
    try:
        assert at_decision.wait(25), result
        assert storage.get_job_status('A', 1) == 'failed'
        assert storage.get_job_status('B', 1) == 'done'
        assert storage.get_job_status('A', 2) == 'queued'
        assert storage.read_job_current_owner('A', 2) is None
        assert ordinary_starts == []
        if is_dag:
            assert storage.get_job_status('C', 1) == 'queued'
            assert storage.read_job_current_owner('C', 1) is None
        owner = storage.read_job_current_owner('A', 1)
        assert storage.get_execution_session(owner['session_id'])['status'] == 'running'
        assert storage.get_component_reservation(('A', 'B'))['session_id'] == owner['session_id']
        peer_output = storage.output_file('B', 1).read_bytes()
        peer_events = storage.read_job_events('B', 1)
        if independent_branch:
            independent_before = storage.output_file('X', 1).read_bytes(), storage.read_job_events('X', 1)
        if order == 'terminal_first':
            release_decision.set()
            thread.join(timeout=20)
            assert not thread.is_alive(), result
            assert storage.get_execution_session(owner['session_id'])['outcome'] == 'failed'
        before = _snapshot(storage)
        command = subprocess.run(
            [sys.executable, '-m', 'micro_workflow_manager', 'restart', 'A', 'job', '1'],
            cwd=tmp_path,
            env=environment,
            capture_output=True, text=True, timeout=20,
        )
        if order == 'terminal_first':
            assert command.returncode == 1, command.stdout + command.stderr
            assert _snapshot(storage) == before
            assert 'error' in result
            assert sorted(starts) == [('A', 0), ('B', 0)] + ([('X', 0)] if independent_branch else [])
            assert ordinary_starts == []
            if is_dag:
                assert storage.get_job_status('C', 1) == 'queued'
            return
        assert command.returncode == 0, command.stdout + command.stderr
        assert storage.current_job_generation('A', 1) == 1
        assert storage.get_execution_session(owner['session_id'])['status'] == 'running'
        assert storage.get_component_reservation(('A', 'B'))['session_id'] == owner['session_id']
        release_decision.set()
        thread.join(timeout=20)
        assert not thread.is_alive(), result
        if order == 'later_request_after_setup_failure':
            assert later_request == [True]
            assert result.get('error') is infrastructure_error, result
            assert sorted(starts) == [('A', 0), ('A', 2), ('B', 0)]
            assert ordinary_starts == []
            assert storage.get_job_status('A', 1) == 'done'
            assert storage.get_job_status('A', 2) == 'queued'
            assert storage.read_job_current_owner('A', 1)['session_id'] == owner['session_id']
            assert storage.read_job_current_owner('A', 1)['generation'] == 2
            assert storage.output_file('B', 1).read_bytes() == peer_output
            assert storage.read_job_events('B', 1) == peer_events
            assert storage.get_execution_session(owner['session_id'])['outcome'] == 'failed'
            assert storage.get_component_reservation(('A', 'B')) is None
            if is_dag:
                assert storage.get_job_status('C', 1) == 'queued'
            return
        if order == 'persistent_setup_failure':
            assert preparation_failed
            assert result.get('error') is infrastructure_error, result
            assert sorted(starts) == [('A', 0), ('B', 0)]
            assert ordinary_starts == []
            assert storage.get_job_status('A', 1) == storage.get_job_status('A', 2) == 'queued'
            assert storage.current_job_generation('A', 1) == 1
            assert storage.read_job_current_owner('A', 1) == owner
            assert storage.output_file('B', 1).read_bytes() == peer_output
            assert storage.read_job_events('B', 1) == peer_events
            assert storage.get_execution_session(owner['session_id'])['outcome'] == 'failed'
            assert storage.get_component_reservation(('A', 'B')) is None
            if is_dag:
                assert storage.get_job_status('C', 1) == 'queued'
            return
        if order == 'setup_error_after_decision':
            assert preparation_failed == [True]
            assert result.get('error') is infrastructure_error, result
            assert sorted(starts) == [('A', 0), ('A', 1), ('B', 0)]
            assert storage.get_job_status('A', 1) == 'done'
            assert storage.get_job_status('A', 2) == 'queued'
            assert ordinary_starts == []
            assert storage.read_job_current_owner('A', 2) is None
            assert storage.read_job_current_owner('A', 1)['session_id'] == owner['session_id']
            assert storage.output_file('B', 1).read_bytes() == peer_output
            assert storage.read_job_events('B', 1) == peer_events
            assert storage.get_execution_session(owner['session_id'])['outcome'] == 'failed'
            assert storage.get_component_reservation(('A', 'B')) is None
            return
        if order == 'cancelled_after_decision':
            assert 'error' in result, result
            assert sorted(starts) == [('A', 0), ('B', 0)]
            assert ordinary_starts == []
            assert storage.get_job_status('A', 2) == 'queued'
            assert storage.get_job_status('A', 1) == 'cancelled'
            assert (storage.read_job_control('A', 1), storage.read_job_events('A', 1),
                    _snapshot(storage)[1]) == cancelled_state[0]
            assert storage.get_execution_session(owner['session_id'])['outcome'] == 'failed'
            assert storage.get_component_reservation(('A', 'B')) is None
            if is_dag:
                assert storage.get_job_status('C', 1) == 'queued'
            return
        assert 'error' not in result, result
        assert sorted(result['value']) == (['A', 'B', 'C'] if is_dag else ['A', 'B']) + (
            ['X'] if independent_branch else []
        )
        generation = 2 if order == 'restart_superseded' else 1
        assert bool(superseded) == (order == 'restart_superseded')
        assert sorted(starts) == [('A', 0), ('A', generation), ('B', 0)] + (
            [('C', 0)] if is_dag else []
        ) + ([('X', 0)] if independent_branch else [])
        if independent_branch:
            assert (storage.output_file('X', 1).read_bytes(), storage.read_job_events('X', 1)) == independent_before
            assert storage.read_job_current_owner('X', 1)['session_id'] == owner['session_id']
            assert storage.get_component_reservation(('X',)) is None
        assert storage.output_file('B', 1).read_bytes() == peer_output
        assert storage.read_job_events('B', 1) == peer_events
        replacement = storage.read_job_current_owner('A', 1)
        assert replacement['job_instance_id'] == owner['job_instance_id']
        assert replacement['session_id'] == owner['session_id']
        assert replacement['generation'] == generation
        sessions = storage.list_execution_sessions()
        assert len(sessions) == 1
        assert sessions[0]['status'] == 'terminal'
        assert sessions[0]['outcome'] == 'done'
        assert storage.get_component_reservation(('A', 'B')) is None
        assert storage.get_node_status('A') == storage.get_node_status('B') == 'done'
        assert ordinary_starts == [0]
        assert storage.get_job_status('A', 2) == 'done'
        assert storage.read_job_current_owner('A', 2)['session_id'] == owner['session_id']
        if is_dag:
            assert storage.get_job_status('C', 1) == 'done'
            assert storage.read_job_current_owner('C', 1)['session_id'] == owner['session_id']
            assert storage.get_component_reservation(('C',)) is None
    finally:
        release_setup_failure.set()
        release_decision.set()
        thread.join(timeout=20)
        assert not thread.is_alive(), result
        _close(storage)


@pytest.mark.parametrize('entry', ['run_component', 'run'])
def test_partial_component_restart_runs_only_accepted_successor_before_remaining_failure(tmp_path, monkeypatch, entry):
    workflow = MicroWorkflow(tmp_path, runner='threaded', persist_graph=False)
    workflow.graph([('A', 'B'), ('B', 'A')] if entry == 'run_component' else [('A', 'C'), ('B', 'D')])
    a, b = NodeRouter('A', runner='threaded', max_threads=1), NodeRouter('B', runner='threaded', max_threads=1)
    started = Barrier(2)
    at_decision, release_decision = Event(), Event()
    calls, result = [], {}
    storage = workflow.storage

    @a.task
    def first(ctx):
        calls.append(('A', ctx.job_id, ctx.execution_generation))
        if ctx.job_id == 1 and ctx.execution_generation == 0:
            started.wait(timeout=20)
            raise ValueError('A first attempt fails')
        return 'accepted A successor'

    @b.task
    def second(ctx):
        calls.append(('B', ctx.job_id, ctx.execution_generation))
        if ctx.job_id == 1:
            started.wait(timeout=20)
            raise ValueError('B remains failed')
        return 'must stay queued'

    workflow.include_routers(a, b)
    if entry == 'run':
        for name in ('C', 'D'):
            router = NodeRouter(name, runner='threaded')

            @router.task
            def descendant(ctx):
                calls.append((ctx.current_node, ctx.job_id, ctx.execution_generation))
                return 'must stay queued after partial repair'

            workflow.include_routers(router)
            workflow.add_job(None, name)
    for node in ('A', 'B'):
        workflow.add_job(None, node)
        workflow.add_job(None, node)
    untouched = {node: (storage.read_job_control(node, 2), storage.read_job_events(node, 2))
                 for node in ('A', 'B')}
    decide = storage.decide_execution_session_exit

    def pause_failed_decision(*args, **kwargs):
        if kwargs.get('outcome') == 'failed' and not at_decision.is_set():
            at_decision.set()
            assert release_decision.wait(30)
        return decide(*args, **kwargs)

    monkeypatch.setattr(storage, 'decide_execution_session_exit', pause_failed_decision)

    def run():
        try:
            result['value'] = workflow.run_component({'A', 'B'}) if entry == 'run_component' else workflow.run()
        except BaseException as error:
            result['error'] = error

    thread = Thread(target=run, name='partial-native-component-restart', daemon=True)
    thread.start()
    try:
        assert at_decision.wait(25), result
        assert all(storage.get_job_status(node, 1) == 'failed' for node in ('A', 'B'))
        owner = storage.read_job_current_owner('A', 1)
        b_before = storage.read_job_current_owner('B', 1), storage.read_job_events('B', 1)
        command = subprocess.run(
            [sys.executable, '-m', 'micro_workflow_manager', 'restart', 'A', 'job', '1'],
            cwd=tmp_path,
            env=dict(os.environ, PYTHONPATH=str(Path(__file__).resolve().parents[1])),
            capture_output=True, text=True, timeout=20,
        )
        assert command.returncode == 0, command.stdout + command.stderr
        release_decision.set()
        thread.join(timeout=20)
        assert not thread.is_alive(), result
        assert 'error' in result and 'Job B/1 failed' in str(result['error']), result
        assert sorted(calls) == [('A', 1, 0), ('A', 1, 1), ('B', 1, 0)]
        assert storage.get_job_status('A', 1) == 'done'
        assert storage.get_job_status('B', 1) == 'failed'
        assert (storage.read_job_current_owner('B', 1), storage.read_job_events('B', 1)) == b_before
        for node in ('A', 'B'):
            assert storage.get_job_status(node, 2) == 'queued'
            assert storage.read_job_current_owner(node, 2) is None
            assert (storage.read_job_control(node, 2), storage.read_job_events(node, 2)) == untouched[node]
            assert storage.get_node_status(node) == ('queued' if entry == 'run' and node == 'A' else 'failed')
        assert storage.read_job_current_owner('A', 1)['session_id'] == owner['session_id']
        for component in workflow.execution_components():
            assert storage.get_component_reservation(component) is None
        if entry == 'run':
            for node in ('C', 'D'):
                assert storage.get_job_status(node, 1) == 'queued'
                assert storage.read_job_current_owner(node, 1) is None
        sessions = storage.list_execution_sessions()
        assert len(sessions) == 1 and sessions[0]['outcome'] == 'failed'
    finally:
        release_decision.set()
        thread.join(timeout=20)
        assert not thread.is_alive(), result
        _close(storage)
