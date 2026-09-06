from __future__ import annotations

import os
import socket
import time

import pytest

from micro_workflow_manager import MicroWorkflow
from micro_workflow_manager.cli.inspect import inspect_command
from micro_workflow_manager.models import Job, now
from micro_workflow_manager.processes import process_identity


def _close(storage):
    storage.db_mutation_barrier()
    deadline = time.monotonic() + 10
    while storage.mutation_writer_diagnostics()['writer_alive']:
        assert time.monotonic() < deadline
        time.sleep(0.01)
    storage.close_database_connections()


@pytest.fixture
def workflow(tmp_path):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'B'), ('B', 'A'), ('B', 'C')])
    storage = workflow.storage
    storage.register_component_topology(workflow.topology.snapshot())
    storage.create_job(Job(node_name='A', job_id=1, params={'value': 'retained input'}))
    for session_id, kind, component in [
        ('current-main', 'main', ('C',)), ('job-interrupt', 'interrupt', ('A', 'B')),
    ]:
        storage.create_execution_session(
            session_id, session_kind=kind, command='run', start_component=component,
            selected_components=[component], started_at=now(), hostname=socket.gethostname(),
            pid=os.getpid(), process_identity=process_identity(os.getpid()),
        )
        storage.reserve_execution_components(session_id, expected_shape=workflow.topology.snapshot().shape_json)
    try:
        yield workflow
    finally:
        _close(storage)


@pytest.mark.parametrize('state', ['active', 'last', 'unclaimed'])
def test_job_inspection_reports_exact_native_owner_independently_of_trace(workflow, state, monkeypatch, capsys):
    storage = workflow.storage
    execution_id = None
    if state != 'unclaimed':
        generation, execution_id = storage.claim_job_execution(
            'A', 1, started_at=now(), session_id='job-interrupt', component=('A', 'B'),
        )
        if state == 'last':
            storage.finalize_job_execution('A', 1, generation, execution_id, 'done')
            storage.finish_execution_session('job-interrupt', outcome='done', finished_at=now())
            storage.release_execution_components('job-interrupt')
    storage.clear_job_events('A', [1])
    before = list(storage.db_connection().iterdump())
    instance = storage.read_job_instance_id('A', 1)

    def obsolete_reader():
        raise AssertionError('Job inspection guessed a singleton owner')

    monkeypatch.setattr(storage, 'get_run_state', obsolete_reader)
    monkeypatch.setattr(storage, 'get_live_main_session', obsolete_reader)
    monkeypatch.setattr(storage, 'get_live_execution_session', obsolete_reader)
    assert inspect_command(workflow, 'A', 1) == 0
    output = capsys.readouterr().out
    assert f'job instance: {instance}' in output
    assert 'retained input' in output
    assert 'current-main' not in output
    if state == 'unclaimed':
        assert 'execution ownership: unclaimed' in output
        assert 'job-interrupt' not in output
    else:
        assert f'execution ownership: {state}' in output
        assert f'execution ID: {execution_id}' in output
        assert 'session ID: job-interrupt' in output
        assert 'session kind: interrupt' in output
        assert 'owner component: A, B' in output
        assert 'claimed generation: 0' in output
        assert ('session status: running' if state == 'active' else 'session status: terminal') in output
        if state == 'last':
            assert 'session outcome: done' in output
    assert list(storage.db_connection().iterdump()) == before
    assert storage.read_job_events('A', 1) == []


@pytest.mark.parametrize('damage', ['missing-owner', 'missing-session', 'wrong-instance', 'active-disagreement', 'wrong-session-scope'])
def test_job_inspection_refuses_damaged_ownership_before_printing(workflow, damage, capsys):
    storage = workflow.storage
    _, execution_id = storage.claim_job_execution(
        'A', 1, started_at=now(), session_id='job-interrupt', component=('A', 'B'),
    )
    connection = storage.db_connection()
    connection.execute('PRAGMA foreign_keys=OFF')
    try:
        if damage == 'missing-owner':
            connection.execute('DELETE FROM job_execution_owners WHERE execution_id=?', (execution_id,))
        elif damage == 'missing-session':
            connection.execute("DELETE FROM execution_sessions WHERE session_id='job-interrupt'")
        elif damage == 'wrong-instance':
            connection.execute('UPDATE job_execution_owners SET job_instance_id=? WHERE execution_id=?',
                               ('0' * 32, execution_id))
        elif damage == 'wrong-session-scope':
            connection.execute("UPDATE job_execution_owners SET session_id='current-main' WHERE execution_id=?",
                               (execution_id,))
        else:
            connection.execute("UPDATE job_instances SET last_execution_id=NULL WHERE node_name='A' AND job_id=1")
    finally:
        connection.execute('PRAGMA foreign_keys=ON')
    before = list(connection.iterdump())
    with pytest.raises(RuntimeError, match='ownership'):
        inspect_command(workflow, 'A', 1)
    assert capsys.readouterr().out == ''
    assert list(connection.iterdump()) == before


def test_job_inspection_refuses_a_replacement_between_owner_and_payload_reads(workflow, monkeypatch, capsys):
    storage = workflow.storage
    original_load = storage.load_job
    replacement_state = []

    def replace_then_load(node, job_id):
        storage.delete_job(node, job_id)
        storage.create_job(Job(node_name=node, job_id=job_id, params={'value': 'replacement'}))
        replacement_state.extend(storage.db_connection().iterdump())
        return original_load(node, job_id)

    monkeypatch.setattr(storage, 'load_job', replace_then_load)
    with pytest.raises(RuntimeError, match='replaced during inspection'):
        inspect_command(workflow, 'A', 1)
    assert capsys.readouterr().out == ''
    assert list(storage.db_connection().iterdump()) == replacement_state


def test_actual_inspect_cli_reports_native_owner_and_preserves_stored_state(tmp_path, monkeypatch, capsys):
    from micro_workflow_manager import cli
    from micro_workflow_manager.cli.project import load_workflow

    monkeypatch.chdir(tmp_path)
    behavior = tmp_path / 'src' / 'node_behavior'
    behavior.mkdir(parents=True)
    (tmp_path / 'src' / 'graph.py').write_text("EDGES=[('A','B')]\n", encoding='utf-8')
    for node in ('A', 'B'):
        (behavior / f'{node}.py').write_text(
            'from micro_workflow_manager import NodeRouter\n'
            f'router=NodeRouter({node!r})\n'
            '@router.task\n'
            'def run(ctx): return None\n', encoding='utf-8',
        )
    assert cli.main(['init']) == 0
    assert cli.main(['graph', 'src/graph.py']) == 0
    workflow = load_workflow(tmp_path)
    storage = workflow.storage
    try:
        storage.register_component_topology(workflow.topology.snapshot())
        storage.create_job(Job(node_name='A', job_id=1, params={'value': 'CLI input'}))
        storage.create_execution_session(
            'cli-owner', session_kind='interrupt', command='run', start_component=('A',),
            selected_components=[('A',)], started_at=now(), hostname=socket.gethostname(),
            pid=os.getpid(), process_identity=process_identity(os.getpid()),
        )
        storage.reserve_execution_components('cli-owner', expected_shape=workflow.topology.snapshot().shape_json)
        generation, execution_id = storage.claim_job_execution(
            'A', 1, started_at=now(), session_id='cli-owner', component=('A',),
        )
        storage.finalize_job_execution('A', 1, generation, execution_id, 'done')
        storage.finish_execution_session('cli-owner', outcome='done', finished_at=now())
        storage.release_execution_components('cli-owner')
        storage.clear_job_events('A', [1])
        before = list(storage.db_connection().iterdump())
        capsys.readouterr()
        assert cli.main(['inspect', 'A', 'job', '1']) == 0
        output = capsys.readouterr().out
        assert 'session ID: cli-owner' in output
        assert f'execution ID: {execution_id}' in output
        assert 'execution ownership: last' in output
        assert 'session status: terminal' in output
        assert 'CLI input' in output
        assert list(storage.db_connection().iterdump()) == before
        assert not (tmp_path / '.mwf' / 'run.json').exists()
    finally:
        _close(storage)


@pytest.mark.parametrize('condition,live,reason', [
    ('local-live', True, 'the recorded process instance is alive'),
    ('missing-job-metadata', True, 'the recorded process instance is alive'),
    ('local-unidentified', True, 'the recorded PID is alive and its heartbeat is not stale'),
    ('local-unidentified-stale', False, 'the session heartbeat is stale; PID existence alone is ambiguous'),
    ('local-reused', False, 'the recorded PID belongs to a different process instance'),
    ('foreign-live', True, 'the run belongs to another host and its heartbeat is fresh'),
    ('foreign-stale', False, 'the other host heartbeat is stale'),
    ('terminal', False, 'no running sequence is recorded'),
])
def test_job_inspection_separates_persisted_session_status_from_liveness(
    workflow, condition, live, reason, monkeypatch, capsys,
):
    storage = workflow.storage
    generation, execution_id = storage.claim_job_execution(
        'A', 1, started_at=now(), session_id='job-interrupt', component=('A', 'B'),
    )
    if condition == 'terminal':
        storage.finalize_job_execution('A', 1, generation, execution_id, 'done')
        storage.finish_execution_session('job-interrupt', outcome='done', finished_at=now())
    elif condition == 'local-reused':
        storage.submit_db_mutation(lambda connection: connection.execute(
            "UPDATE execution_sessions SET process_identity='previous-process' WHERE session_id='job-interrupt'",
        ))
    elif condition.startswith('local-unidentified'):
        heartbeat = '2000-01-01T00:00:00' if condition.endswith('-stale') else now()
        storage.submit_db_mutation(lambda connection: connection.execute(
            "UPDATE execution_sessions SET process_identity=NULL, heartbeat_at=? WHERE session_id='job-interrupt'",
            (heartbeat,),
        ))
    elif condition == 'missing-job-metadata':
        storage.submit_db_mutation(lambda connection: connection.execute(
            "UPDATE jobs SET active_pid=NULL, active_started_at=NULL WHERE node_name='A' AND job_id=1",
        ))
    elif condition.startswith('foreign'):
        heartbeat = '2000-01-01T00:00:00' if condition == 'foreign-stale' else now()
        storage.submit_db_mutation(lambda connection: connection.execute(
            "UPDATE execution_sessions SET hostname='other-host', heartbeat_at=? WHERE session_id='job-interrupt'",
            (heartbeat,),
        ))
    before = list(storage.db_connection().iterdump())

    def no_notification(*args, **kwargs):
        raise AssertionError('Inspection emitted a mutation notification')

    monkeypatch.setattr(storage, 'notify_state_change', no_notification)
    monkeypatch.setattr(storage, 'notify_queue_change', no_notification)
    assert inspect_command(workflow, 'A', 1) == 0
    output = capsys.readouterr().out
    assert ('session status: terminal' if condition == 'terminal' else 'session status: running') in output
    assert f"session liveness: {'live' if live else 'not live'}" in output
    assert f'session liveness reason: {reason}' in output
    if condition == 'missing-job-metadata':
        assert 'active process: (not recorded)' in output
        assert 'active since: (not recorded)' in output
        assert 'session ID: job-interrupt' in output
    assert list(storage.db_connection().iterdump()) == before
