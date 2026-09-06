from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import textwrap
import time
from datetime import datetime, timezone
from pathlib import Path

import pytest

from micro_workflow_manager import MicroWorkflow
from micro_workflow_manager.cli.top import render_top, top_snapshot
from micro_workflow_manager.models import Job, now
from micro_workflow_manager.monitor import render_snapshot, workflow_snapshot
from micro_workflow_manager.processes import process_identity


def _close(storage):
    storage.db_mutation_barrier()
    deadline = time.perf_counter() + 10
    while storage.mutation_writer_diagnostics()['writer_alive']:
        assert time.perf_counter() < deadline
        time.sleep(0.01)
    storage.close_database_connections()


def _session(storage, session_id, kind, component, parent=None, hostname=None):
    return storage.create_execution_session(
        session_id, session_kind=kind, command='run' if kind == 'main' else 'interrupt',
        start_component=component, selected_components=[component],
        selected_jobs=[('A', 1)] if kind == 'main' else [],
        parent_session_id=parent, started_at=now(), hostname=hostname or socket.gethostname(),
        pid=os.getpid(), process_identity=process_identity(os.getpid()),
        details={'start_node': component[0]},
    )


@pytest.mark.parametrize('view', ['monitor', 'top'])
def test_monitoring_lists_exact_native_main_interrupts_and_history(tmp_path, monkeypatch, view):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'B'), ('B', 'A'), ('C', 'D'), ('D', 'C'), ('E', 'F'), ('F', 'E')])
    storage = workflow.storage
    storage.create_job(Job(node_name='A', job_id=1, params={'value': 7}))
    storage.register_component_topology(workflow.topology.snapshot())
    _session(storage, 'finished-main', 'main', ('A', 'B'))
    storage.finish_execution_session('finished-main', outcome='done', finished_at=now())
    _session(storage, 'current-main', 'main', ('A', 'B'))
    _session(storage, 'first-interrupt', 'interrupt', ('C', 'D'), parent='current-main')
    _session(storage, 'second-interrupt', 'interrupt', ('E', 'F'))
    for session_id in ('current-main', 'first-interrupt', 'second-interrupt'):
        storage.reserve_execution_components(session_id, expected_shape=workflow.topology.snapshot().shape_json)
    before_sessions = storage.list_execution_sessions()
    before_events = storage.read_job_events('A', 1)

    def obsolete_reader():
        raise AssertionError('Monitoring still reads the removed singleton run model')

    monkeypatch.setattr(storage, 'get_run_state', obsolete_reader)
    try:
        snapshot = workflow_snapshot(workflow) if view == 'monitor' else top_snapshot(workflow, list(workflow.nodes))
        assert snapshot['sessions'] == before_sessions
        assert 'run_state' not in snapshot
        assert 'active_run' not in snapshot
        text = render_snapshot(snapshot) if view == 'monitor' else render_top(snapshot)
        for session in before_sessions:
            assert session['session_id'] in text
        assert 'first-interrupt' in text and 'parent=current-main' in text
        assert 'done' in text
        if view == 'top':
            for session_id in ('current-main', 'first-interrupt', 'second-interrupt'):
                observed = snapshot['session_diagnostics'][session_id]
                assert observed['process']['alive'] is True
                assert observed['mutation_writer']['source'] == 'local'
            assert snapshot['mutation_writer']['scope'] == 'observer-process'
        assert storage.list_execution_sessions() == before_sessions
        assert storage.read_job_events('A', 1) == before_events
        assert storage.get_job_status('A', 1) == 'queued'
        assert not (tmp_path / '.mwf' / 'run.json').exists()
    finally:
        _close(storage)


def test_top_does_not_attribute_local_process_metrics_to_a_foreign_session(tmp_path, monkeypatch):
    from micro_workflow_manager.cli import top

    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'B')])
    _session(workflow.storage, 'foreign-interrupt', 'interrupt', ('A',), hostname='other-mwf-host')

    def reject_local_probe(pid):
        raise AssertionError('A foreign session was attributed to a local process')

    monkeypatch.setattr(top, '_pid_snapshot', reject_local_probe)
    try:
        snapshot = top_snapshot(workflow, ['A', 'B'])
        diagnostics = snapshot['session_diagnostics']['foreign-interrupt']
        assert diagnostics['process']['pid'] == os.getpid()
        assert diagnostics['process']['alive'] is None
        assert diagnostics['mutation_writer']['source'] == 'unavailable'
        assert 'foreign-interrupt' in render_top(snapshot)
    finally:
        _close(workflow.storage)


@pytest.mark.parametrize('state', ['finished', 'reused-pid', 'missing-identity'])
def test_top_does_not_assign_process_metrics_without_current_session_identity(tmp_path, monkeypatch, state):
    from micro_workflow_manager.cli import top

    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'B')])
    storage = workflow.storage
    _session(storage, 'uncertain-session', 'interrupt', ('A',))
    if state == 'finished':
        storage.finish_execution_session('uncertain-session', outcome='done', finished_at=now())
    else:
        identity = 'earlier-process-instance' if state == 'reused-pid' else None
        storage.submit_db_mutation(lambda connection: connection.execute(
            'UPDATE execution_sessions SET process_identity=? WHERE session_id=?',
            (identity, 'uncertain-session'),
        ))
    before = storage.list_execution_sessions()

    def reject_probe(pid):
        raise AssertionError('Metrics were attributed using only the numeric PID')

    monkeypatch.setattr(top, '_pid_snapshot', reject_probe)
    try:
        diagnostics = top_snapshot(workflow, ['A'])['session_diagnostics']['uncertain-session']
        assert diagnostics['process'] == {'pid': os.getpid(), 'alive': None}
        assert diagnostics['mutation_writer'] == {'source': 'unavailable', 'age_seconds': None}
        assert storage.list_execution_sessions() == before
    finally:
        _close(storage)


@pytest.mark.parametrize('view', ['monitor', 'top'])
def test_monitoring_renders_native_timezone_aware_session_times(tmp_path, view):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'B')])
    workflow.storage.create_execution_session(
        'aware-session', session_kind='interrupt', command='interrupt',
        start_component=('A',), selected_components=[('A',)],
        started_at=datetime.now(timezone.utc).isoformat(),
        hostname=socket.gethostname(), pid=os.getpid(), process_identity=process_identity(os.getpid()),
    )
    try:
        snapshot = workflow_snapshot(workflow) if view == 'monitor' else top_snapshot(workflow, ['A'])
        rendered = render_snapshot(snapshot) if view == 'monitor' else render_top(snapshot)
        assert 'session=aware-session' in rendered
        assert 'elapsed=' in rendered
    finally:
        _close(workflow.storage)


def test_top_observes_writer_of_verified_session_in_another_local_process(tmp_path):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'B')])
    _close(workflow.storage)
    code = textwrap.dedent('''
        import os, socket, sys, time
        from pathlib import Path
        from micro_workflow_manager import MicroWorkflow
        from micro_workflow_manager.models import Job, now
        from micro_workflow_manager.processes import process_identity
        root = Path(sys.argv[1])
        workflow = MicroWorkflow(root, runner='direct', persist_graph=False)
        workflow.graph([('A', 'B')])
        storage = workflow.storage
        storage.create_execution_session(
            'child-session', session_kind='interrupt', command='interrupt',
            start_component=('A',), selected_components=[('A',)], started_at=now(),
            hostname=socket.gethostname(), pid=os.getpid(), process_identity=process_identity(os.getpid()),
        )
        storage.create_job(Job(node_name='A', job_id=1, params={'value': 9}))
        storage.db_mutation_barrier()
        deadline = time.monotonic() + 10
        while storage.mutation_writer_diagnostics()['writer_alive']:
            assert time.monotonic() < deadline
            time.sleep(0.01)
        (root / 'child-ready').write_text('ready', encoding='utf-8')
        try:
            deadline = time.monotonic() + 20
            while not (root / 'child-stop').exists():
                assert time.monotonic() < deadline
                time.sleep(0.01)
        finally:
            storage.close_database_connections()
    ''')
    environment = dict(os.environ, PYTHONPATH=str(Path(__file__).resolve().parents[1]))
    child = subprocess.Popen([sys.executable, '-c', code, str(tmp_path)], env=environment)
    try:
        deadline = time.monotonic() + 15
        while not (tmp_path / 'child-ready').exists():
            assert child.poll() is None, 'Child exited before publishing its session'
            assert time.monotonic() < deadline
            time.sleep(0.01)
        snapshot = top_snapshot(workflow, ['A', 'B'])
        diagnostics = snapshot['session_diagnostics']['child-session']
        session_pid = snapshot['sessions'][0]['pid']
        assert session_pid != os.getpid()
        assert diagnostics['process']['pid'] == session_pid
        assert diagnostics['process']['alive'] is True
        writer = diagnostics['mutation_writer']
        assert writer['source'] == 'active-process'
        assert writer['pid'] == session_pid
        assert writer['hostname'] == socket.gethostname()
        assert writer['process_identity'] == process_identity(session_pid)
        assert snapshot['mutation_writer']['pid'] == os.getpid()
        diagnostic_path = tmp_path / '.mwf' / 'mutation_writer.json'
        original = diagnostic_path.read_bytes()
        persisted = json.loads(original)
        assert writer['durability_backlog'] == persisted['durability_backlog']
        assert writer['updated_at'] == persisted['updated_at']
        assert writer['age_seconds'] >= 0
        assert workflow.storage.get_job_status('A', 1) == 'queued'
        try:
            for field, value in (
                ('pid', os.getpid()), ('hostname', 'unrelated-host'),
                ('process_identity', 'earlier-process'), ('updated_at', 'nan'),
            ):
                changed = dict(writer, **{field: value})
                diagnostic_path.write_text(json.dumps(changed), encoding='utf-8')
                observed = top_snapshot(workflow, ['A'])['session_diagnostics']['child-session']
                assert observed['mutation_writer'] == {'source': 'unavailable', 'age_seconds': None}
        finally:
            diagnostic_path.write_bytes(original)
    finally:
        (tmp_path / 'child-stop').write_text('stop', encoding='utf-8')
        try:
            child.wait(timeout=10)
        except subprocess.TimeoutExpired:
            child.kill()
            child.wait(timeout=5)
        _close(workflow.storage)
    assert child.returncode == 0


def test_top_renders_recorded_api_settings_for_each_native_session(tmp_path):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'B')])
    workflow.storage.create_execution_session(
        'settings-session', session_kind='interrupt', command='interrupt',
        start_component=('A',), selected_components=[('A',)], started_at=now(),
        hostname=socket.gethostname(), pid=os.getpid(), process_identity=process_identity(os.getpid()),
        details={
            'api_startup_strategy': 'balanced', 'api_startup_windows': 'auto:1-2',
            'api_max_admission_burst': '768', 'api_admission_target_rounds': '5',
            'api_claim_transaction_rows': '64',
        },
    )
    try:
        text = render_top(top_snapshot(workflow, ['A']))
        for field in ('strategy=balanced', 'windows=auto:1-2', 'burst=768', 'rounds=5', 'claim-tx=64'):
            assert field in text
    finally:
        _close(workflow.storage)
