from __future__ import annotations

import re

import pytest

from micro_workflow_manager import cli
from micro_workflow_manager.models import Job, now
from tests.test_064_read_only_previews import (
    _close,
    _initialize_native_project,
    _mark_execution_session_stale,
    _snapshot,
    _wait_writer,
)
from tests.test_146_native_applied_recovery import (
    _assert_current_format,
    _assert_recovered_failed_scope,
    _create_active_scope,
    _scope_rows,
)


RECOVERY_RECEIPTS = 'recovery_receipts'
TARGET_SESSION = 'failed-A'
SAFE_SESSION = 'safe-Z'


def _claim_second_job(storage):
    storage.create_job(Job(node_name='A', job_id=2, params={'scope': TARGET_SESSION}))
    generation, execution_id = storage.claim_job_execution(
        'A',
        2,
        started_at=now(),
        session_id=TARGET_SESSION,
        component=('A',),
    )
    return {
        'session_id': TARGET_SESSION,
        'generation': generation,
        'execution_id': execution_id,
        'instance_id': storage.read_job_instance_id('A', 2),
        'owner': storage.get_job_execution_owner(execution_id),
    }


def _job_row(storage, node, job_id):
    return tuple(storage.db_connection().execute(
        'SELECT status, generation, active_execution_id, active_pid, '
        'active_thread_id, active_started_at, restart_requested_at, '
        'restart_requested_by_pid, restart_reason '
        'FROM jobs WHERE node_name=? AND job_id=?',
        (node, job_id),
    ).fetchone())


def _file_identity(path):
    status = path.stat()
    return (
        path.read_bytes(),
        status.st_mode,
        status.st_dev,
        status.st_ino,
        status.st_size,
        status.st_mtime_ns,
    )


def _scope_business_rows(storage, node, session_id):
    rows = _scope_rows(storage, node, session_id)
    receipts = rows.pop(RECOVERY_RECEIPTS, [])
    return rows, receipts


def _recovery_receipts(storage, session_id):
    connection = storage.db_connection()
    columns = [str(row[1]) for row in connection.execute(
        f'PRAGMA table_info("{RECOVERY_RECEIPTS}")',
    )]
    assert {'operation_id', 'session_id', 'state'} <= set(columns)
    rows = []
    for row in connection.execute(
        f'SELECT * FROM "{RECOVERY_RECEIPTS}" WHERE session_id=? ORDER BY operation_id',
        (session_id,),
    ):
        values = dict(zip(columns, tuple(row), strict=True))
        assert type(values['operation_id']) is str
        assert re.fullmatch(r'[0-9a-f]{32}', values['operation_id'])
        assert values['session_id'] == session_id
        assert values['state'] in {'prepared', 'committed', 'aborted'}
        rows.append(values)
    return rows


def _install_failure_writer_hook(monkeypatch, boundary):
    from micro_workflow_manager.storage import native_recovery

    message = f'injected native recovery {boundary}'
    definitions = {
        'second-job': ('fail_native_recovery_second_job', """
            CREATE TRIGGER fail_native_recovery_second_job
            BEFORE UPDATE OF status, generation ON jobs
            WHEN OLD.node_name='A' AND OLD.job_id=2
                 AND OLD.status='running' AND NEW.status='queued'
                 AND NEW.generation=OLD.generation+1
            BEGIN
                SELECT RAISE(ABORT, 'injected native recovery second-job');
            END
        """),
        'component-settlement': ('fail_native_recovery_component_settlement', """
            CREATE TRIGGER fail_native_recovery_component_settlement
            BEFORE UPDATE OF lifecycle ON component_states
            WHEN OLD.component_key='[\"A\"]' AND NEW.lifecycle='failed'
            BEGIN
                SELECT RAISE(ABORT, 'injected native recovery component-settlement');
            END
        """),
        'hold-deletion': ('fail_native_recovery_hold_deletion', """
            CREATE TRIGGER fail_native_recovery_hold_deletion
            BEFORE DELETE ON component_holds
            WHEN OLD.session_id='failed-A'
            BEGIN
                SELECT RAISE(ABORT, 'injected native recovery hold-deletion');
            END
        """),
        'reservation-release': ('fail_native_recovery_reservation_release', """
            CREATE TRIGGER fail_native_recovery_reservation_release
            BEFORE DELETE ON component_reservations
            WHEN OLD.session_id='failed-A'
            BEGIN
                SELECT RAISE(ABORT, 'injected native recovery reservation-release');
            END
        """),
        'terminal-session': ('fail_native_recovery_terminal_session', """
            CREATE TRIGGER fail_native_recovery_terminal_session
            BEFORE UPDATE OF status ON execution_sessions
            WHEN OLD.session_id='failed-A' AND OLD.status='running'
                 AND NEW.status='terminal'
            BEGIN
                SELECT RAISE(ABORT, 'injected native recovery terminal-session');
            END
        """),
    }
    trigger_name, statement = definitions[boundary]
    original = native_recovery._commit_session_recovery
    state = {'installed': False, 'removed': False}

    def inject_after_observation(storage, plan, receipt):
        if plan.session_id != TARGET_SESSION:
            return original(storage, plan, receipt)
        assert not state['installed']
        connection = storage.db_connection()
        connection.execute(statement)
        connection.commit()
        state['installed'] = True
        try:
            return original(storage, plan, receipt)
        finally:
            connection.execute(f'DROP TRIGGER "{trigger_name}"')
            connection.commit()
            state['removed'] = True

    monkeypatch.setattr(
        native_recovery,
        '_commit_session_recovery',
        inject_after_observation,
    )
    return message, state


@pytest.mark.parametrize(
    'failure_boundary',
    (
        'second-job',
        'component-settlement',
        'hold-deletion',
        'reservation-release',
        'terminal-session',
    ),
)
def test_recovery_writer_failure_restores_one_session_and_continues(
    tmp_path,
    monkeypatch,
    capsys,
    failure_boundary,
):
    workflow = _initialize_native_project(
        tmp_path,
        monkeypatch,
        edges=[('A', 'A'), ('C', 'C'), ('Z', 'Z')],
    )
    storage = workflow.storage
    _assert_current_format(storage)
    shape = workflow.topology.snapshot().shape_json
    first = _create_active_scope(storage, shape, 'A', TARGET_SESSION, live=True)
    second = _claim_second_job(storage)
    _mark_execution_session_stale(storage, TARGET_SESSION)
    safe = _create_active_scope(storage, shape, 'Z', SAFE_SESSION)
    storage.acquire_component_holds(TARGET_SESSION, [('C',)])
    storage.acquire_component_holds(TARGET_SESSION, [('C',)])

    invalid_outputs = []
    for job_id, payload in ((1, b'invalid output one'), (2, b'{invalid output two')):
        path = storage.output_file('A', job_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(payload)
        invalid_outputs.append(path)
    assert not storage.output_file('Z', 1).exists()

    storage.db_mutation_barrier()
    _wait_writer(storage)
    injected_message, injection = _install_failure_writer_hook(
        monkeypatch,
        failure_boundary,
    )

    before_business, before_receipts = _scope_business_rows(storage, 'A', TARGET_SESSION)
    assert before_receipts == []
    before_node_tree = _snapshot(tmp_path / 'node')
    before_files = {path: _file_identity(path) for path in invalid_outputs}
    before_jobs = {job_id: _job_row(storage, 'A', job_id) for job_id in (1, 2)}
    assert all(row[0] == 'running' and row[1] == 0 for row in before_jobs.values())
    assert storage.db_connection().execute(
        'SELECT hold_count FROM component_holds WHERE session_id=?',
        (TARGET_SESSION,),
    ).fetchone()[0] == 2

    capsys.readouterr()
    try:
        assert cli.main(['recover']) == 1
        captured = capsys.readouterr()
        output = captured.out + captured.err
        assert TARGET_SESSION in output
        assert SAFE_SESSION in output
        assert injected_message in output
        assert injection == {'installed': True, 'removed': True}
        storage.db_mutation_barrier()
        _wait_writer(storage)

        after_business, after_receipts = _scope_business_rows(storage, 'A', TARGET_SESSION)
        assert after_business == before_business
        assert _snapshot(tmp_path / 'node') == before_node_tree
        assert {path: _file_identity(path) for path in invalid_outputs} == before_files
        assert {job_id: _job_row(storage, 'A', job_id) for job_id in (1, 2)} == before_jobs
        assert storage.get_job_execution_owner(first['execution_id']) == first['owner']
        assert storage.get_job_execution_owner(second['execution_id']) == second['owner']
        assert storage.read_job_current_owner('A', 1) == first['owner']
        assert storage.read_job_current_owner('A', 2) == second['owner']
        assert storage.db_connection().execute(
            'SELECT hold_count FROM component_holds WHERE session_id=?',
            (TARGET_SESSION,),
        ).fetchone()[0] == 2
        assert storage.db_connection().execute(
            'SELECT 1 FROM component_reservations WHERE session_id=?',
            (TARGET_SESSION,),
        ).fetchone() is not None
        assert storage.db_connection().execute(
            'SELECT 1 FROM pending_component_executions WHERE session_id=?',
            (TARGET_SESSION,),
        ).fetchone() is not None
        assert storage.db_connection().execute(
            'SELECT status, outcome, finished_at FROM execution_sessions WHERE session_id=?',
            (TARGET_SESSION,),
        ).fetchone() is not None
        assert tuple(storage.db_connection().execute(
            'SELECT status, outcome, finished_at FROM execution_sessions WHERE session_id=?',
            (TARGET_SESSION,),
        ).fetchone()) == ('running', None, None)

        assert len(after_receipts) == 1
        target_receipts = _recovery_receipts(storage, TARGET_SESSION)
        assert len(target_receipts) == 1
        assert target_receipts[0]['state'] == 'aborted'
        safe_receipts = _recovery_receipts(storage, SAFE_SESSION)
        assert len(safe_receipts) == 1
        assert safe_receipts[0]['state'] == 'committed'
        _assert_recovered_failed_scope(storage, 'Z', safe, terminal=False)
        assert storage.get_job_execution_owner(safe['execution_id']) == safe['owner']
    finally:
        _close(storage)
