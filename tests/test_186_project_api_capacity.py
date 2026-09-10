"""API capacity follows active executions across limit changes and recovery."""

from __future__ import annotations

import subprocess
import time

from micro_workflow_manager import cli
from micro_workflow_manager.storage import FileStorage
from tests.test_036_hoeflein_scheduling import make_project
from tests.test_064_read_only_previews import (
    _initialize_native_project,
    _mark_execution_session_stale,
)
from tests.test_146_native_applied_recovery import _create_active_scope, _scope_rows
from tests.test_155_explicit_interrupt_execution import _start, _wait_until
from tests.test_179_project_api_admission import _permit_identities


def _future_handler(node):
    return f'''
        import time
        from concurrent.futures import ThreadPoolExecutor
        from micro_workflow_manager import NodeRouter
        router = NodeRouter({node!r}, runner='api', interrupt=True)
        router.create_job(params={{}})
        @router.task
        def work(ctx):
            root = ctx.system.storage.project_dir
            (root / '{node}-handler-entered').write_text(ctx.execution_id, encoding='utf-8')
            def provider():
                deadline = time.monotonic() + 90
                (root / '{node}-provider-active').write_text(ctx.execution_id, encoding='utf-8')
                while not (root / '{node}-release').exists():
                    assert time.monotonic() < deadline
                    time.sleep(0.01)
                return 'provider complete'
            with ThreadPoolExecutor(max_workers=1) as executor:
                result = executor.submit(provider).result()
            ctx.checkpoint('provider completed')
            return result
    '''


def test_active_future_waits_count_when_cap_is_installed_lowered_and_cleared(
    tmp_path, monkeypatch,
):
    nodes = ('U', 'V', 'W', 'X')
    make_project(
        tmp_path, monkeypatch,
        edges=f'EDGES = {[(node, node) for node in nodes]!r}',
        runner='api', files={node: _future_handler(node) for node in nodes},
    )
    storage = FileStorage(tmp_path)
    processes = {}

    def launch(node, *, admitted):
        process = _start(tmp_path, ['run', node, '--interrupt', '--runner', 'api'])
        processes[node] = process
        _wait_until(lambda: storage.read_job_current_owner(node, 1) is not None
                    or process.poll() is not None)
        assert process.poll() is None, process.communicate(timeout=5)
        if admitted:
            wait_for_handler(node)

    def wait_for_handler(node):
        process = processes[node]
        _wait_until(lambda: (tmp_path / f'{node}-provider-active').exists()
                    or process.poll() is not None)
        assert process.poll() is None, process.communicate(timeout=5)
        execution_id = storage.read_job_current_owner(node, 1)['execution_id']
        assert (tmp_path / f'{node}-handler-entered').read_text(encoding='utf-8') == execution_id
        assert (tmp_path / f'{node}-provider-active').read_text(encoding='utf-8') == execution_id

    def finish(node):
        (tmp_path / f'{node}-release').touch()
        output, errors = processes[node].communicate(timeout=35)
        assert processes[node].returncode == 0, output + errors

    def assert_waiting(node, expected_holders):
        # Observe multiple scheduler retry opportunities without releasing any
        # provider. Claim bookkeeping precedes capacity admission; the first
        # handler statement and the physical provider must both remain blocked.
        deadline = time.monotonic() + 0.3
        while time.monotonic() < deadline:
            assert processes[node].poll() is None
            assert not (tmp_path / f'{node}-handler-entered').exists()
            assert not (tmp_path / f'{node}-provider-active').exists()
            assert {row[1] for row in _permit_identities(storage)} == expected_holders
            assert storage.db_connection().execute(
                "SELECT COUNT(*) FROM job_events WHERE node_name=? AND event='task_started'",
                (node,),
            ).fetchone()[0] == 1
            time.sleep(0.02)

    try:
        assert storage.read_api_total_limit() is None
        launch('U', admitted=True)
        launch('V', admitted=True)
        assert {row[1] for row in _permit_identities(storage)} == {'U', 'V'}
        storage.set_api_total_limit(1)
        launch('W', admitted=False)
        assert_waiting('W', {'U', 'V'})
        finish('U')
        assert_waiting('W', {'V'})

        storage.clear_api_total_limit()
        wait_for_handler('W')
        assert {row[1] for row in _permit_identities(storage)} == {'V', 'W'}
        storage.set_api_total_limit(1)
        launch('X', admitted=False)
        assert_waiting('X', {'V', 'W'})
        finish('V')
        assert_waiting('X', {'W'})
        finish('W')
        wait_for_handler('X')
        assert {row[1] for row in _permit_identities(storage)} == {'X'}
        finish('X')
        assert _permit_identities(storage) == []
        assert storage.read_api_total_limit() is None
        assert all(storage.get_job_status(node, 1) == 'done' for node in nodes)
    finally:
        for node in nodes:
            (tmp_path / f'{node}-release').touch(exist_ok=True)
        for process in processes.values():
            if process.poll() is None:
                try:
                    process.communicate(timeout=20)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.communicate(timeout=10)
                    raise
        storage.close_database_connections()


def test_native_recovery_releases_only_abandoned_session_capacity(tmp_path, monkeypatch):
    workflow = _initialize_native_project(
        tmp_path, monkeypatch, edges=[('A', 'A'), ('B', 'B')],
    )
    storage = workflow.storage
    try:
        shape = workflow.topology.snapshot().shape_json
        abandoned = _create_active_scope(storage, shape, 'A', 'abandoned-api', live=True)
        continuing = _create_active_scope(storage, shape, 'B', 'continuing-api', live=True)
        storage.set_api_total_limit(2)
        for node, owner in [('A', abandoned), ('B', continuing)]:
            assert storage.try_acquire_api_execution_permit(
                owner['session_id'], node, 1, owner['generation'], owner['execution_id'],
            ) is True
        _mark_execution_session_stale(storage, abandoned['session_id'])
        before = _scope_rows(storage, 'B', continuing['session_id'])
        assert cli.main(['recover']) == 0
        assert _scope_rows(storage, 'B', continuing['session_id']) == before
        assert storage.read_api_total_limit() == 2
        assert _permit_identities(storage) == [(
            continuing['session_id'], 'B', 1,
            continuing['generation'], continuing['execution_id'],
        )]
        assert storage.get_job_status('A', 1) == 'queued'
        assert storage.get_execution_session(abandoned['session_id'])['status'] == 'terminal'
        assert storage.get_execution_session(continuing['session_id'])['status'] == 'running'
    finally:
        storage.close_database_connections()
