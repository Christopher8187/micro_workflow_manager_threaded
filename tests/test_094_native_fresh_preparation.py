from __future__ import annotations

import networkx as nx
import pytest
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

from micro_workflow_manager import cli
from micro_workflow_manager.cli.project import load_workflow
from micro_workflow_manager.cli.run_commands import run_from
from micro_workflow_manager.models import Job
from micro_workflow_manager.storage import FileStorage
from micro_workflow_manager.topology import ComponentTopology
from tests.test_036_hoeflein_scheduling import make_project
from tests.test_076_component_state_transitions import _seed_state, _session
from tests.test_090_component_session_settlement import _close, _rows
from tests.test_093_native_cli_readiness import _node_files


def _fresh_owner(tmp_path):
    storage = FileStorage._create_new_project_state(tmp_path)
    snapshot = ComponentTopology(nx.DiGraph([('A', 'B')]), []).snapshot()
    storage.register_component_topology(snapshot)
    _session(storage, 'fresh-main', 'main', ('A',), snapshot)
    storage.reserve_execution_components('fresh-main', expected_shape=snapshot.shape_json)
    return storage, snapshot


@pytest.mark.parametrize('lifecycle,misaligned', [
    ('queued', False), ('done', False), ('done', True),
    ('sampled', False), ('sampled', True), ('failed', False), ('failed', True),
])
@pytest.mark.parametrize('authority', ['run', 'reset'])
def test_full_preparation_queues_and_realigns_only_its_component(tmp_path, lifecycle, misaligned, authority):
    storage, snapshot = _fresh_owner(tmp_path)
    try:
        if authority == 'reset':
            storage.finish_execution_session('fresh-main', outcome='done', finished_at='2026-09-06T12:00:00+00:00')
            storage.release_execution_components('fresh-main')

        def complete(expected):
            if authority == 'reset':
                return storage.complete_component_reset_preparation(('A',), expected_state=expected)
            return storage.complete_component_fresh_preparation('fresh-main', ('A',), expected_state=expected)

        _seed_state(
            storage, ('A',), lifecycle=lifecycle,
            stability='stable' if lifecycle in ('done', 'sampled') else None,
            origin=None, generation=7, misaligned=int(misaligned),
        )
        before = _rows(storage)
        if authority == 'reset':
            prepared_from = storage.read_component_reset_preparation(('A',), expected_shape=snapshot.shape_json)
        else:
            prepared_from = storage.read_component_fresh_preparation(
                'fresh-main', ('A',), expected_shape=snapshot.shape_json,
            )
        assert _rows(storage) == before
        assert prepared_from == storage.get_component_state(('A',))
        expected = {**prepared_from, 'lifecycle': 'queued',
                    'stability': None, 'instability_origin': None, 'misaligned': False,
                    'alignment_generation': 8}

        assert complete(prepared_from) == 8

        assert storage.get_component_state(('A',)) == expected
        for row in before['component_states']:
            if row['component_key'] == '["A"]':
                row.update(lifecycle='queued', stability=None, instability_origin=None,
                           misaligned=0, alignment_generation=8,
                           retained_result_shape_id=None,
                           retained_result_alignment_generation=None)
        assert _rows(storage) == before
        with pytest.raises(RuntimeError, match='changed'):
            complete(prepared_from)
        assert _rows(storage) == before
    finally:
        _close(storage)

    reopened = FileStorage(tmp_path)
    try:
        assert reopened.get_component_state(('A',)) == expected
        assert _rows(reopened) == before
    finally:
        _close(reopened)


@pytest.mark.parametrize('damage', [
    'running', 'wrong-shape', 'stale-generation', 'missing-session', 'terminal-session',
    'missing-reservation', 'outside-selection', 'missing-state', 'invalid-state',
    'held', 'pending', 'active-job', 'selected-job', 'changed-result',
    'active-pid', 'active-thread', 'active-started',
])
def test_full_preparation_transition_refuses_unowned_or_invalid_state(tmp_path, damage):
    storage, snapshot = _fresh_owner(tmp_path)
    try:
        session, shape = 'fresh-main', snapshot.shape_json
        expected = storage.get_component_state(('A',))
        if damage == 'running':
            storage.submit_db_mutation(lambda connection: connection.execute(
                "UPDATE component_states SET lifecycle='running' WHERE component_key=?", ('["A"]',),
            ))
        elif damage == 'wrong-shape':
            shape = snapshot.shape_json.replace('"A"', '"C"')
            expected['shape_json'] = shape
        elif damage == 'stale-generation':
            expected['alignment_generation'] = 1
        elif damage == 'missing-session':
            session = 'absent'
        elif damage == 'terminal-session':
            storage.finish_execution_session(session, outcome='done', finished_at='2026-09-06T12:00:00+00:00')
        elif damage == 'missing-reservation':
            storage.release_execution_components(session)
        elif damage == 'outside-selection':
            storage.submit_db_mutation(lambda connection: connection.execute(
                'DELETE FROM session_components WHERE session_id=?', (session,),
            ))
        elif damage == 'missing-state':
            storage.db_mutation_barrier()
            connection = storage.db_connection()
            connection.execute('PRAGMA foreign_keys=OFF')
            try:
                assert connection.execute(
                    'DELETE FROM component_states WHERE component_key=?', ('["A"]',),
                ).rowcount == 1
            finally:
                connection.execute('PRAGMA foreign_keys=ON')
        elif damage == 'held':
            storage.acquire_component_holds(session, [('A',)])
        elif damage == 'pending':
            storage.submit_db_mutation(lambda connection: connection.execute(
                'INSERT INTO pending_component_executions '
                '(session_id, component_key, shape_id, starting_shape_id, alignment_generation, '
                'completion_ready, execution_kind, starting_lifecycle, starting_misaligned, '
                'stability, instability_origin) '
                "SELECT ?, component_key, shape_id, shape_id, 0, 0, 'full', 'queued', 0, 'stable', NULL "
                'FROM component_states WHERE component_key=?',
                (session, '["A"]'),
            ))
        elif damage == 'active-job':
            storage.create_job(Job(node_name='A', job_id=1, params={}))
            storage.claim_job_execution('A', 1, started_at='2026-09-06T12:00:00+00:00',
                                        session_id=session, component=('A',))
        elif damage.startswith('active-'):
            storage.create_job(Job(node_name='A', job_id=1, params={}))
            field, value = {
                'active-pid': ('active_pid', 123), 'active-thread': ('active_thread_id', 456),
                'active-started': ('active_started_at', '2026-09-06T12:00:00+00:00'),
            }[damage]
            storage.submit_db_mutation(lambda connection: connection.execute(
                f'UPDATE jobs SET {field}=? WHERE node_name=? AND job_id=1', (value, 'A'),
            ))
        elif damage == 'selected-job':
            storage.create_job(Job(node_name='A', job_id=1, params={}))
            def select_job(connection):
                connection.execute("UPDATE execution_sessions SET selection_kind='jobs' WHERE session_id=?", (session,))
                connection.execute(
                    'INSERT INTO session_jobs(session_id, position, node_name, job_id, job_instance_id) '
                    'SELECT ?, 0, node_name, job_id, instance_id FROM job_instances WHERE node_name=? AND job_id=1',
                    (session, 'A'),
                )
            storage.submit_db_mutation(select_job)
        elif damage == 'changed-result':
            _seed_state(
                storage, ('A',), lifecycle='done', stability='stable',
                origin=None, generation=0,
            )
        elif damage == 'invalid-state':
            def corrupt(connection):
                connection.execute('PRAGMA ignore_check_constraints=ON')
                try:
                    connection.execute("UPDATE component_states SET lifecycle='done', stability=NULL")
                finally:
                    connection.execute('PRAGMA ignore_check_constraints=OFF')
            storage.submit_db_mutation(corrupt)
        before = _rows(storage)
        if damage not in ('stale-generation', 'changed-result'):
            with pytest.raises(RuntimeError):
                storage.read_component_fresh_preparation(session, ('A',), expected_shape=shape)
            assert _rows(storage) == before
        with pytest.raises(RuntimeError):
            storage.complete_component_fresh_preparation(
                session, ('A',), expected_state=expected,
            )
        assert _rows(storage) == before
    finally:
        _close(storage)


@pytest.mark.parametrize('failure', ['ABORT', 'IGNORE'])
def test_full_preparation_state_update_rolls_back_trigger_effects(tmp_path, failure):
    storage, snapshot = _fresh_owner(tmp_path)
    try:
        observed = storage.read_component_fresh_preparation('fresh-main', ('A',), expected_shape=snapshot.shape_json)
        expression = "RAISE(ABORT, 'injected preparation failure')" if failure == 'ABORT' else 'RAISE(IGNORE)'
        storage.submit_db_mutation(lambda connection: connection.execute(
            'CREATE TRIGGER fail_preparation BEFORE UPDATE OF alignment_generation ON component_states '
            "BEGIN INSERT INTO metadata(key,value) VALUES('fresh_test_marker','must roll back'); "
            f'SELECT {expression}; END',
        ))
        before = _rows(storage)
        with pytest.raises(sqlite3.IntegrityError if failure == 'ABORT' else RuntimeError):
            storage.complete_component_fresh_preparation('fresh-main', ('A',), expected_state=observed)
        assert _rows(storage) == before
        storage.submit_db_mutation(lambda connection: connection.execute('DROP TRIGGER fail_preparation'))
        assert storage.complete_component_fresh_preparation('fresh-main', ('A',), expected_state=observed) == 1
    finally:
        _close(storage)


@pytest.mark.parametrize('field,value', [
    ('alignment_generation', False), ('alignment_generation', True), ('alignment_generation', 0.0),
    ('misaligned', 0), ('misaligned', 1), ('members', ['A']), ('unexpected', None),
])
def test_full_preparation_rejects_malformed_expected_observations(tmp_path, field, value):
    storage, snapshot = _fresh_owner(tmp_path)
    try:
        expected = storage.read_component_fresh_preparation('fresh-main', ('A',), expected_shape=snapshot.shape_json)
        expected[field] = value
        before = _rows(storage)
        with pytest.raises(ValueError):
            storage.complete_component_fresh_preparation('fresh-main', ('A',), expected_state=expected)
        assert _rows(storage) == before
    finally:
        _close(storage)


def test_competing_full_preparation_completions_have_one_winner(tmp_path):
    storage, snapshot = _fresh_owner(tmp_path)
    barrier = Barrier(2)
    try:
        expected = storage.read_component_fresh_preparation('fresh-main', ('A',), expected_shape=snapshot.shape_json)

        def complete():
            barrier.wait(timeout=5)
            try:
                return storage.complete_component_fresh_preparation('fresh-main', ('A',), expected_state=expected)
            except RuntimeError as error:
                assert 'Component changed' in str(error)
                return 'refused'

        with ThreadPoolExecutor(max_workers=2) as executor:
            first, second = executor.submit(complete), executor.submit(complete)
            assert sorted([first.result(timeout=10), second.result(timeout=10)], key=str) == [1, 'refused']
        assert storage.get_component_state(('A',))['alignment_generation'] == 1
    finally:
        _close(storage)


@pytest.mark.parametrize('lifecycle', ['done', 'sampled'])
def test_full_preparation_clears_established_interrupt_lineage(tmp_path, lifecycle):
    storage, snapshot = _fresh_owner(tmp_path)
    try:
        _session(storage, 'old-interrupt', 'interrupt', ('B',), snapshot)
        storage.finish_execution_session('old-interrupt', outcome='done', finished_at='2026-09-06T12:00:00+00:00')
        _seed_state(
            storage, ('A',), lifecycle=lifecycle, stability='unstable',
            origin='old-interrupt', generation=0, misaligned=1,
        )
        expected = storage.read_component_fresh_preparation('fresh-main', ('A',), expected_shape=snapshot.shape_json)
        origin = storage.get_execution_session('old-interrupt')
        assert storage.complete_component_fresh_preparation('fresh-main', ('A',), expected_state=expected) == 1
        assert storage.get_component_state(('A',)) == {
            **expected, 'lifecycle': 'queued', 'stability': None, 'instability_origin': None,
            'misaligned': False, 'alignment_generation': 1,
        }
        assert storage.get_execution_session('old-interrupt') == origin
    finally:
        _close(storage)


@pytest.mark.parametrize('kind', ['main', 'interrupt'])
def test_reset_refuses_every_live_session_without_mutating_the_project(tmp_path, monkeypatch, kind):
    make_project(
        tmp_path, monkeypatch, edges="EDGES = [('A', 'B')]",
        files={name: f'''
            from micro_workflow_manager import NodeRouter
            router = NodeRouter('{name}')
            router.create_job(number=1)
            @router.task
            def run(ctx):
                ctx.write_output('ran.txt', '{name}')
        ''' for name in ('A', 'B')},
    )
    assert cli.main(['runfrom', 'A']) == 0
    storage = FileStorage(tmp_path)
    try:
        snapshot = ComponentTopology(nx.DiGraph([('A', 'B')]), []).snapshot()
        _session(storage, 'other-live', kind, ('B',), snapshot)
        before, files = _rows(storage), _node_files(tmp_path)
        assert cli.main(['reset', 'A', '--yes']) == 1
        assert _rows(storage) == before
        assert _node_files(tmp_path) == files
    finally:
        _close(storage)


@pytest.mark.parametrize('generated', [False, True])
@pytest.mark.parametrize('failure', ['invalid-output', 'ABORT', 'IGNORE'])
def test_failed_wide_fresh_preparation_keeps_completed_components_queued(tmp_path, monkeypatch, generated, failure):
    make_project(
        tmp_path, monkeypatch, edges="EDGES = [('A', 'B'), ('B', 'C'), ('A', 'C')]",
        files={name: f'''
            from micro_workflow_manager import NodeRouter
            router = NodeRouter('{name}')
            router.create_job(number=1)
            @router.task
            def run(ctx):
                if '{name}' == 'A' and {generated!r}:
                    ctx.node('C').add()
                ctx.write_output('ran.txt', '{name}')
        ''' for name in ('A', 'B', 'C')},
    )
    assert cli.main(['runfrom', 'A']) == 0
    workflow = load_workflow(tmp_path)
    storage = workflow.storage
    try:
        before = {node: storage.get_component_state((node,)) for node in ('A', 'B', 'C')}
        input_path = tmp_path / 'node' / 'A' / 'input' / 'project-owned.txt'
        input_path.write_bytes(b'established incoming input')
        output_dir = tmp_path / 'node' / 'B' / 'output'
        if failure == 'invalid-output':
            (output_dir / 'ran.txt').unlink()
            output_dir.rmdir()
            output_dir.write_bytes(b'output directory replaced by a file')
            error, message = ValueError, 'Expected directory:'
        else:
            expression = "RAISE(ABORT, 'injected preparation failure')" if failure == 'ABORT' else 'RAISE(IGNORE)'
            storage.submit_db_mutation(lambda connection: connection.execute(
                'CREATE TRIGGER fail_second_preparation BEFORE UPDATE OF alignment_generation ON component_states '
                "WHEN NEW.component_key='[\"B\"]' BEGIN "
                "INSERT INTO metadata(key,value) VALUES('second_fresh_marker','must roll back'); "
                f'SELECT {expression}; END',
            ))
            error = sqlite3.IntegrityError if failure == 'ABORT' else RuntimeError
            message = 'injected preparation failure' if failure == 'ABORT' else 'Component changed'
        before_rows = _rows(storage)
        before_files = _node_files(tmp_path)
        with pytest.raises(error, match=message):
            run_from(tmp_path, workflow, 'A')
        assert storage.get_component_state(('A',)) == {
            **before['A'], 'lifecycle': 'queued', 'stability': None,
            'instability_origin': None, 'misaligned': False,
            'alignment_generation': before['A']['alignment_generation'] + 1,
        }
        assert storage.get_component_state(('B',)) == before['B']
        assert storage.get_component_state(('C',)) == before['C']
        assert _rows(storage)['metadata'] == before_rows['metadata']
        for table in ('jobs', 'job_instances', 'job_events', 'nodes', 'job_sequences'):
            assert [row for row in _rows(storage)[table] if row['node_name'] in ('B', 'C')] == [
                row for row in before_rows[table] if row['node_name'] in ('B', 'C')]
        assert {name: data for name, data in _node_files(tmp_path).items() if name.startswith(('node/B/', 'node/C/'))} == {
            name: data for name, data in before_files.items() if name.startswith(('node/B/', 'node/C/'))}
        assert storage.get_job_status('A', 1) == 'queued'
        assert storage.get_job_status('C', 1) == 'done'
        assert input_path.read_bytes() == b'established incoming input'
        assert (tmp_path / 'node' / 'C' / 'output' / 'ran.txt').read_bytes() == b'C'
        assert not (tmp_path / 'node' / 'A' / 'output' / 'ran.txt').exists()
        for node in ('A', 'B', 'C'):
            assert storage.get_component_reservation((node,)) is None
        assert all(session['status'] == 'terminal' for session in storage.list_execution_sessions())
    finally:
        _close(storage)
