from __future__ import annotations

import time
import sqlite3
import re
import subprocess
import sys

import networkx as nx
import pytest

from micro_workflow_manager.storage import FileStorage
from micro_workflow_manager.topology import ComponentTopology


def _close(storage):
    storage.db_mutation_barrier()
    deadline = time.perf_counter() + 10
    while storage.mutation_writer_diagnostics()['writer_alive']:
        assert time.perf_counter() < deadline, 'Mutation writer did not retire'
        time.sleep(0.01)
    storage.close_thread_connection()


def test_fresh_component_state_preserves_exact_producing_shape_after_reopen(tmp_path):
    graph = nx.DiGraph([('A', 'B'), ('B', 'C')])
    snapshot = ComponentTopology(graph, [('A', 'B')]).snapshot()
    storage = FileStorage._create_new_project_state(tmp_path)
    expected = {
        ('A', 'B'): {
            'members': ('A', 'B'), 'shape_json': snapshot.shape_json,
            'lifecycle': 'queued', 'stability': None, 'instability_origin': None,
            'misaligned': False, 'alignment_generation': 0,
        },
        ('C',): {
            'members': ('C',), 'shape_json': snapshot.shape_json,
            'lifecycle': 'queued', 'stability': None, 'instability_origin': None,
            'misaligned': False, 'alignment_generation': 0,
        },
    }
    try:
        assert storage.register_component_topology(snapshot) is True
        for members, state in expected.items():
            assert storage.get_component_state(members) == state
    finally:
        _close(storage)

    reopened = FileStorage(tmp_path)
    try:
        for members, state in expected.items():
            assert reopened.get_component_state(members) == state
        assert reopened.get_component_state(('B', 'A', 'B')) == expected[('A', 'B')]
        assert reopened.get_component_state(('Unknown',)) is None
        assert reopened.register_component_topology(snapshot) is False
        assert reopened.database_integrity_check() == 'ok'
    finally:
        _close(reopened)


def test_component_state_creation_rolls_back_every_row_and_allows_retry(tmp_path):
    snapshot = ComponentTopology(nx.DiGraph([('A', 'B'), ('B', 'C')]), [('A', 'B')]).snapshot()
    storage = FileStorage._create_new_project_state(tmp_path)
    try:
        storage.submit_db_mutation(lambda connection: connection.execute('''
            CREATE TRIGGER reject_second_state BEFORE INSERT ON component_states
            WHEN NEW.component_key='["C"]'
            BEGIN SELECT RAISE(ABORT, 'injected component state failure'); END
        '''))
        with pytest.raises(sqlite3.IntegrityError, match='injected component state failure'):
            storage.register_component_topology(snapshot)
        assert {table: storage.db_connection().execute(f'SELECT COUNT(*) FROM {table}').fetchone()[0]
                for table in ('graph_shapes', 'component_definitions', 'component_states')} == {
                    'graph_shapes': 0, 'component_definitions': 0, 'component_states': 0,
                }
    finally:
        storage.submit_db_mutation(lambda connection: connection.execute(
            'DROP TRIGGER IF EXISTS reject_second_state'
        ))
        _close(storage)
    reopened = FileStorage(tmp_path)
    try:
        for members in (('A', 'B'), ('C',)):
            assert reopened.get_component_definition(members) is None
            assert reopened.get_component_state(members) is None
        assert reopened.register_component_topology(snapshot) is True
        assert reopened.get_component_state(('A', 'B'))['lifecycle'] == 'queued'
        assert reopened.get_component_state(('C',))['lifecycle'] == 'queued'
    finally:
        _close(reopened)


@pytest.mark.parametrize('entrypoint', ['read', 'register'])
def test_reregistration_preserves_results_and_refuses_missing_state(tmp_path, entrypoint):
    snapshot = ComponentTopology(nx.DiGraph([('A', 'B')]), []).snapshot()
    storage = FileStorage._create_new_project_state(tmp_path)
    try:
        storage.register_component_topology(snapshot)
        storage.submit_db_mutation(lambda connection: connection.execute(
            "UPDATE component_states SET lifecycle='done', stability='stable', "
            "misaligned=1, alignment_generation=7 WHERE component_key='[\"A\"]'"
        ))
        saved = storage.get_component_state(('A',))
        assert saved['lifecycle'] == 'done' and saved['stability'] == 'stable'
        assert saved['misaligned'] is True and saved['alignment_generation'] == 7
        assert storage.register_component_topology(snapshot) is False
        assert storage.get_component_state(('A',)) == saved
        storage.submit_db_mutation(lambda connection: connection.execute(
            "DELETE FROM component_states WHERE component_key='[\"A\"]'"
        ))
    finally:
        _close(storage)
    reopened = FileStorage(tmp_path)
    try:
        before = reopened.get_component_state(('B',))
        definition = reopened.get_component_definition(('A',))
        with pytest.raises(RuntimeError, match='[Ii]ncomplete component state'):
            if entrypoint == 'read':
                reopened.get_component_state(('A',))
            else:
                reopened.register_component_topology(snapshot)
        assert reopened.get_component_definition(('A',)) == definition
        assert reopened.get_component_state(('B',)) == before
        assert reopened.db_connection().execute(
            "SELECT COUNT(*) FROM component_states WHERE component_key='[\"A\"]'"
        ).fetchone()[0] == 0
    finally:
        _close(reopened)


def test_reregistration_refuses_missing_definition_without_recreating_history(tmp_path):
    snapshot = ComponentTopology(nx.DiGraph([('A', 'B')]), []).snapshot()
    storage = FileStorage._create_new_project_state(tmp_path)
    try:
        storage.register_component_topology(snapshot)

        def lose_component(connection):
            connection.execute(
                "UPDATE component_states SET lifecycle='done', stability='stable', "
                "misaligned=1, alignment_generation=7"
            )
            connection.execute('DELETE FROM component_states WHERE component_key=?', ('["A"]',))
            connection.execute('DELETE FROM component_definitions WHERE component_key=?', ('["A"]',))

        storage.submit_db_mutation(lose_component)
        surviving_state = storage.get_component_state(('B',))
        database = storage.state_database_path()
    finally:
        _close(storage)

    tables = ('graph_shapes', 'component_definitions', 'component_states')
    with sqlite3.connect(database) as connection:
        before = {table: connection.execute(f'SELECT * FROM {table} ORDER BY 1').fetchall()
                  for table in tables}
    reopened = FileStorage(tmp_path)
    try:
        with pytest.raises(RuntimeError, match='Incomplete registered component topology'):
            reopened.register_component_topology(snapshot)
        assert reopened.get_component_definition(('A',)) is None
        assert reopened.get_component_state(('A',)) is None
        assert reopened.get_component_state(('B',)) == surviving_state
        with sqlite3.connect(database) as connection:
            assert {table: connection.execute(f'SELECT * FROM {table} ORDER BY 1').fetchall()
                    for table in tables} == before
    finally:
        _close(reopened)


@pytest.mark.parametrize('extra_has_state', [True, False])
def test_reregistration_refuses_extra_definition_outside_producing_partition(tmp_path, extra_has_state):
    snapshot = ComponentTopology(nx.DiGraph([('A', 'B')]), []).snapshot()
    storage = FileStorage._create_new_project_state(tmp_path)
    try:
        storage.register_component_topology(snapshot)

        def add_extra_definition(connection):
            connection.execute(
                "UPDATE component_states SET lifecycle='done', stability='stable', "
                "misaligned=1, alignment_generation=7"
            )
            connection.execute(
                'INSERT INTO component_definitions(component_key, shape_id) '
                'SELECT ?, shape_id FROM graph_shapes WHERE shape_json=?',
                ('["Extra"]', snapshot.shape_json),
            )
            if extra_has_state:
                connection.execute('INSERT INTO component_states(component_key) VALUES(?)', ('["Extra"]',))

        storage.submit_db_mutation(add_extra_definition)
        saved = {members: storage.get_component_state(members) for members in [('A',), ('B',)]}
        database = storage.state_database_path()
    finally:
        _close(storage)
    tables = ('graph_shapes', 'component_definitions', 'component_states')
    with sqlite3.connect(database) as connection:
        before = {table: connection.execute(f'SELECT * FROM {table} ORDER BY 1').fetchall()
                  for table in tables}
    reopened = FileStorage(tmp_path)
    try:
        with pytest.raises(RuntimeError, match='Incomplete registered component topology'):
            reopened.register_component_topology(snapshot)
        assert {members: reopened.get_component_state(members) for members in saved} == saved
        with sqlite3.connect(database) as connection:
            assert {table: connection.execute(f'SELECT * FROM {table} ORDER BY 1').fetchall()
                    for table in tables} == before
    finally:
        _close(reopened)


@pytest.mark.parametrize('lifecycle,stability,origin,misaligned,generation,valid', [
    ('queued', None, None, 0, 0, True),
    ('running', None, None, 0, 0, True),
    ('running', 'stable', None, 0, 2, True),
    ('running', 'unstable', 'int-live', 0, 3, True),
    ('running', 'unstable', 'int-done', 0, 4, True),
    ('sampled', 'stable', None, 0, 0, True),
    ('sampled', 'stable', None, 1, 2, True),
    ('sampled', 'unstable', 'int-done', 1, 3, True),
    ('done', 'stable', None, 1, 5, True),
    ('done', 'unstable', 'int-done', 0, 1, True),
    ('done', 'unstable', ' int-spaced ', 0, 1, True),
    ('failed', None, None, 0, 0, True),
    ('failed', None, None, 1, 2, True),
    ('waiting', None, None, 0, 0, False),
    ('cancelled', None, None, 0, 0, False),
    ('skipped', None, None, 0, 0, False),
    ('queued', 'stable', None, 0, 0, False),
    ('queued', 'unstable', 'int-live', 0, 0, False),
    ('queued', None, 'int-live', 0, 0, False),
    ('queued', None, None, 1, 0, False),
    ('running', 'stable', 'int-live', 0, 0, False),
    ('running', 'unstable', None, 0, 0, False),
    ('running', None, None, 1, 0, False),
    ('running', None, 'int-live', 0, 0, False),
    ('sampled', None, None, 0, 0, False),
    ('done', None, None, 0, 0, False),
    ('done', 'stable', 'int-live', 0, 0, False),
    ('done', 'unstable', None, 0, 0, False),
    ('failed', 'stable', None, 0, 0, False),
    ('failed', 'unstable', 'int-live', 0, 0, False),
    ('failed', None, 'int-live', 0, 0, False),
    ('done', 'unknown', None, 0, 0, False),
    ('done', 'unstable', '', 0, 0, False),
    ('done', 'unstable', ' ', 0, 0, False),
    ('done', 'unstable', '\t\r\n', 0, 0, False),
    ('done', 'unstable', '\u2003', 0, 0, False),
    ('done', 'unstable', 'main-1', 0, 0, False),
    ('done', 'unstable', 'missing-session', 0, 0, False),
    ('queued', None, None, 2, 0, False),
    ('queued', None, None, 'invalid', 0, False),
    ('queued', None, None, 0, -1, False),
    ('queued', None, None, 0, 0.5, False),
    ('queued', None, None, 0, 'invalid', False),
])
def test_component_state_reader_preserves_valid_lineage_and_refuses_damage(
    tmp_path, lifecycle, stability, origin, misaligned, generation, valid,
):
    snapshot = ComponentTopology(nx.DiGraph([('A', 'B')]), []).snapshot()
    storage = FileStorage._create_new_project_state(tmp_path)
    try:
        storage.register_component_topology(snapshot)
        sessions = [('int-live', 'interrupt'), ('int-done', 'interrupt'), ('main-1', 'main')]
        if origin == ' int-spaced ':
            sessions.append((origin, 'interrupt'))
        for session_id, kind in sessions:
            storage.create_execution_session(
                session_id, session_kind=kind, command='run', start_component=('A',),
                selected_components=[('A',)], started_at='2026-09-05T12:00:00+00:00',
                hostname='worker.example', pid=123, process_identity=session_id,
            )
        storage.finish_execution_session(
            'int-done', outcome='done', finished_at='2026-09-05T12:01:00+00:00',
        )
        # Seed later states and damage that this read-only slice cannot create.
        # The disposable connection alone disables CHECK/FK enforcement.
        with sqlite3.connect(storage.state_database_path()) as connection:
            connection.execute('PRAGMA foreign_keys=ON')
            if not valid:
                connection.execute('PRAGMA ignore_check_constraints=ON')
                connection.execute('PRAGMA foreign_keys=OFF')
                if origin in (' ', '\t\r\n', '\u2003'):
                    connection.execute(
                        "UPDATE execution_sessions SET session_id=? WHERE session_id='int-live'", (origin,),
                    )
            connection.execute(
                'UPDATE component_states SET lifecycle=?, stability=?, instability_origin=?, '
                'misaligned=?, alignment_generation=? WHERE component_key=?',
                (lifecycle, stability, origin, misaligned, generation, '["A"]'),
            )
        before = tuple(storage.db_connection().execute(
            'SELECT * FROM component_states WHERE component_key=?', ('["A"]',),
        ).fetchone())
        if valid:
            assert storage.get_component_state(('A',)) == {
                'members': ('A',), 'shape_json': snapshot.shape_json,
                'lifecycle': lifecycle, 'stability': stability, 'instability_origin': origin,
                'misaligned': bool(misaligned), 'alignment_generation': generation,
            }
        else:
            with pytest.raises(RuntimeError, match='[Ii]nvalid component state'):
                storage.get_component_state(('A',))
        assert tuple(storage.db_connection().execute(
            'SELECT * FROM component_states WHERE component_key=?', ('["A"]',),
        ).fetchone()) == before
    finally:
        _close(storage)


@pytest.mark.parametrize('lifecycle,stability,origin,misaligned', [
    ('queued', 'stable', None, 0),
    ('queued', 'unstable', 'int-1', 0),
    ('queued', None, None, 1),
    ('running', 'stable', 'int-1', 0),
    ('running', 'unstable', None, 0),
    ('running', None, None, 1),
    ('sampled', None, None, 0),
    ('done', None, None, 0),
    ('done', 'stable', 'int-1', 0),
    ('failed', 'stable', None, 0),
    ('failed', None, 'int-1', 0),
])
def test_component_state_schema_refuses_impossible_result_combinations(
    tmp_path, lifecycle, stability, origin, misaligned,
):
    snapshot = ComponentTopology(nx.DiGraph([('A', 'B')]), []).snapshot()
    storage = FileStorage._create_new_project_state(tmp_path)
    try:
        storage.register_component_topology(snapshot)
        storage.create_execution_session(
            'int-1', session_kind='interrupt', command='run', start_component=('A',),
            selected_components=[('A',)], started_at='2026-09-05T12:00:00+00:00',
            hostname='worker.example', pid=123, process_identity='instance-1',
        )
        before = storage.get_component_state(('A',))
        with pytest.raises(sqlite3.IntegrityError):
            storage.submit_db_mutation(lambda connection: connection.execute(
                'UPDATE component_states SET lifecycle=?, stability=?, instability_origin=?, '
                'misaligned=? WHERE component_key=?',
                (lifecycle, stability, origin, misaligned, '["A"]'),
            ))
        assert storage.get_component_state(('A',)) == before
        assert storage.database_integrity_check() == 'ok'
    finally:
        _close(storage)


def test_component_state_keeps_producing_shape_and_separate_split_identities(tmp_path):
    graph = nx.DiGraph([('A', 'B')])
    merged = ComponentTopology(graph, [('A', 'B')]).snapshot()
    split = ComponentTopology(graph, []).snapshot()
    changed = ComponentTopology(nx.DiGraph([('A', 'B'), ('B', 'C')]), [('A', 'B')]).snapshot()
    storage = FileStorage._create_new_project_state(tmp_path)
    try:
        storage.register_component_topology(merged)
        storage.submit_db_mutation(lambda connection: connection.execute(
            "UPDATE component_states SET lifecycle='done', stability='stable'"
        ))
        result = storage.get_component_state(('A', 'B'))
        with pytest.raises(RuntimeError, match='different graph shape'):
            storage.register_component_topology(changed)
        assert storage.get_component_state(('A', 'B')) == result
        assert storage.get_component_state(('C',)) is None
        assert storage.register_component_topology(split) is True
        for members in [('A',), ('B',)]:
            assert storage.get_component_state(members) == {
                'members': members, 'shape_json': split.shape_json,
                'lifecycle': 'queued', 'stability': None, 'instability_origin': None,
                'misaligned': False, 'alignment_generation': 0,
            }
        assert storage.register_component_topology(merged) is False
        assert storage.get_component_state(('A', 'B')) == result
    finally:
        _close(storage)


def test_component_state_reader_refuses_version4_without_changing_raw_node_state(tmp_path):
    storage = FileStorage(tmp_path)
    try:
        storage.set_node_status('A', 'done')
        output = storage.node_output_dir('A') / 'user.txt'
        output.write_bytes(b'preserved user output')
        before = tuple(storage.db_connection().execute(
            "SELECT * FROM nodes WHERE node_name='A'"
        ).fetchone())
        with pytest.raises(RuntimeError, match='session-capable database'):
            storage.get_component_state(('A',))
        assert storage.get_node_status('A') == 'done'
        assert tuple(storage.db_connection().execute(
            "SELECT * FROM nodes WHERE node_name='A'"
        ).fetchone()) == before
        assert output.read_bytes() == b'preserved user output'
        assert storage.db_connection().execute(
            "SELECT name FROM sqlite_master WHERE name='component_states'"
        ).fetchone() is None
        assert storage.db_connection().execute(
            "SELECT value FROM metadata WHERE key='database_schema_version'"
        ).fetchone()[0] == '4'
    finally:
        _close(storage)


@pytest.mark.parametrize('damage', ['missing-table', 'origin-reference', 'boolean-check',
                                   'generation-check', 'lifecycle-check'])
def test_private_version5_component_state_declarations_refuse_silent_upgrade(tmp_path, damage):
    storage = FileStorage._create_new_project_state(tmp_path)
    database = storage.state_database_path()
    _close(storage)
    with sqlite3.connect(database) as connection:
        declaration = connection.execute(
            "SELECT sql FROM sqlite_master WHERE name='component_states'"
        ).fetchone()[0]
        connection.execute('DROP TABLE component_states')
        if damage != 'missing-table':
            if damage == 'origin-reference':
                modified = declaration.replace(' REFERENCES execution_sessions(session_id)', '')
            elif damage == 'boolean-check':
                modified = declaration.replace("typeof(misaligned)='integer' AND ", '')
            elif damage == 'generation-check':
                modified = declaration.replace("typeof(alignment_generation)='integer' AND ", '')
            else:
                modified = re.sub(r'CHECK\(\([\s\S]*?\) IS 1\)', 'CHECK(1)', declaration, count=1)
            assert modified != declaration, 'The requested declaration damage was not applied'
            connection.execute(modified)
        before = connection.execute('SELECT type,name,sql FROM sqlite_master ORDER BY type,name').fetchall()
    # Old-version state is opened by a new process. Existing live storage
    # instances deliberately cache schema initialization for their process.
    script = '''
import sys
from micro_workflow_manager.storage import FileStorage
try:
    FileStorage(sys.argv[1])
except RuntimeError as error:
    assert 'Incomplete SQLite execution-session schema' in str(error), str(error)
else:
    raise AssertionError('Incomplete private version-5 state was accepted')
'''
    result = subprocess.run([sys.executable, '-c', script, str(tmp_path)],
                            capture_output=True, text=True, timeout=20)
    assert result.returncode == 0, result.stdout + result.stderr
    with sqlite3.connect(database) as connection:
        assert connection.execute('SELECT type,name,sql FROM sqlite_master ORDER BY type,name').fetchall() == before
        assert connection.execute("SELECT value FROM metadata WHERE key='database_schema_version'").fetchone()[0] == '5'
