from __future__ import annotations

import json
import sqlite3
import subprocess
import sys
import time
from dataclasses import replace

import networkx as nx
import pytest

from micro_workflow_manager.storage import FileStorage
from micro_workflow_manager.topology import ComponentTopology


def test_storage_open_preserves_lightweight_graph_free_imports(tmp_path):
    script = '''
import sys
from micro_workflow_manager.storage import FileStorage
assert 'networkx' not in sys.modules, 'Storage import loaded graph dependencies'
assert 'micro_workflow_manager.topology' not in sys.modules
storage = FileStorage(sys.argv[1])
assert 'networkx' not in sys.modules, 'Ordinary storage creation loaded graph dependencies'
assert storage.db_connection().execute("SELECT value FROM metadata WHERE key='database_schema_version'").fetchone()[0] == '5'
assert storage.database_integrity_check() == 'ok'
storage.close_database_connections()
'''
    result = subprocess.run([sys.executable, '-c', script, str(tmp_path)],
                            capture_output=True, text=True, timeout=20)
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize('members', [('B', 'A'), ('A', 'B'), ('B', 'A', 'B')])
def test_component_identity_and_session_scope_keep_exact_sorted_members(tmp_path, members):
    topology = ComponentTopology(nx.DiGraph([('A', 'B')]), [('A', 'B')])
    assert topology.component_key(members) == ('A', 'B')
    storage = FileStorage._create_new_project_state(tmp_path)
    storage.create_execution_session(
        'int-17', session_kind='interrupt', command='run',
        start_component=members, selected_components=[members],
        started_at='2026-09-05T12:00:00+00:00', hostname='worker.example',
        pid=123, process_identity='instance-1',
    )
    storage.close_database_connections()

    reopened = FileStorage(tmp_path)
    session = reopened.get_execution_session('int-17')
    assert session['start_component'] == ('A', 'B')
    assert session['selected_components'] == [('A', 'B')]
    reopened.close_database_connections()


def _definition_rows(storage):
    connection = storage.db_connection()
    return {
        'shapes': [tuple(row) for row in connection.execute('SELECT shape_json FROM graph_shapes ORDER BY shape_json')],
        'components': [tuple(row) for row in connection.execute(
            'SELECT * FROM component_definitions ORDER BY component_key'
        )],
    }


def _session(storage, session_id, components, *, kind='interrupt'):
    return storage.create_execution_session(
        session_id, session_kind=kind, command='run',
        start_component=components[0], selected_components=components,
        started_at='2026-09-05T12:00:00+00:00', hostname='worker.example',
        pid=123, process_identity='instance-1',
    )


@pytest.mark.parametrize('reverse', [False, True])
def test_graph_shape_preserves_exact_topology_without_declaration_order(reverse):
    nodes = ['Isolated', 'C', 'B', 'A']
    edges = [('B', 'C'), ('A', 'B'), ('A', 'B')]
    autostart = [('A', 'B'), ('A', 'B')]
    if reverse:
        nodes.reverse()
        edges.reverse()
        autostart.reverse()
    graph = nx.DiGraph()
    graph.add_nodes_from(nodes)
    graph.add_edges_from(edges)
    topology = ComponentTopology(graph, autostart)
    before = (list(graph.nodes), list(graph.edges), list(autostart))

    shape = topology.graph_shape()

    assert json.loads(shape) == {
        'nodes': ['A', 'B', 'C', 'Isolated'],
        'edges': [['A', 'B'], ['B', 'C']],
        'autostart_edges': [['A', 'B']],
    }
    equivalent = nx.DiGraph([('A', 'B'), ('B', 'C')])
    equivalent.add_node('Isolated')
    assert ComponentTopology(equivalent, [('A', 'B')]).graph_shape() == shape
    assert (list(graph.nodes), list(graph.edges), list(autostart)) == before


def test_graph_shape_changes_with_raw_nodes_edges_and_effective_autostart():
    graph = nx.DiGraph([('A', 'B'), ('B', 'C')])
    autostart = []
    topology = ComponentTopology(graph, autostart)
    original = topology.graph_shape()
    graph.add_node('Isolated')
    with_node = topology.graph_shape()
    graph.add_edge('A', 'C')
    with_edge = topology.graph_shape()
    autostart.append(('A', 'B'))
    with_autostart = topology.graph_shape()
    assert len({original, with_node, with_edge, with_autostart}) == 4

    # Current component calculation ignores declarations absent from the raw graph.
    autostart.extend([('Absent', 'C'), ('C', 'A'), ('A', 'B')])
    assert topology.graph_shape() == with_autostart
    assert {topology.component_key(c) for c in topology.hoeflein_components()} == {
        ('A', 'B'), ('C',), ('Isolated',),
    }


def test_topology_snapshot_keeps_components_and_shape_from_the_same_observation():
    graph = nx.DiGraph([('A', 'B'), ('B', 'C')])
    autostart = [('A', 'B')]
    topology = ComponentTopology(graph, autostart)
    captured = topology.snapshot()

    graph.add_edge('C', 'A')
    autostart.clear()

    assert captured.components == (('A', 'B'), ('C',))
    assert json.loads(captured.shape_json) == {
        'nodes': ['A', 'B', 'C'],
        'edges': [['A', 'B'], ['B', 'C']],
        'autostart_edges': [['A', 'B']],
    }
    assert topology.snapshot().components == (('A', 'B', 'C'),)


def test_private_component_definitions_keep_producing_shape_after_reopen(tmp_path):
    graph = nx.DiGraph([('A', 'B'), ('B', 'C')])
    graph.add_node('Isolated')
    snapshot = ComponentTopology(graph, [('A', 'B')]).snapshot()
    storage = FileStorage._create_new_project_state(tmp_path)

    assert storage.register_component_topology(snapshot) is True
    assert storage.register_component_topology(snapshot) is False
    storage.close_database_connections()

    reopened = FileStorage(tmp_path)
    for component in [('A', 'B'), ('C',), ('Isolated',)]:
        assert reopened.get_component_definition(component) == {
            'members': component, 'shape_json': snapshot.shape_json,
        }
    assert reopened.get_component_definition(('B', 'A', 'B')) == {
        'members': ('A', 'B'), 'shape_json': snapshot.shape_json,
    }
    assert reopened.get_component_definition(('A',)) is None
    assert not (tmp_path / '.mwf' / 'run.json').exists()
    assert not (tmp_path / '.mwf_run.json').exists()
    assert reopened.database_integrity_check() == 'ok'
    reopened.close_database_connections()


def test_many_component_definitions_keep_bounded_storage_and_exact_shapes(tmp_path):
    graph = nx.DiGraph()
    graph.add_nodes_from(f'N{position:04d}' for position in range(1024))
    snapshot = ComponentTopology(graph, []).snapshot()
    storage = FileStorage._create_new_project_state(tmp_path)
    assert storage.register_component_topology(snapshot)
    storage.db_mutation_barrier()
    database = storage.state_database_path()
    storage.close_database_connections()

    # A small graph must not duplicate its full declaration for every component.
    with sqlite3.connect(database) as connection:
        connection.execute('PRAGMA wal_checkpoint(TRUNCATE)')
    assert database.stat().st_size < 2 * 1024 * 1024

    reopened = FileStorage(tmp_path)
    for members in snapshot.components:
        assert reopened.get_component_definition(members) == {
            'members': members, 'shape_json': snapshot.shape_json,
        }
        assert reopened.get_component_state(members) == {
            'members': members, 'shape_json': snapshot.shape_json,
            'lifecycle': 'queued', 'stability': None, 'instability_origin': None,
            'misaligned': False, 'alignment_generation': 0,
        }
    assert reopened.register_component_topology(snapshot) is False
    assert reopened.database_integrity_check() == 'ok'
    reopened.close_database_connections()


def test_registration_refuses_an_existing_exact_component_under_another_shape_atomically(tmp_path):
    old = ComponentTopology(nx.DiGraph([('A', 'B')]), [('A', 'B')]).snapshot()
    changed = ComponentTopology(nx.DiGraph([('A', 'B'), ('B', 'C')]), [('A', 'B')]).snapshot()
    storage = FileStorage._create_new_project_state(tmp_path)
    storage.register_component_topology(old)
    before = _definition_rows(storage)

    with pytest.raises(RuntimeError, match='different graph shape') as refused:
        storage.register_component_topology(changed)

    assert 'A' in str(refused.value) and 'B' in str(refused.value)
    assert _definition_rows(storage) == before
    assert storage.get_component_definition(('C',)) is None
    storage.close_database_connections()


def test_component_registration_rolls_back_shape_and_all_definitions_on_write_failure(tmp_path):
    snapshot = ComponentTopology(nx.DiGraph([('A', 'B'), ('B', 'C')]), [('A', 'B')]).snapshot()
    storage = FileStorage._create_new_project_state(tmp_path)
    with sqlite3.connect(storage.state_database_path()) as connection:
        connection.execute('''
            CREATE TRIGGER fail_component_registration BEFORE INSERT ON component_definitions
            WHEN NEW.component_key='["C"]'
            BEGIN SELECT RAISE(ABORT, 'injected definition failure'); END
        ''')
    before = _definition_rows(storage)

    with pytest.raises(sqlite3.IntegrityError, match='injected definition failure'):
        storage.register_component_topology(snapshot)

    assert _definition_rows(storage) == before == {'shapes': [], 'components': []}
    with sqlite3.connect(storage.state_database_path()) as connection:
        connection.execute('DROP TRIGGER fail_component_registration')
    assert storage.register_component_topology(snapshot) is True
    storage.close_database_connections()


@pytest.mark.parametrize('components', [
    (('A',), ('B',), ('C',)),
    (('A', 'B'), ('B', 'C')),
    (('A', 'B'),),
    (('A', 'B'), ('C',), ('D',)),
    ((), ('A', 'B'), ('C',)),
    (('A', 'B'), ('A', 'B'), ('C',)),
    (('B', 'A'), ('C',)),
])
def test_registration_rejects_component_partitions_that_do_not_match_the_snapshot(tmp_path, components):
    valid = ComponentTopology(nx.DiGraph([('A', 'B'), ('B', 'C')]), [('A', 'B')]).snapshot()
    damaged = replace(valid, components=components)
    storage = FileStorage._create_new_project_state(tmp_path)

    with pytest.raises(ValueError, match='snapshot'):
        storage.register_component_topology(damaged)

    assert _definition_rows(storage) == {'shapes': [], 'components': []}
    storage.close_database_connections()


@pytest.mark.parametrize('shape_json', [
    '{broken',
    '[]',
    '{"nodes":["A","B"],"edges":[["A","B"]]}',
    '{"nodes":"AB","edges":[["A","B"]],"autostart_edges":[["A","B"]]}',
    '{"nodes":["A","B"],"edges":[["A","Outside"]],"autostart_edges":[]}',
    '{"nodes":["A","B"],"edges":[["A","B"]],"autostart_edges":[["B","A"]]}',
    '{"nodes":["A","../B"],"edges":[["A","../B"]],"autostart_edges":[]}',
    '{"nodes":["A",false],"edges":[],"autostart_edges":[]}',
    '{"nodes":["A","B"],"edges":[["A"]],"autostart_edges":[]}',
])
def test_registration_rejects_malformed_or_inconsistent_graph_shapes(tmp_path, shape_json):
    valid = ComponentTopology(nx.DiGraph([('A', 'B')]), [('A', 'B')]).snapshot()
    storage = FileStorage._create_new_project_state(tmp_path)

    with pytest.raises(ValueError):
        storage.register_component_topology(replace(valid, shape_json=shape_json))

    assert _definition_rows(storage) == {'shapes': [], 'components': []}
    storage.close_database_connections()


def test_component_operations_do_not_upgrade_ordinary_version_four_storage(tmp_path):
    storage = FileStorage(tmp_path)
    snapshot = ComponentTopology(nx.DiGraph([('A', 'B')]), []).snapshot()
    connection = storage.db_connection()
    before = [tuple(row) for row in connection.execute('SELECT * FROM metadata ORDER BY key')]

    with pytest.raises(RuntimeError, match='session-capable database'):
        storage.register_component_topology(snapshot)
    with pytest.raises(RuntimeError, match='session-capable database'):
        storage.get_component_definition(('A',))
    for operation in [
        lambda: storage.reserve_execution_components('int-17', expected_shape=snapshot.shape_json),
        lambda: storage.get_component_reservation(('A',)),
        lambda: storage.release_execution_components('int-17'),
        lambda: storage.acquire_component_holds('int-17', [('A',)]),
        lambda: storage.get_component_holds(('A',)),
        lambda: storage.release_component_holds('int-17', [('A',)]),
    ]:
        with pytest.raises(RuntimeError, match='session-capable database'):
            operation()

    assert [tuple(row) for row in connection.execute('SELECT * FROM metadata ORDER BY key')] == before
    assert connection.execute("SELECT value FROM metadata WHERE key='database_schema_version'").fetchone()[0] == '4'
    tables = {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert {'graph_shapes', 'component_definitions', 'component_reservations', 'component_holds'}.isdisjoint(tables)
    assert not (tmp_path / '.mwf' / 'run.json').exists()
    assert not (tmp_path / '.mwf_run.json').exists()
    storage.close_database_connections()


@pytest.mark.parametrize('damage', [
    'older-private-shape', 'missing-shape-reference', 'missing-reservation-reference', 'missing-hold-check',
    'missing-owner-session-reference', 'missing-owner-component-reference',
    'missing-shape-uniqueness', 'missing-shape-identifier', 'older-private-json-reference',
])
def test_component_schema_damage_refuses_in_a_new_process_without_importing_legacy_data(tmp_path, damage):
    storage = FileStorage._create_new_project_state(tmp_path)
    storage.close_database_connections()
    with sqlite3.connect(tmp_path / '.mwf' / 'state.sqlite3') as connection:
        if damage in {'older-private-shape', 'missing-shape-reference', 'older-private-json-reference'}:
            connection.execute('DROP TABLE component_definitions')
        if damage == 'older-private-shape':
            connection.execute('DROP TABLE graph_shapes')
        elif damage == 'missing-shape-reference':
            connection.execute('CREATE TABLE component_definitions(component_key TEXT PRIMARY KEY, shape_id INTEGER NOT NULL)')
        elif damage in {'missing-shape-uniqueness', 'missing-shape-identifier'}:
            connection.execute('DROP TABLE graph_shapes')
            primary = '' if damage == 'missing-shape-identifier' else ' PRIMARY KEY'
            unique = '' if damage == 'missing-shape-uniqueness' else ' UNIQUE'
            connection.execute(f'CREATE TABLE graph_shapes(shape_id INTEGER{primary}, shape_json TEXT NOT NULL{unique})')
        elif damage == 'older-private-json-reference':
            connection.execute('DROP TABLE graph_shapes')
            connection.execute('CREATE TABLE graph_shapes(shape_json TEXT PRIMARY KEY)')
            connection.execute('''CREATE TABLE component_definitions(
                component_key TEXT PRIMARY KEY, shape_json TEXT NOT NULL REFERENCES graph_shapes(shape_json))''')
        elif damage == 'missing-reservation-reference':
            connection.execute('DROP TABLE component_reservations')
            connection.execute('''CREATE TABLE component_reservations(
                component_key TEXT PRIMARY KEY, session_id TEXT NOT NULL REFERENCES execution_sessions(session_id))''')
        elif damage == 'missing-hold-check':
            connection.execute('DROP TABLE component_holds')
            connection.execute('''CREATE TABLE component_holds(
                session_id TEXT NOT NULL REFERENCES execution_sessions(session_id),
                component_key TEXT NOT NULL REFERENCES component_definitions(component_key),
                hold_count INTEGER NOT NULL, PRIMARY KEY(session_id, component_key))''')
        else:
            original = connection.execute(
                "SELECT sql FROM sqlite_master WHERE name='job_execution_owners'"
            ).fetchone()[0]
            connection.execute('DROP TABLE job_execution_owners')
            reference = (' REFERENCES execution_sessions(session_id)'
                         if damage == 'missing-owner-session-reference'
                         else ' REFERENCES component_definitions(component_key)')
            changed = original.replace(reference, '')
            assert changed != original, 'Damage fixture did not remove the named reference'
            connection.execute(changed)
    legacy = tmp_path / 'node' / 'A' / 'node_state.json'
    legacy.parent.mkdir(parents=True)
    legacy.write_bytes(b'{"status":"failed"}')
    script = '''
import sys
from micro_workflow_manager.storage import FileStorage
try:
    FileStorage(sys.argv[1])
except RuntimeError as error:
    assert 'Incomplete SQLite execution-session schema' in str(error), str(error)
else:
    raise AssertionError('Incomplete private component schema was accepted')
'''
    result = subprocess.run([sys.executable, '-c', script, str(tmp_path)],
                            capture_output=True, text=True, timeout=20)
    assert result.returncode == 0, result.stderr
    assert legacy.read_bytes() == b'{"status":"failed"}'


@pytest.mark.parametrize('reverse', [False, True])
def test_split_and_merged_memberships_remain_distinct_definitions(reverse, tmp_path):
    combined = ComponentTopology(nx.DiGraph([('A', 'B')]), [('A', 'B')]).snapshot()
    split = ComponentTopology(nx.DiGraph([('A', 'B')]), []).snapshot()
    snapshots = [combined, split] if not reverse else [split, combined]
    storage = FileStorage._create_new_project_state(tmp_path)
    for snapshot in snapshots:
        storage.register_component_topology(snapshot)
    storage.close_database_connections()

    reopened = FileStorage(tmp_path)
    for component, shape in [(('A', 'B'), combined.shape_json), (('A',), split.shape_json), (('B',), split.shape_json)]:
        assert reopened.get_component_definition(component) == {'members': component, 'shape_json': shape}
    assert len(_definition_rows(reopened)['components']) == 3
    reopened.close_database_connections()


def test_session_reserves_its_whole_persisted_scope_and_keeps_exact_owners_after_reopen(tmp_path):
    snapshot = ComponentTopology(nx.DiGraph([('A', 'B'), ('B', 'C')]), [('A', 'B')]).snapshot()
    storage = FileStorage._create_new_project_state(tmp_path)
    storage.register_component_topology(snapshot)
    _session(storage, 'int-17', [('A', 'B'), ('C',)])

    assert storage.reserve_execution_components('int-17', expected_shape=snapshot.shape_json) is True
    assert storage.reserve_execution_components('int-17', expected_shape=snapshot.shape_json) is False
    storage.close_database_connections()

    reopened = FileStorage(tmp_path)
    for component in [('A', 'B'), ('C',)]:
        assert reopened.get_component_reservation(component) == {'members': component, 'session_id': 'int-17'}
    assert reopened.get_component_reservation(('Outside',)) is None
    assert reopened.get_execution_session('int-17')['selected_components'] == [('A', 'B'), ('C',)]
    assert not (tmp_path / '.mwf' / 'run.json').exists()
    reopened.close_database_connections()


def test_reservation_conflict_reports_every_exact_owner_and_grants_no_partial_scope(tmp_path):
    graph = nx.DiGraph([('A', 'B'), ('B', 'C')])
    graph.add_node('D')
    snapshot = ComponentTopology(graph, [('A', 'B')]).snapshot()
    storage = FileStorage._create_new_project_state(tmp_path)
    storage.register_component_topology(snapshot)
    for session_id, scope in [('int-17', [('A', 'B')]), ('int-19', [('D',)]),
                              ('int-18', [('A', 'B'), ('C',), ('D',)])]:
        _session(storage, session_id, scope)
    storage.reserve_execution_components('int-17', expected_shape=snapshot.shape_json)
    storage.reserve_execution_components('int-19', expected_shape=snapshot.shape_json)
    storage.heartbeat_execution_session('int-17', '2000-01-01T00:00:00+00:00')

    with pytest.raises(RuntimeError) as refused:
        storage.reserve_execution_components('int-18', expected_shape=snapshot.shape_json)

    assert refused.value.conflicts == ((('A', 'B'), 'int-17'), (('D',), 'int-19'))
    assert 'int-17' in str(refused.value) and 'int-19' in str(refused.value)
    assert storage.get_component_reservation(('C',)) is None
    assert storage.get_component_reservation(('A', 'B'))['session_id'] == 'int-17'
    assert storage.get_component_reservation(('D',))['session_id'] == 'int-19'
    assert storage.get_execution_session('int-17')['heartbeat_at'] == '2000-01-01T00:00:00+00:00'
    storage.close_database_connections()


@pytest.mark.parametrize('known', [False, True])
def test_only_a_persisted_running_session_can_acquire_reservations(tmp_path, known):
    snapshot = ComponentTopology(nx.DiGraph([('A', 'B')]), []).snapshot()
    storage = FileStorage._create_new_project_state(tmp_path)
    storage.register_component_topology(snapshot)
    if known:
        _session(storage, 'int-17', [('A',), ('B',)])
        storage.finish_execution_session('int-17', outcome='done', finished_at='2026-09-05T12:01:00+00:00')

    with pytest.raises(RuntimeError, match='running session'):
        storage.reserve_execution_components('int-17', expected_shape=snapshot.shape_json)

    assert storage.get_component_reservation(('A',)) is None
    assert storage.get_component_reservation(('B',)) is None
    storage.close_database_connections()


def test_partial_same_session_reservation_refuses_as_damaged_state_without_filling_it(tmp_path):
    snapshot = ComponentTopology(nx.DiGraph([('A', 'B')]), []).snapshot()
    storage = FileStorage._create_new_project_state(tmp_path)
    storage.register_component_topology(snapshot)
    _session(storage, 'int-17', [('A',), ('B',)])
    with sqlite3.connect(storage.state_database_path()) as connection:
        connection.execute('INSERT INTO component_reservations VALUES(?, ?)', ('["A"]', 'int-17'))

    with pytest.raises(RuntimeError, match='partial reservation'):
        storage.reserve_execution_components('int-17', expected_shape=snapshot.shape_json)

    assert storage.get_component_reservation(('A',))['session_id'] == 'int-17'
    assert storage.get_component_reservation(('B',)) is None
    storage.close_database_connections()


def test_reservation_outside_immutable_session_scope_refuses_without_adding_selected_scope(tmp_path):
    snapshot = ComponentTopology(nx.DiGraph([('A', 'B')]), []).snapshot()
    storage = FileStorage._create_new_project_state(tmp_path)
    storage.register_component_topology(snapshot)
    _session(storage, 'int-17', [('A',)])
    with sqlite3.connect(storage.state_database_path()) as connection:
        connection.execute('INSERT INTO component_reservations VALUES(?, ?)', ('["B"]', 'int-17'))

    with pytest.raises(RuntimeError, match='damaged'):
        storage.reserve_execution_components('int-17', expected_shape=snapshot.shape_json)

    assert storage.get_component_reservation(('A',)) is None
    assert storage.get_component_reservation(('B',))['session_id'] == 'int-17'
    storage.close_database_connections()


@pytest.mark.parametrize('unknown_component', [False, True])
def test_reservation_requires_every_component_under_the_expected_shape(tmp_path, unknown_component):
    snapshot = ComponentTopology(nx.DiGraph([('A', 'B')]), []).snapshot()
    storage = FileStorage._create_new_project_state(tmp_path)
    storage.register_component_topology(snapshot)
    scope = [('A',), ('Outside',)] if unknown_component else [('A',), ('B',)]
    _session(storage, 'int-17', scope)
    expected_shape = snapshot.shape_json if unknown_component else 'another graph shape'

    with pytest.raises(RuntimeError, match='expected graph shape'):
        storage.reserve_execution_components('int-17', expected_shape=expected_shape)

    assert storage.get_component_reservation(('A',)) is None
    assert storage.get_component_reservation(('B',)) is None
    assert storage.get_component_reservation(('Outside',)) is None
    storage.close_database_connections()


def test_two_processes_racing_for_one_scope_produce_one_complete_owner(tmp_path):
    snapshot = ComponentTopology(nx.DiGraph([('A', 'B'), ('B', 'C')]), [('A', 'B')]).snapshot()
    storage = FileStorage._create_new_project_state(tmp_path)
    storage.register_component_topology(snapshot)
    for session_id in ['int-17', 'int-18']:
        _session(storage, session_id, [('A', 'B'), ('C',)])
    script = '''
import json, sys, time
from pathlib import Path
from micro_workflow_manager.storage import FileStorage
root, session_id, shape = Path(sys.argv[1]), sys.argv[2], sys.argv[3]
storage = FileStorage(root)
print('ready', flush=True)
deadline = time.monotonic() + 15
while not (root / 'start-reservations').exists():
    if time.monotonic() > deadline:
        raise RuntimeError('parent did not release reservation race')
    time.sleep(0.001)
try:
    storage.reserve_execution_components(session_id, expected_shape=shape)
except RuntimeError as error:
    assert error.conflicts, str(error)
    print(json.dumps({'result': 'refused', 'session': session_id}), flush=True)
else:
    print(json.dumps({'result': 'reserved', 'session': session_id}), flush=True)
finally:
    storage.close_database_connections()
'''
    processes = [subprocess.Popen(
        [sys.executable, '-c', script, str(tmp_path), session_id, snapshot.shape_json],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    ) for session_id in ['int-17', 'int-18']]
    outcomes = []
    try:
        for process in processes:
            assert process.stdout.readline().strip() == 'ready'
        (tmp_path / 'start-reservations').touch()
        for process in processes:
            stdout, stderr = process.communicate(timeout=20)
            assert process.returncode == 0, stderr
            outcomes.append(json.loads(stdout))
    finally:
        for process in processes:
            if process.poll() is None:
                process.kill()
                process.communicate(timeout=5)
    assert sorted(result['result'] for result in outcomes) == ['refused', 'reserved']
    winner = next(result['session'] for result in outcomes if result['result'] == 'reserved')
    assert storage.get_component_reservation(('A', 'B'))['session_id'] == winner
    assert storage.get_component_reservation(('C',))['session_id'] == winner
    storage.close_database_connections()


def test_failed_reservation_batch_retains_no_partial_scope_and_can_retry(tmp_path):
    snapshot = ComponentTopology(nx.DiGraph([('A', 'B'), ('B', 'C')]), [('A', 'B')]).snapshot()
    storage = FileStorage._create_new_project_state(tmp_path)
    storage.register_component_topology(snapshot)
    _session(storage, 'int-17', [('A', 'B'), ('C',)])
    with sqlite3.connect(storage.state_database_path()) as connection:
        connection.execute('''
            CREATE TRIGGER fail_reservation BEFORE INSERT ON component_reservations
            WHEN NEW.component_key='["C"]'
            BEGIN SELECT RAISE(ABORT, 'injected reservation failure'); END
        ''')
    with pytest.raises(sqlite3.IntegrityError, match='injected reservation failure'):
        storage.reserve_execution_components('int-17', expected_shape=snapshot.shape_json)
    assert storage.get_component_reservation(('A', 'B')) is None
    assert storage.get_component_reservation(('C',)) is None
    with sqlite3.connect(storage.state_database_path()) as connection:
        connection.execute('DROP TRIGGER fail_reservation')
    assert storage.reserve_execution_components('int-17', expected_shape=snapshot.shape_json)
    storage.close_database_connections()


def test_releasing_one_session_keeps_disjoint_owners_and_all_session_history(tmp_path):
    snapshot = ComponentTopology(nx.DiGraph([('A', 'B'), ('B', 'C')]), []).snapshot()
    storage = FileStorage._create_new_project_state(tmp_path)
    storage.register_component_topology(snapshot)
    main = _session(storage, 'main-1', [('A',)], kind='main')
    _session(storage, 'int-17', [('B',), ('C',)])
    storage.reserve_execution_components('main-1', expected_shape=snapshot.shape_json)
    storage.reserve_execution_components('int-17', expected_shape=snapshot.shape_json)
    storage.finish_execution_session('int-17', outcome='done', finished_at='2026-09-05T12:01:00+00:00')
    terminal = storage.get_execution_session('int-17')

    assert storage.release_execution_components('int-17') == 2
    assert storage.release_execution_components('int-17') == 0
    assert storage.release_execution_components('unknown') == 0

    assert storage.get_component_reservation(('A',)) == {'members': ('A',), 'session_id': 'main-1'}
    assert storage.get_component_reservation(('B',)) is None
    assert storage.get_component_reservation(('C',)) is None
    assert storage.get_execution_session('main-1') == main
    assert storage.get_execution_session('int-17') == terminal
    _session(storage, 'int-18', [('B',), ('C',)])
    assert storage.reserve_execution_components('int-18', expected_shape=snapshot.shape_json)
    storage.close_database_connections()


def test_shared_predecessor_holds_keep_per_session_counts_and_heartbeats_after_reopen(tmp_path):
    snapshot = ComponentTopology(nx.DiGraph([('P', 'A'), ('P', 'B')]), []).snapshot()
    storage = FileStorage._create_new_project_state(tmp_path)
    storage.register_component_topology(snapshot)
    _session(storage, 'int-17', [('A',)])
    _session(storage, 'int-18', [('B',)])

    assert storage.acquire_component_holds('int-17', [('P',)]) == {('P',): 1}
    assert storage.acquire_component_holds('int-17', [('P',)]) == {('P',): 2}
    assert storage.acquire_component_holds('int-18', [('P',)]) == {('P',): 1}
    storage.heartbeat_execution_session('int-17', '2026-09-05T12:01:00+00:00')
    storage.close_database_connections()

    reopened = FileStorage(tmp_path)
    assert reopened.get_component_holds(('P',)) == [
        {'session_id': 'int-17', 'count': 2, 'heartbeat_at': '2026-09-05T12:01:00+00:00'},
        {'session_id': 'int-18', 'count': 1, 'heartbeat_at': '2026-09-05T12:00:00+00:00'},
    ]
    assert reopened.get_component_holds(('A',)) == []
    assert reopened.get_execution_session('int-17')['selected_components'] == [('A',)]
    assert reopened.get_execution_session('int-18')['selected_components'] == [('B',)]
    assert not (tmp_path / '.mwf' / 'run.json').exists()
    reopened.close_database_connections()


@pytest.mark.parametrize('combined_first', [False, True])
def test_reservations_refuse_raw_node_overlap_across_split_and_merged_shapes(tmp_path, combined_first):
    graph = nx.DiGraph([('A', 'B')])
    combined = ComponentTopology(graph, [('A', 'B')]).snapshot()
    split = ComponentTopology(graph, []).snapshot()
    storage = FileStorage._create_new_project_state(tmp_path)
    for snapshot in [combined, split]:
        storage.register_component_topology(snapshot)
    first, second = (combined, split) if combined_first else (split, combined)
    owned = ('A', 'B') if combined_first else ('A',)
    _session(storage, 'int-17', [owned])
    _session(storage, 'int-18', second.components)
    storage.reserve_execution_components('int-17', expected_shape=first.shape_json)

    with pytest.raises(RuntimeError) as refused:
        storage.reserve_execution_components('int-18', expected_shape=second.shape_json)

    assert refused.value.conflicts == ((owned, 'int-17'),)
    assert storage.get_component_reservation(owned) == {'members': owned, 'session_id': 'int-17'}
    for component in second.components:
        assert storage.get_component_reservation(component) is None
    storage.close_database_connections()


@pytest.mark.parametrize('components', [[], [('P',)]])
@pytest.mark.parametrize('known', [False, True])
def test_only_a_persisted_running_session_can_acquire_holds(tmp_path, known, components):
    snapshot = ComponentTopology(nx.DiGraph([('P', 'A')]), []).snapshot()
    storage = FileStorage._create_new_project_state(tmp_path)
    storage.register_component_topology(snapshot)
    if known:
        _session(storage, 'int-17', [('A',)])
        storage.finish_execution_session('int-17', outcome='done', finished_at='2026-09-05T12:01:00+00:00')

    with pytest.raises(RuntimeError, match='running session'):
        storage.acquire_component_holds('int-17', components)

    assert storage.get_component_holds(('P',)) == []
    storage.close_database_connections()


def test_releasing_counted_holds_keeps_other_sessions_and_deletes_at_zero(tmp_path):
    snapshot = ComponentTopology(nx.DiGraph([('P', 'A'), ('P', 'B')]), []).snapshot()
    storage = FileStorage._create_new_project_state(tmp_path)
    storage.register_component_topology(snapshot)
    _session(storage, 'int-17', [('A',)])
    _session(storage, 'int-18', [('B',)])
    storage.acquire_component_holds('int-17', [('P',)])
    storage.acquire_component_holds('int-17', [('P',)])
    storage.acquire_component_holds('int-18', [('P',)])
    storage.finish_execution_session('int-17', outcome='done', finished_at='2026-09-05T12:01:00+00:00')

    assert storage.release_component_holds('int-17', [('P',)]) == {('P',): 1}
    assert storage.get_component_holds(('P',)) == [
        {'session_id': 'int-17', 'count': 1, 'heartbeat_at': '2026-09-05T12:00:00+00:00'},
        {'session_id': 'int-18', 'count': 1, 'heartbeat_at': '2026-09-05T12:00:00+00:00'},
    ]
    assert storage.release_component_holds('int-17', [('P',)]) == {('P',): 0}
    assert storage.release_component_holds('int-17', [('P',)]) == {('P',): 0}
    assert storage.release_component_holds('unknown', [('P',)]) == {('P',): 0}
    storage.close_database_connections()

    reopened = FileStorage(tmp_path)
    assert reopened.get_component_holds(('P',)) == [
        {'session_id': 'int-18', 'count': 1, 'heartbeat_at': '2026-09-05T12:00:00+00:00'},
    ]
    assert reopened.get_execution_session('int-17')['status'] == 'terminal'
    reopened.close_database_connections()


@pytest.mark.parametrize('operation', ['acquire_component_holds', 'release_component_holds'])
def test_one_hold_batch_rejects_duplicate_component_requests_without_changing_counts(tmp_path, operation):
    snapshot = ComponentTopology(nx.DiGraph([('P', 'Q'), ('Q', 'A')]), [('P', 'Q')]).snapshot()
    storage = FileStorage._create_new_project_state(tmp_path)
    storage.register_component_topology(snapshot)
    _session(storage, 'int-17', [('A',)])
    storage.acquire_component_holds('int-17', [('P', 'Q')])
    before = storage.get_component_holds(('P', 'Q'))

    with pytest.raises(ValueError, match='duplicate'):
        getattr(storage, operation)('int-17', [('Q', 'P'), ('P', 'Q')])

    assert storage.get_component_holds(('P', 'Q')) == before
    storage.close_database_connections()


@pytest.mark.parametrize('operation', ['acquire_component_holds', 'release_component_holds'])
def test_failed_hold_batch_preserves_all_counts_and_can_retry(tmp_path, operation):
    snapshot = ComponentTopology(nx.DiGraph([('P', 'A'), ('Q', 'A')]), []).snapshot()
    storage = FileStorage._create_new_project_state(tmp_path)
    storage.register_component_topology(snapshot)
    _session(storage, 'int-17', [('A',)])
    for _ in range(2):
        storage.acquire_component_holds('int-17', [('P',), ('Q',)])
    before = [storage.get_component_holds(component) for component in [('P',), ('Q',)]]
    event = 'INSERT' if operation == 'acquire_component_holds' else 'UPDATE'
    with sqlite3.connect(storage.state_database_path()) as connection:
        connection.execute(f'''
            CREATE TRIGGER fail_hold BEFORE {event} ON component_holds
            WHEN NEW.component_key='["Q"]'
            BEGIN SELECT RAISE(ABORT, 'injected hold failure'); END
        ''')

    with pytest.raises(sqlite3.IntegrityError, match='injected hold failure'):
        getattr(storage, operation)('int-17', [('P',), ('Q',)])

    assert [storage.get_component_holds(component) for component in [('P',), ('Q',)]] == before
    with sqlite3.connect(storage.state_database_path()) as connection:
        connection.execute('DROP TRIGGER fail_hold')
    expected = 3 if operation == 'acquire_component_holds' else 1
    assert getattr(storage, operation)('int-17', [('P',), ('Q',)]) == {('P',): expected, ('Q',): expected}
    storage.close_database_connections()


def test_unknown_component_rolls_back_hold_batch_and_reads_keep_stale_owners(tmp_path):
    snapshot = ComponentTopology(nx.DiGraph([('P', 'A')]), []).snapshot()
    storage = FileStorage._create_new_project_state(tmp_path)
    storage.register_component_topology(snapshot)
    _session(storage, 'int-17', [('A',)])

    with pytest.raises(sqlite3.IntegrityError):
        storage.acquire_component_holds('int-17', [('P',), ('Unknown',)])

    assert storage.get_component_holds(('P',)) == []
    assert storage.acquire_component_holds('int-17', []) == {}
    storage.acquire_component_holds('int-17', [('P',)])
    storage.heartbeat_execution_session('int-17', '2000-01-01T00:00:00+00:00')
    assert storage.get_component_holds(('P',)) == [
        {'session_id': 'int-17', 'count': 1, 'heartbeat_at': '2000-01-01T00:00:00+00:00'},
    ]
    assert storage.release_component_holds('int-17', []) == {}
    storage.close_database_connections()


def test_empty_topology_registers_once_and_reopens_without_component_rows(tmp_path):
    snapshot = ComponentTopology(nx.DiGraph(), []).snapshot()
    storage = FileStorage._create_new_project_state(tmp_path)
    assert storage.register_component_topology(snapshot) is True
    assert storage.register_component_topology(snapshot) is False
    storage.close_database_connections()

    reopened = FileStorage(tmp_path)
    assert _definition_rows(reopened) == {'shapes': [(snapshot.shape_json,)], 'components': []}
    assert reopened.database_integrity_check() == 'ok'
    reopened.close_database_connections()


@pytest.mark.parametrize('shape_json', [
    '{"nodes":["B","A"],"edges":[["A","B"]],"autostart_edges":[["A","B"]]}',
    '{"nodes":["A","B"],"edges":[["A","B"],["A","B"]],"autostart_edges":[["A","B"]]}',
    '{ "nodes": ["A", "B"], "edges": [["A", "B"]], "autostart_edges": [["A", "B"]] }',
])
def test_noncanonical_spelling_of_an_equivalent_shape_refuses_without_inserting(tmp_path, shape_json):
    snapshot = ComponentTopology(nx.DiGraph([('A', 'B')]), [('A', 'B')]).snapshot()
    storage = FileStorage._create_new_project_state(tmp_path)

    with pytest.raises(ValueError, match='canonical graph shape'):
        storage.register_component_topology(replace(snapshot, shape_json=shape_json))

    assert _definition_rows(storage) == {'shapes': [], 'components': []}
    storage.close_database_connections()


@pytest.mark.parametrize('same_shape', [False, True])
def test_two_processes_register_one_component_without_orphaning_a_conflicting_shape(tmp_path, same_shape):
    first = ComponentTopology(nx.DiGraph([('A', 'B')]), [('A', 'B')]).snapshot()
    second = first if same_shape else ComponentTopology(
        nx.DiGraph([('A', 'B'), ('B', 'C')]), [('A', 'B')],
    ).snapshot()
    storage = FileStorage._create_new_project_state(tmp_path)
    storage.close_database_connections()
    script = '''
import json, sys, time
from pathlib import Path
from micro_workflow_manager.storage import FileStorage
from micro_workflow_manager.topology import ComponentTopologySnapshot
root = Path(sys.argv[1])
snapshot = ComponentTopologySnapshot(sys.argv[2], tuple(tuple(c) for c in json.loads(sys.argv[3])))
storage = FileStorage(root)
(root / ('ready-' + sys.argv[4])).touch()
deadline = time.monotonic() + 15
while not (root / 'start-registration').exists():
    if time.monotonic() > deadline:
        raise RuntimeError('parent did not release registration race')
    time.sleep(0.001)
try:
    changed = storage.register_component_topology(snapshot)
except RuntimeError as error:
    assert 'different graph shape' in str(error), str(error)
    print(json.dumps({'result': 'refused', 'shape': snapshot.shape_json}), flush=True)
else:
    print(json.dumps({'result': 'inserted' if changed else 'unchanged', 'shape': snapshot.shape_json}), flush=True)
finally:
    storage.close_database_connections()
'''
    processes = [subprocess.Popen(
        [sys.executable, '-c', script, str(tmp_path), snapshot.shape_json, json.dumps(snapshot.components), str(position)],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    ) for position, snapshot in enumerate([first, second])]
    outcomes = []
    try:
        deadline = time.monotonic() + 15
        while not all((tmp_path / ('ready-' + str(position))).exists() for position in range(2)):
            assert time.monotonic() < deadline, 'Registration children did not become ready'
            assert all(process.poll() is None for process in processes), 'Registration child exited before race'
            time.sleep(0.001)
        (tmp_path / 'start-registration').touch()
        for process in processes:
            stdout, stderr = process.communicate(timeout=20)
            assert process.returncode == 0, stderr
            outcomes.append(json.loads(stdout))
    finally:
        for process in processes:
            if process.poll() is None:
                process.kill()
                process.communicate(timeout=5)
    assert sorted(outcome['result'] for outcome in outcomes) == ['inserted', 'unchanged' if same_shape else 'refused']
    winner = next(outcome['shape'] for outcome in outcomes if outcome['result'] == 'inserted')
    reopened = FileStorage(tmp_path)
    assert reopened.get_component_definition(('A', 'B'))['shape_json'] == winner
    assert _definition_rows(reopened)['shapes'] == [(winner,)]
    assert (reopened.get_component_definition(('C',)) is not None) == ('C' in json.loads(winner)['nodes'])
    assert reopened.database_integrity_check() == 'ok'
    reopened.close_database_connections()


@pytest.mark.parametrize(('members', 'stored_key'), [
    (('é', 'A', 'é'), '["A", "\\u00e9"]'),
    (('A, B', 'A'), '["A", "A, B"]'),
    (('B"quoted', 'A'), '["A", "B\\"quoted"]'),
])
def test_component_keys_keep_existing_bytes_across_every_persisted_association(tmp_path, members, stored_key):
    graph = nx.DiGraph([(members[0], members[1])])
    snapshot = ComponentTopology(graph, list(graph.edges)).snapshot()
    storage = FileStorage._create_new_project_state(tmp_path)
    storage.register_component_topology(snapshot)
    _session(storage, 'int-17', [members])
    storage.reserve_execution_components('int-17', expected_shape=snapshot.shape_json)
    storage.acquire_component_holds('int-17', [members])
    connection = storage.db_connection()

    assert connection.execute('SELECT start_component FROM execution_sessions').fetchone()[0] == stored_key
    for table in ['session_components', 'component_definitions', 'component_reservations', 'component_holds']:
        assert connection.execute(f'SELECT component_key FROM {table}').fetchone()[0] == stored_key

    storage.close_database_connections()
    reopened = FileStorage(tmp_path)
    expected = tuple(json.loads(stored_key))
    assert reopened.get_component_definition(members)['members'] == expected
    assert reopened.get_execution_session('int-17')['selected_components'] == [expected]
    assert reopened.get_component_reservation(members)['members'] == expected
    assert reopened.get_component_holds(members)[0]['count'] == 1
    reopened.close_database_connections()


def test_historical_combined_and_split_holds_keep_separate_counts_and_release_identity(tmp_path):
    graph = nx.DiGraph([('A', 'B')])
    storage = FileStorage._create_new_project_state(tmp_path)
    for autostart in [[('A', 'B')], []]:
        storage.register_component_topology(ComponentTopology(graph, autostart).snapshot())
    _session(storage, 'int-17', [('A', 'B')])
    _session(storage, 'int-18', [('A',)])
    storage.acquire_component_holds('int-17', [('A', 'B')])
    storage.acquire_component_holds('int-17', [('A', 'B')])
    storage.acquire_component_holds('int-18', [('A',)])
    assert storage.release_component_holds('int-17', [('A', 'B')]) == {('A', 'B'): 1}
    assert storage.get_component_holds(('A', 'B')) == [
        {'session_id': 'int-17', 'count': 1, 'heartbeat_at': '2026-09-05T12:00:00+00:00'},
    ]
    assert storage.get_component_holds(('A',)) == [
        {'session_id': 'int-18', 'count': 1, 'heartbeat_at': '2026-09-05T12:00:00+00:00'},
    ]
    storage.release_component_holds('int-17', [('A', 'B')])
    assert storage.get_component_holds(('A', 'B')) == []
    assert storage.get_component_holds(('A',))[0]['count'] == 1
    storage.close_database_connections()
