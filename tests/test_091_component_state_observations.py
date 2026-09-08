from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor
from threading import Event

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
    storage.close_database_connections()


def test_component_observations_keep_exact_records_and_requested_order(tmp_path):
    storage = FileStorage._create_new_project_state(tmp_path)
    topology = ComponentTopology(nx.DiGraph([('A', 'C'), ('B', 'C')]), []).snapshot()
    storage.register_component_topology(topology)
    try:
        before = [storage.get_component_state(component) for component in topology.components]
        observed = storage.read_component_states([('B',), ('A',)], expected_shape=topology.shape_json)
        assert list(observed) == [('B',), ('A',)]
        assert observed == {component: {
            'members': component, 'shape_json': topology.shape_json, 'lifecycle': 'queued',
            'stability': None, 'instability_origin': None, 'misaligned': False,
            'alignment_generation': 0,
        } for component in [('B',), ('A',)]}
        assert [storage.get_component_state(component) for component in topology.components] == before
        assert not storage.db_connection().in_transaction
    finally:
        _close(storage)


@pytest.mark.parametrize('allow_missing', [False, True])
def test_component_observations_use_one_snapshot_during_a_concurrent_change(tmp_path, allow_missing):
    storage = FileStorage._create_new_project_state(tmp_path)
    topology = ComponentTopology(nx.DiGraph([('A', 'C'), ('B', 'C')]), []).snapshot()
    storage.register_component_topology(topology)
    current_graph = nx.DiGraph([('A', 'C'), ('B', 'C')])
    if allow_missing:
        current_graph.add_node('absent')
    current = ComponentTopology(current_graph, []).snapshot()
    other = FileStorage(tmp_path)
    first_read, proceed = Event(), Event()
    trace_errors = []

    def observe():
        connection = storage.db_connection()
        state_reads = 0

        def trace(sql):
            nonlocal state_reads
            if 'FROM component_states AS state ' in sql:
                state_reads += 1
                if state_reads == 2:
                    first_read.set()
                    if not proceed.wait(10):
                        trace_errors.append('Concurrent observation was not released')

        connection.set_trace_callback(trace)
        try:
            requested = [('A',), ('B',), ('absent',)] if allow_missing else [('A',), ('B',)]
            options = {'allow_missing': True} if allow_missing else {}
            result = storage.read_component_states(requested, expected_shape=current.shape_json, **options)
            assert not connection.in_transaction
            return result
        finally:
            connection.set_trace_callback(None)

    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(observe)
            try:
                assert first_read.wait(5), 'Observation did not read its first component'
                other.submit_db_mutation(lambda connection: connection.execute(
                    "UPDATE component_states SET lifecycle='running', alignment_generation=1 "
                    "WHERE component_key IN ('[\"A\"]', '[\"B\"]')",
                ))
                assert other.get_component_state(('A',))['lifecycle'] == 'running'
                assert other.get_component_state(('B',))['lifecycle'] == 'running'
            finally:
                proceed.set()
            observed = future.result(timeout=15)
        assert not trace_errors
        assert [(observed[node]['lifecycle'], observed[node]['alignment_generation'])
                for node in [('A',), ('B',)]] == [
            ('queued', 0), ('queued', 0),
        ]
        if allow_missing:
            assert observed[('absent',)] is None
    finally:
        proceed.set()
        _close(other)
        _close(storage)


@pytest.mark.parametrize('outer_transaction', [False, True])
@pytest.mark.parametrize('allow_missing', [False, True])
@pytest.mark.parametrize('damage', ['none', 'wrong-shape', 'missing-component', 'missing-state', 'invalid-state'])
def test_component_observation_cleanup_preserves_the_callers_transaction(
    tmp_path, outer_transaction, allow_missing, damage,
):
    storage = FileStorage._create_new_project_state(tmp_path)
    topology = ComponentTopology(nx.DiGraph([('A', 'B')]), []).snapshot()
    storage.register_component_topology(topology)
    connection = storage.db_connection()
    requested = [('A',), ('B',)]
    shape = topology.shape_json
    if damage == 'wrong-shape':
        shape = topology.shape_json.replace('"B"', '"C"')
    elif damage == 'missing-component':
        requested.insert(1, ('C',))
        current_graph = nx.DiGraph([('A', 'B')])
        current_graph.add_node('C')
        shape = ComponentTopology(current_graph, []).snapshot().shape_json
    elif damage == 'missing-state':
        storage.submit_db_mutation(lambda writer: writer.execute(
            'DELETE FROM component_states WHERE component_key=?', ('["B"]',),
        ))
    elif damage == 'invalid-state':
        def corrupt(writer):
            writer.execute('PRAGMA ignore_check_constraints=ON')
            try:
                writer.execute("UPDATE component_states SET lifecycle='done', stability=NULL")
            finally:
                writer.execute('PRAGMA ignore_check_constraints=OFF')
        storage.submit_db_mutation(corrupt)
    try:
        if outer_transaction:
            connection.execute('BEGIN IMMEDIATE')
            connection.execute('UPDATE component_states SET alignment_generation=7')
        options = {'allow_missing': True} if allow_missing else {}
        if damage == 'none' or (damage == 'missing-component' and allow_missing):
            observed = storage.read_component_states(requested, expected_shape=shape, **options)
            assert list(observed) == requested
            assert {state['alignment_generation'] for state in observed.values() if state is not None} == {
                7 if outer_transaction else 0,
            }
            if damage == 'missing-component':
                assert observed[('C',)] is None
        else:
            with pytest.raises(RuntimeError):
                storage.read_component_states(requested, expected_shape=shape, **options)
        assert connection.in_transaction is outer_transaction
        if outer_transaction:
            connection.rollback()
        assert {row[0] for row in connection.execute('SELECT alignment_generation FROM component_states')} == {0}
        connection.execute('BEGIN')
        connection.rollback()
    finally:
        if connection.in_transaction:
            connection.rollback()
        _close(storage)
