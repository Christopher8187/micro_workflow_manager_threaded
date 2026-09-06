from __future__ import annotations

import os

import networkx as nx
import pytest

from micro_workflow_manager import MicroWorkflow
from micro_workflow_manager.models import Job, now
from micro_workflow_manager.storage import FileStorage
from micro_workflow_manager.topology import ComponentTopology
from tests.test_090_component_session_settlement import _close, _rows


def _storage(tmp_path, *, selected=True, pending=False):
    storage = FileStorage._create_new_project_state(tmp_path)
    topology = ComponentTopology(nx.DiGraph([('A', 'B'), ('B', 'A'), ('B', 'C')]), []).snapshot()
    storage.register_component_topology(topology)
    for job_id in (1, 2):
        storage.create_job(Job(node_name='A', job_id=job_id, params={}))
    storage.create_execution_session(
        'producing-session', session_kind='main', command='run_job' if selected else 'run_component',
        start_component=('A', 'B'), selected_components=[('A', 'B')],
        selected_jobs=[('A', 1)] if selected else [], started_at=now(),
        hostname='claim-test', pid=os.getpid(), process_identity='claim-test',
    )
    storage.reserve_execution_components('producing-session', expected_shape=topology.shape_json)
    storage.submit_db_mutation(lambda connection: connection.execute(
        'UPDATE component_states SET alignment_generation=7 WHERE component_key=?', ('["A", "B"]',),
    ))
    if pending:
        storage.begin_queued_component_execution(
            'producing-session', ('A', 'B'), expected_shape=topology.shape_json,
            expected_alignment_generation=7, successful_lineage=('stable', None),
        )
    return storage, topology


def _claim(storage, *, batch=False):
    arguments = dict(started_at=now(), session_id='producing-session', component=('A', 'B'))
    if batch:
        return storage.claim_job_executions_batch('A', [1, 2], **arguments)
    return [storage.claim_job_execution('A', 1, **arguments)]


@pytest.mark.parametrize('runner', ['direct', 'threaded', 'api'])
@pytest.mark.parametrize('selected', [False, True])
def test_public_execution_records_producing_identity_before_task_and_keeps_it_after_exit(tmp_path, runner, selected):
    workflow = MicroWorkflow(tmp_path, runner=runner, persist_graph=False)
    workflow.graph([('A', 'B'), ('B', 'A'), ('A', 'C')])
    storage = workflow.storage
    observed = []

    @workflow.task('A')
    def produce(ctx):
        owner = storage.read_job_current_owner('A', 1)
        observed.append(owner)
        ctx.node('C').write_input('source.txt', 'produced')

    @workflow.task('B')
    def peer(ctx):
        return None

    try:
        workflow.start('A')
        if selected:
            workflow.run_job('A', 1, ignore_readiness=True)
        else:
            workflow.run_component(('A', 'B'), ignore_readiness=True)
        owner, = observed
        shape_id = storage.db_connection().execute(
            'SELECT shape_id FROM graph_shapes WHERE shape_json=?', (workflow.topology.snapshot().shape_json,),
        ).fetchone()['shape_id']
        assert owner.get('shape_id') == shape_id
        assert owner.get('alignment_generation') == 0
        assert owner['component'] == ('A', 'B')
        assert owner['node_name'] == 'A'
        assert owner['job_id'] == 1
        assert storage.db_connection().execute('SELECT COUNT(*) FROM pending_component_executions').fetchone()[0] == 0
        assert storage.get_job_execution_owner(owner['execution_id']) == owner
        assert storage.read_job_current_owner('A', 1) == owner
        assert storage.get_execution_session(owner['session_id'])['status'] == 'terminal'
        assert (tmp_path / 'node/C/input/A/source.txt').read_text() == 'produced'
    finally:
        _close(storage)


@pytest.mark.parametrize('mode', ['selected-job', 'reserved-component', 'begun-component'])
def test_claim_captures_identity_that_survives_current_state_changes_and_trace_removal(tmp_path, mode):
    storage, topology = _storage(tmp_path, selected=mode == 'selected-job', pending=mode == 'begun-component')
    try:
        generation, execution_id = _claim(storage)[0]
        owner = storage.get_job_execution_owner(execution_id)
        assert owner.get('alignment_generation') == 7
        shape_id = owner.get('shape_id')
        assert isinstance(shape_id, int)
        assert storage.db_connection().execute(
            'SELECT shape_json FROM graph_shapes WHERE shape_id=?', (shape_id,),
        ).fetchone()['shape_json'] == topology.shape_json
        storage.finalize_job_execution('A', 1, generation, execution_id, 'done')
        storage.clear_job_events('A', [1])

        # Simulate a later accepted definition update, retaining the old shape.
        # Historical reads must not reconstruct the producer from current state.
        def advance(connection):
            connection.execute('DELETE FROM pending_component_executions')
            next_shape = connection.execute(
                'INSERT INTO graph_shapes(shape_json) VALUES(?)',
                (topology.shape_json.replace('"C"', '"D"'),),
            ).lastrowid
            connection.execute('UPDATE component_definitions SET shape_id=?', (next_shape,))
            connection.execute('UPDATE component_states SET alignment_generation=8')

        storage.submit_db_mutation(advance)
        assert storage.read_job_current_owner('A', 1) == owner
        assert storage.get_job_execution_owner(execution_id) == owner
    finally:
        _close(storage)
    reopened = FileStorage(tmp_path)
    try:
        assert reopened.read_job_current_owner('A', 1) == owner
        assert reopened.get_job_execution_owner(execution_id) == owner
    finally:
        _close(reopened)


@pytest.mark.parametrize('batch', [False, True])
def test_selected_job_claim_refuses_an_unselected_job_without_partial_claims(tmp_path, batch):
    storage, _ = _storage(tmp_path)
    try:
        if not batch:
            storage.submit_db_mutation(lambda connection: connection.execute('UPDATE session_jobs SET job_id=2'))
        before = _rows(storage)
        with pytest.raises(RuntimeError, match='selected job'):
            _claim(storage, batch=batch)
        assert _rows(storage) == before
    finally:
        _close(storage)


@pytest.mark.parametrize('damage', ['missing-state', 'invalid-generation', 'pending-generation', 'pending-shape'])
def test_claim_refuses_damaged_producing_identity_before_mutating_jobs(tmp_path, damage):
    storage, topology = _storage(tmp_path, selected=False, pending=damage.startswith('pending-'))
    try:
        def corrupt(connection):
            if damage == 'missing-state':
                connection.execute('DELETE FROM component_states WHERE component_key=?', ('["A", "B"]',))
            elif damage == 'invalid-generation':
                connection.execute('PRAGMA ignore_check_constraints=ON')
                try:
                    connection.execute('UPDATE component_states SET alignment_generation=-1')
                finally:
                    connection.execute('PRAGMA ignore_check_constraints=OFF')
            elif damage == 'pending-generation':
                connection.execute('UPDATE pending_component_executions SET alignment_generation=8')
            else:
                shape_id = connection.execute(
                    'INSERT INTO graph_shapes(shape_json) VALUES(?)',
                    (topology.shape_json.replace('"C"', '"D"'),),
                ).lastrowid
                connection.execute('UPDATE pending_component_executions SET shape_id=?', (shape_id,))

        storage.submit_db_mutation(corrupt)
        before = _rows(storage)
        with pytest.raises(RuntimeError):
            _claim(storage, batch=True)
        assert _rows(storage) == before
    finally:
        _close(storage)


@pytest.mark.parametrize('operation', ['explicit', 'auto', 'batch'])
@pytest.mark.parametrize('clear_trace', [False, True])
def test_selected_execution_keeps_recursive_creation_ancestry(tmp_path, operation, clear_trace):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'B'), ('B', 'A')])
    storage = workflow.storage
    children = {}

    @workflow.task('A')
    def root(ctx):
        if ctx.job_id != 1:
            return
        target = ctx.node('B')
        if operation == 'explicit':
            child = target.add(job_id=7)
        elif operation == 'auto':
            child = target.add()
        else:
            child, = target.add_many([{}])
        children['B'] = child.job_id
        if clear_trace:
            storage.clear_job_events('A', [1])
        workflow.run_job('B', child.job_id, ignore_readiness=True)

    @workflow.task('B')
    def child(ctx):
        grandchild = ctx.node('A').add()
        children['A'] = grandchild.job_id
        if clear_trace:
            storage.clear_job_events('B', [ctx.job_id])
        workflow.run_job('A', grandchild.job_id, ignore_readiness=True)

    try:
        workflow.start('A', job_id=1)
        workflow.run_job('A', 1, ignore_readiness=True)
        root_owner = storage.read_job_current_owner('A', 1)
        child_owner = storage.read_job_current_owner('B', children['B'])
        grandchild_owner = storage.read_job_current_owner('A', children['A'])
        assert root_owner['created_by_execution_id'] is None
        assert child_owner['created_by_execution_id'] == root_owner['execution_id']
        assert grandchild_owner['created_by_execution_id'] == child_owner['execution_id']
        assert child_owner['session_id'] == grandchild_owner['session_id'] == root_owner['session_id']
        for owner in (root_owner, child_owner, grandchild_owner):
            instance = storage.db_connection().execute(
                'SELECT created_by_execution_id FROM job_instances WHERE node_name=? AND job_id=?',
                (owner['node_name'], owner['job_id']),
            ).fetchone()
            assert instance['created_by_execution_id'] == owner['created_by_execution_id']
        selected, = storage.db_connection().execute(
            'SELECT node_name, job_id, job_instance_id FROM session_jobs WHERE session_id=?',
            (root_owner['session_id'],),
        ).fetchall()
        assert tuple(selected) == ('A', 1, root_owner['job_instance_id'])
        assert storage.get_execution_session(root_owner['session_id'])['selection_kind'] == 'jobs'

        storage.delete_job('B', children['B'])
        assert storage.get_job_execution_owner(child_owner['execution_id']) == child_owner
        assert storage.get_job_execution_owner(grandchild_owner['execution_id']) == grandchild_owner
        workflow.run_job('A', children['A'], ignore_readiness=True)
        later_owner = storage.read_job_current_owner('A', children['A'])
        assert later_owner['session_id'] != root_owner['session_id']
        assert later_owner['created_by_execution_id'] == child_owner['execution_id']
        assert storage.get_job_execution_owner(grandchild_owner['execution_id']) == grandchild_owner
    finally:
        _close(storage)


@pytest.mark.parametrize('damage', ['recreated-root', 'removed-roots'])
def test_selected_root_damage_cannot_admit_unrelated_current_instances(tmp_path, damage):
    storage, _ = _storage(tmp_path)
    try:
        if damage == 'recreated-root':
            storage.delete_job('A', 1)
            storage.create_job(Job(node_name='A', job_id=1, params={'replacement': True}))
        else:
            storage.submit_db_mutation(lambda connection: connection.execute('DELETE FROM session_jobs'))
        before = _rows(storage)
        with pytest.raises(RuntimeError, match='selected job'):
            _claim(storage)
        assert _rows(storage) == before
    finally:
        _close(storage)


@pytest.mark.parametrize('operation', ['explicit', 'auto', 'batch'])
def test_idempotent_reuse_does_not_attach_old_work_to_a_new_selected_invocation(tmp_path, operation):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'B'), ('B', 'A')])
    storage = workflow.storage
    children = []

    @workflow.task('A')
    def root(ctx):
        target = ctx.node('B')
        if operation == 'explicit':
            child = target.add(job_id=7, idempotency_key='shared')
        elif operation == 'auto':
            child = target.add(idempotency_key='shared')
        else:
            child, = target.add_many([{}], idempotency_keys=['shared'])
        if children:
            assert child.job_id == children[0]
            with pytest.raises(RuntimeError, match='selected job'):
                workflow.run_job('B', child.job_id, ignore_readiness=True)
        else:
            children.append(child.job_id)
            workflow.run_job('B', child.job_id, ignore_readiness=True)

    @workflow.task('B')
    def child(ctx):
        return None

    try:
        workflow.start('A', job_id=1)
        workflow.run_job('A', 1, ignore_readiness=True)
        owner = storage.read_job_current_owner('B', children[0])
        workflow.run_job('A', 1, ignore_readiness=True)
        assert storage.list_job_ids('B') == children
        assert storage.read_job_current_owner('B', children[0]) == owner
        assert storage.db_connection().execute(
            "SELECT created_by_execution_id FROM job_instances WHERE node_name='B' AND job_id=?", (children[0],),
        ).fetchone()['created_by_execution_id'] == owner['created_by_execution_id']
    finally:
        _close(storage)


@pytest.mark.parametrize('operation', ['observation', 'claim'])
def test_changed_instance_creator_cannot_rewrite_execution_history(tmp_path, operation):
    storage, _ = _storage(tmp_path, selected=False)
    try:
        claims = _claim(storage, batch=True)
        for job_id, (generation, execution_id) in enumerate(claims, 1):
            storage.finalize_job_execution('A', job_id, generation, execution_id, 'done')
        original = storage.get_job_execution_owner(claims[0][1])
        storage.submit_db_mutation(lambda connection: connection.execute(
            "UPDATE job_instances SET created_by_execution_id=? WHERE node_name='A' AND job_id=1",
            (claims[1][1],),
        ))
        before = _rows(storage)
        with pytest.raises(RuntimeError):
            if operation == 'observation':
                storage.read_job_current_owner('A', 1)
            else:
                _claim(storage)
        assert _rows(storage) == before
        assert storage.get_job_execution_owner(claims[0][1]) == original
    finally:
        _close(storage)


@pytest.mark.parametrize('damage', ['missing-roots', 'unexpected-roots'])
@pytest.mark.parametrize('operation', ['get', 'list', 'prepare', 'exit'])
def test_session_scope_damage_refuses_reads_and_full_preparation(tmp_path, damage, operation):
    storage, topology = _storage(tmp_path, selected=damage == 'missing-roots')
    try:
        if damage == 'missing-roots':
            storage.submit_db_mutation(lambda connection: connection.execute('DELETE FROM session_jobs'))
        else:
            storage.submit_db_mutation(lambda connection: connection.execute(
                'INSERT INTO session_jobs(session_id, position, node_name, job_id, job_instance_id) '
                "SELECT 'producing-session', 0, node_name, job_id, instance_id FROM job_instances "
                "WHERE node_name='A' AND job_id=1",
            ))
        before = _rows(storage)
        with pytest.raises(RuntimeError):
            if operation == 'get':
                storage.get_execution_session('producing-session')
            elif operation == 'list':
                storage.list_live_execution_sessions()
            elif operation == 'exit':
                storage.decide_execution_session_exit('producing-session', outcome='done', finished_at=now())
            else:
                storage.read_component_fresh_preparation(
                    'producing-session', ('A', 'B'), expected_shape=topology.shape_json,
                )
        assert _rows(storage) == before
    finally:
        _close(storage)


@pytest.mark.parametrize('damage', [
    'invalid-shape', 'different-component', 'string-component', 'unsorted-component',
    'negative-alignment', 'empty-creator', 'missing-creator', 'unsafe-shape-node', 'unselected-session',
])
@pytest.mark.parametrize('reader', ['history', 'current'])
def test_owner_readers_refuse_damaged_producing_identity(tmp_path, damage, reader):
    storage, topology = _storage(tmp_path, selected=False)
    try:
        _, execution_id = _claim(storage)[0]
        if damage == 'unselected-session':
            storage.create_execution_session(
                'other-session', session_kind='interrupt', command='run', start_component=('C',),
                selected_components=[('C',)], started_at=now(), hostname='claim-test',
                pid=os.getpid(), process_identity='claim-test',
            )
        connection = storage.db_connection()
        connection.execute('PRAGMA foreign_keys=OFF')
        connection.execute('PRAGMA ignore_check_constraints=ON')
        try:
            if damage in ('invalid-shape', 'unsafe-shape-node'):
                connection.execute('DROP TRIGGER prevent_graph_shape_update')
            if damage == 'invalid-shape':
                connection.execute("UPDATE graph_shapes SET shape_json='not JSON'")
            elif damage == 'unsafe-shape-node':
                graph = nx.DiGraph([('A', 'B'), ('B', 'A'), ('B', 'C')])
                graph.add_node('../unsafe')
                alternate = ComponentTopology(graph, []).snapshot()
                connection.execute('UPDATE graph_shapes SET shape_json=?', (alternate.shape_json,))
            elif damage == 'different-component':
                alternate = ComponentTopology(nx.DiGraph([('A', 'B'), ('B', 'C')]), []).snapshot()
                shape_id = connection.execute(
                    'INSERT INTO graph_shapes(shape_json) VALUES(?)', (alternate.shape_json,),
                ).lastrowid
                connection.execute('UPDATE job_execution_owners SET shape_id=?', (shape_id,))
            elif damage in ('string-component', 'unsorted-component'):
                key = '"AB"' if damage == 'string-component' else '["B", "A"]'
                connection.execute('UPDATE job_execution_owners SET component_key=?', (key,))
            elif damage == 'unselected-session':
                connection.execute("UPDATE job_execution_owners SET session_id='other-session'")
            elif damage == 'negative-alignment':
                connection.execute('UPDATE job_execution_owners SET alignment_generation=-1')
            else:
                creator = '' if damage == 'empty-creator' else 'missing-execution'
                connection.execute('UPDATE job_execution_owners SET created_by_execution_id=?', (creator,))
        finally:
            connection.execute('PRAGMA foreign_keys=ON')
            connection.execute('PRAGMA ignore_check_constraints=OFF')
        before = _rows(storage)
        with pytest.raises(RuntimeError):
            if reader == 'history':
                storage.get_job_execution_owner(execution_id)
            else:
                storage.read_job_current_owner('A', 1)
        assert _rows(storage) == before
    finally:
        _close(storage)
