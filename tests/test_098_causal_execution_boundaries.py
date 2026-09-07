from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from threading import Event, Thread

import pytest

from micro_workflow_manager import MicroWorkflow
from micro_workflow_manager.errors import JobFailedError
from tests.test_090_component_session_settlement import _close, _rows


@pytest.mark.parametrize('catch_child_failure', [False, True])
def test_selected_child_restart_survives_root_exit(tmp_path, catch_child_failure):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'B'), ('B', 'A')])
    storage = workflow.storage
    child_failed, release_root = Event(), Event()
    calls, result = [], {}

    @workflow.task('A')
    def root(ctx):
        calls.append(('A', ctx.job_id, ctx.execution_generation))
        child = ctx.node('B').add()
        try:
            workflow.run_job('B', child.job_id, ignore_readiness=True)
        except JobFailedError:
            child_failed.set()
            assert release_root.wait(30)
            if not catch_child_failure:
                raise
        return 'root result'

    @workflow.task('B')
    def child(ctx):
        calls.append(('B', ctx.job_id, ctx.execution_generation))
        if ctx.execution_generation == 0:
            raise ValueError('child needs a restart')
        return 'child result'

    def run():
        try:
            result['value'] = workflow.run_job('A', 1, ignore_readiness=True)
        except BaseException as error:
            result['error'] = error

    workflow.start('A', job_id=1)
    workflow.add_job(None, 'A', job_id=2)
    thread = Thread(target=run, daemon=True)
    thread.start()
    try:
        assert child_failed.wait(20), result
        root_owner = storage.read_job_current_owner('A', 1)
        child_owner = storage.read_job_current_owner('B', 1)
        assert child_owner['created_by_execution_id'] == root_owner['execution_id']
        command = subprocess.run(
            [sys.executable, '-m', 'micro_workflow_manager', 'restart', 'B', 'job', '1'],
            cwd=tmp_path, env=dict(os.environ, PYTHONPATH=str(Path(__file__).resolve().parents[1])),
            capture_output=True, text=True, timeout=20,
        )
        assert command.returncode == 0, command.stdout + command.stderr
        assert storage.get_execution_session(root_owner['session_id'])['status'] == 'running'
        assert storage.current_job_generation('B', 1) == 1
        release_root.set()
        thread.join(timeout=30)
        assert not thread.is_alive(), result
        assert calls == [('A', 1, 0), ('B', 1, 0), ('B', 1, 1)]
        if catch_child_failure:
            assert result == {'value': 'root result'}
            assert storage.get_job_status('A', 1) == 'done'
        else:
            assert isinstance(result.get('error'), JobFailedError), result
        successor = storage.read_job_current_owner('B', 1)
        assert successor['session_id'] == root_owner['session_id']
        assert successor['generation'] == 1
        assert successor['created_by_execution_id'] == root_owner['execution_id']
        assert storage.get_job_status('B', 1) == 'done'
        assert storage.get_node_status('B') == ('queued' if catch_child_failure else 'failed')
        assert storage.get_job_status('A', 2) == 'queued'
        assert storage.read_job_current_owner('A', 2) is None
        assert storage.read_job_current_owner('A', 1) == root_owner
        assert len([event for event in storage.read_job_events('A', 1) if event['event'] == 'started']) == 1
        assert storage.get_execution_session(root_owner['session_id'])['status'] == 'terminal'
        assert storage.get_component_reservation(('A', 'B')) is None
    finally:
        release_root.set()
        thread.join(timeout=30)
        assert not thread.is_alive(), result
        _close(storage)


def test_process_selected_root_records_child_creator_and_executes_same_component_child(tmp_path):
    source = tmp_path / 'src'
    behaviors = source / 'node_behavior'
    behaviors.mkdir(parents=True)
    graph = source / 'graph.py'
    graph.write_text("EDGES = [('A', 'B'), ('B', 'A')]\n", encoding='utf-8')
    (behaviors / 'A.py').write_text(
        'from micro_workflow_manager import NodeRouter\n'
        'router = NodeRouter("A", runner="process", max_threads=1)\n'
        '@router.task\n'
        'def work(ctx):\n'
        '    child = ctx.node("B").add()\n'
        '    ctx.system.run_job("B", child.job_id, ignore_readiness=True)\n'
        '    return child.job_id\n', encoding='utf-8',
    )
    (behaviors / 'B.py').write_text(
        'from micro_workflow_manager import NodeRouter\n'
        'router = NodeRouter("B", runner="direct")\n'
        '@router.task\n'
        'def work(ctx):\n'
        '    return ctx.system.storage.read_job_current_owner("B", ctx.job_id)\n', encoding='utf-8',
    )
    workflow = MicroWorkflow(tmp_path, runner='process', process_graph_path=graph, persist_graph=False)
    workflow.graph([('A', 'B'), ('B', 'A')])
    workflow.include_node_dir(behaviors)
    workflow.add_job(None, 'A', job_id=1)
    storage = workflow.storage
    try:
        workflow.run_jobs('A', [1], ignore_readiness=True)
        root = storage.read_job_current_owner('A', 1)
        child = storage.read_job_current_owner('B', 1)
        assert child['created_by_execution_id'] == root['execution_id']
        assert child['session_id'] == root['session_id']
        assert storage.get_job_status('B', 1) == 'done'
        assert storage.get_execution_session(root['session_id'])['selected_jobs'] == [('A', 1)]
    finally:
        _close(storage)


@pytest.mark.parametrize('operation', ['explicit', 'auto', 'batch'])
def test_finished_creator_cannot_publish_more_jobs(tmp_path, operation):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'B'), ('B', 'A')])
    targets = []

    @workflow.task('A')
    def root(ctx):
        targets.append(ctx.node('B'))

    workflow.start('A', job_id=1)
    storage = workflow.storage
    try:
        workflow.run_job('A', 1, ignore_readiness=True)
        before = _rows(storage)
        files = {p.relative_to(tmp_path): p.read_bytes() for p in (tmp_path / 'node').rglob('*') if p.is_file()}
        with pytest.raises(Exception):
            if operation == 'explicit':
                targets[0].add(job_id=8)
            elif operation == 'auto':
                targets[0].add()
            else:
                targets[0].add_many([{}, {}])
        assert _rows(storage) == before
        assert {p.relative_to(tmp_path): p.read_bytes() for p in (tmp_path / 'node').rglob('*') if p.is_file()} == files
    finally:
        _close(storage)


def test_grouped_claim_refuses_unrelated_job_and_commits_selected_root(tmp_path):
    from micro_workflow_manager.models import now
    from micro_workflow_manager.storage.execution_claims import ExecutionClaimBatch
    from tests.test_097_execution_producing_identity import _storage

    storage, _ = _storage(tmp_path)
    try:
        before = storage.read_job_owner_observation('A', 2), storage.read_job_events('A', 2)
        fields = dict(node_name='A', started_at=now(), pid=os.getpid(), thread_id=1,
                      event_time=now(), session_id='producing-session', component=('A', 'B'))
        batches = [
            ExecutionClaimBatch(job_ids=(2,), execution_ids=('invalid-claim',), **fields),
            ExecutionClaimBatch(job_ids=(1,), execution_ids=('valid-claim',), **fields),
        ]
        results = storage.submit_db_mutation(lambda connection: storage._apply_execution_claim_batches(connection, batches))
        assert results[0][0] is False and isinstance(results[0][1], RuntimeError)
        assert results[1][0] is True
        assert storage.read_job_current_owner('A', 1)['execution_id'] == 'valid-claim'
        assert (storage.read_job_owner_observation('A', 2), storage.read_job_events('A', 2)) == before
    finally:
        _close(storage)


@pytest.mark.parametrize('change_creator', [False, True])
def test_preparation_captures_creator_and_retained_reset_preserves_it(tmp_path, change_creator):
    from micro_workflow_manager.models import Job, now
    from micro_workflow_manager.storage.job_preparation import read_job_preparation
    from micro_workflow_manager.storage.preparation_files import stage_preparation_files
    from tests.test_097_execution_producing_identity import _storage, _claim

    storage, topology = _storage(tmp_path, selected=False)
    try:
        root_generation, root_execution = _claim(storage)[0]
        storage.create_job(Job(node_name='B', job_id=1, params={},
                               parent={'from_node': 'A', 'from_job_id': 1},
                               producer_component=('A', 'B'), job_kind='component'),
                           producer_execution_id=root_execution)
        child_generation, child_execution = storage.claim_job_execution(
            'B', 1, started_at=now(), session_id='producing-session', component=('A', 'B'),
        )
        storage.finalize_job_execution('B', 1, child_generation, child_execution, 'done')
        storage.finalize_job_execution('A', 1, root_generation, root_execution, 'done')
        storage.finish_execution_session('producing-session', outcome='done', finished_at=now())
        storage.release_execution_components('producing-session')
        output = storage.output_file('B', 1)
        storage.atomic_write_json(output, {'original': True})
        original_bytes = output.read_bytes()
        expected = storage.read_component_reset_preparation(('A', 'B'), expected_shape=topology.shape_json)
        plan = read_job_preparation(storage, ['A', 'B'], set(), reset_retained=True, preserve_external=False)
        if change_creator:
            with pytest.raises(RuntimeError, match='Jobs changed during full preparation'):
                with stage_preparation_files(tmp_path, plan):
                    assert not output.exists()
                    storage.submit_db_mutation(lambda connection: connection.execute(
                        "UPDATE job_instances SET created_by_execution_id=? WHERE node_name='B' AND job_id=1",
                        (child_execution,),
                    ))
                    before = _rows(storage)
                    storage.complete_component_reset_preparation(('A', 'B'), expected_state=expected, job_preparation=plan)
            assert output.read_bytes() == original_bytes
            assert _rows(storage) == before
        else:
            with stage_preparation_files(tmp_path, plan):
                storage.complete_component_reset_preparation(('A', 'B'), expected_state=expected, job_preparation=plan)
            assert not output.exists()
            instance = storage.db_connection().execute(
                "SELECT created_by_execution_id, last_execution_id FROM job_instances WHERE node_name='B' AND job_id=1",
            ).fetchone()
            assert tuple(instance) == (root_execution, None)
            assert storage.get_job_execution_owner(child_execution)['created_by_execution_id'] == root_execution
    finally:
        _close(storage)


def test_recorded_producing_shape_cannot_be_rewritten(tmp_path):
    import sqlite3
    import networkx as nx
    from micro_workflow_manager.topology import ComponentTopology
    from tests.test_097_execution_producing_identity import _storage, _claim

    storage, _ = _storage(tmp_path, selected=False)
    try:
        _, execution_id = _claim(storage)[0]
        original = storage.get_job_execution_owner(execution_id)
        altered = ComponentTopology(nx.DiGraph([('A', 'B'), ('B', 'A'), ('B', 'D')]), []).snapshot()
        before = _rows(storage)
        with pytest.raises(sqlite3.IntegrityError, match='immutable'):
            storage.submit_db_mutation(lambda connection: connection.execute(
                'UPDATE graph_shapes SET shape_json=? WHERE shape_id=?', (altered.shape_json, original['shape_id']),
            ))
        assert _rows(storage) == before
        assert storage.get_job_execution_owner(execution_id) == original
    finally:
        _close(storage)


@pytest.mark.parametrize('damage', ['creator-cycle', 'missing-ancestor', 'recreated-child'])
def test_damaged_selected_ancestry_cannot_admit_descendants(tmp_path, damage):
    from micro_workflow_manager.models import Job, now
    from tests.test_097_execution_producing_identity import _storage, _claim

    storage, _ = _storage(tmp_path)
    try:
        root_generation, root_execution = _claim(storage)[0]
        storage.create_job(Job(node_name='B', job_id=1, params={},
                               parent={'from_node': 'A', 'from_job_id': 1},
                               producer_component=('A', 'B'), job_kind='component'),
                           producer_execution_id=root_execution)
        generation, execution_id = storage.claim_job_execution(
            'B', 1, started_at=now(), session_id='producing-session', component=('A', 'B'),
        )
        storage.create_job(Job(node_name='A', job_id=3, params={},
                               parent={'from_node': 'B', 'from_job_id': 1},
                               producer_component=('A', 'B'), job_kind='component'),
                           producer_execution_id=execution_id)
        storage.finalize_job_execution('B', 1, generation, execution_id, 'done')
        storage.finalize_job_execution('A', 1, root_generation, root_execution, 'done')
        if damage == 'recreated-child':
            storage.delete_job('A', 3)
            storage.create_job(Job(node_name='A', job_id=3, params={'replacement': True}))
        else:
            connection = storage.db_connection()
            connection.execute('PRAGMA foreign_keys=OFF')
            try:
                creator = execution_id if damage == 'creator-cycle' else 'absent-ancestor'
                connection.execute('UPDATE job_execution_owners SET created_by_execution_id=? WHERE execution_id=?',
                                   (creator, execution_id))
            finally:
                connection.execute('PRAGMA foreign_keys=ON')
        before = _rows(storage)
        with pytest.raises(RuntimeError):
            storage.claim_job_execution('A', 3, started_at=now(),
                                        session_id='producing-session', component=('A', 'B'))
        assert _rows(storage) == before
    finally:
        _close(storage)


@pytest.mark.parametrize('operation', ['read', 'exit'])
def test_selected_root_outside_component_scope_refuses_before_changes(tmp_path, operation):
    from micro_workflow_manager.models import now
    from tests.test_097_execution_producing_identity import _storage

    storage, _ = _storage(tmp_path)
    try:
        storage.submit_db_mutation(lambda connection: connection.execute("UPDATE session_jobs SET node_name='C'"))
        before = _rows(storage)
        with pytest.raises(RuntimeError, match='selected job'):
            if operation == 'read':
                storage.get_execution_session('producing-session')
            else:
                storage.decide_execution_session_exit('producing-session', outcome='done', finished_at=now())
        assert _rows(storage) == before
    finally:
        _close(storage)


@pytest.mark.parametrize('operation', ['explicit', 'auto', 'batch'])
@pytest.mark.parametrize('logical_parent', [None, 'C'])
def test_public_job_creation_inherits_actual_task_creator_independently_of_trace(tmp_path, operation, logical_parent):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'B'), ('B', 'A'), ('C', 'B')])
    storage = workflow.storage

    @workflow.task('A')
    def root(ctx):
        if operation == 'explicit':
            child = workflow.add_job(logical_parent, 'B', job_id=7)
        elif operation == 'auto':
            child = workflow.add_job(logical_parent, 'B')
        else:
            child, = workflow.add_jobs(logical_parent, 'B', [{}])
        assert child.parent == (None if logical_parent is None else {'from_node': 'C', 'from_job_id': None})
        instance = storage.db_connection().execute(
            "SELECT created_by_execution_id FROM job_instances WHERE node_name='B' AND job_id=?", (child.job_id,),
        ).fetchone()
        assert instance['created_by_execution_id'] == ctx.execution_id
        reloaded = storage.load_job('B', child.job_id)
        assert reloaded.producer_component == ('A', 'B') and reloaded.job_kind == 'component'
        workflow.run_job('B', child.job_id, ignore_readiness=True)
        return child.job_id

    @workflow.task('B')
    def child(ctx):
        return 'child'

    @workflow.task('C')
    def prerequisite(ctx):
        return 'ready'

    workflow.start('A', job_id=1)
    workflow.start('C', job_id=1)
    try:
        workflow.run_node('C')
        parent_state = storage.get_component_state(('C',))
        parent_owner = storage.read_job_current_owner('C', 1)
        assert parent_state['lifecycle'] == 'done' and parent_state['stability'] == 'stable'
        child_id = workflow.run_job('A', 1, ignore_readiness=True)
        root_owner = storage.read_job_current_owner('A', 1)
        child_owner = storage.read_job_current_owner('B', child_id)
        assert child_owner['created_by_execution_id'] == root_owner['execution_id']
        assert child_owner['session_id'] == root_owner['session_id']
        assert storage.get_component_state(('C',)) == parent_state
        assert storage.read_job_current_owner('C', 1) == parent_owner
        assert parent_owner['session_id'] != root_owner['session_id']
    finally:
        _close(storage)


def test_task_cannot_publish_with_another_active_tasks_retained_handle(tmp_path):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'B'), ('B', 'A')])
    storage = workflow.storage
    handles = []

    @workflow.task('A')
    def root(ctx):
        handles.append(ctx.node('B'))
        child = handles[0].add()
        workflow.run_job('B', child.job_id, ignore_readiness=True)

    @workflow.task('B')
    def child(ctx):
        before = _rows(storage)
        with pytest.raises(RuntimeError, match='producer|execution'):
            handles[0].add()
        assert _rows(storage) == before

    workflow.start('A', job_id=1)
    try:
        workflow.run_job('A', 1, ignore_readiness=True)
        assert storage.list_job_ids('B') == [1]
    finally:
        _close(storage)


@pytest.mark.parametrize('component', ['"AB"', '["../unsafe", "A"]', '[]', 'broken-json'])
@pytest.mark.parametrize('operation', ['read', 'list', 'exit'])
def test_session_readers_refuse_damaged_component_declarations(tmp_path, component, operation):
    from tests.test_097_execution_producing_identity import _storage

    storage, _ = _storage(tmp_path)
    try:
        def damage(connection):
            connection.execute('UPDATE session_components SET component_key=?', (component,))
            if component == '[]':
                connection.execute("UPDATE execution_sessions SET start_component='[]', selection_kind='components'")
                connection.execute('DELETE FROM session_jobs')
        storage.submit_db_mutation(damage)
        before = _rows(storage)
        with pytest.raises(RuntimeError):
            if operation == 'read':
                storage.get_execution_session('producing-session')
            elif operation == 'list':
                storage.list_execution_sessions()
            else:
                from micro_workflow_manager.models import now
                storage.decide_execution_session_exit('producing-session', outcome='done', finished_at=now())
        assert _rows(storage) == before
    finally:
        _close(storage)


@pytest.mark.parametrize('damaged', [False, True])
def test_selected_restart_keeps_valid_pending_completion_and_repairs_before_invalid_completion(tmp_path, damaged):
    from micro_workflow_manager.models import now
    from tests.test_097_execution_producing_identity import _storage, _claim

    storage, topology = _storage(tmp_path, pending=True)
    try:
        generation, execution_id = _claim(storage)[0]
        storage.finalize_job_execution('A', 1, generation, execution_id, 'failed')
        storage.request_owned_job_restarts(storage.plan_owned_job_restarts([('A', 1)]))
        if damaged:
            storage.submit_db_mutation(lambda connection: connection.execute(
                "UPDATE pending_component_executions SET stability='unstable', instability_origin='producing-session'",
            ))
        before = _rows(storage)
        decision = storage.decide_execution_session_exit('producing-session', outcome='done', finished_at=now())
        assert _rows(storage) == before
        expected, = decision['restarts'].values()
        assert decision['released'] == 0
        if damaged:
            assert not decision.get('component_restarts')
        else:
            proposal = decision['component_restarts'][('A', 1)]
            assert proposal.component == ('A', 'B')
            assert proposal.expected_shape == topology.shape_json
            assert proposal.expected_alignment_generation == 7
        next_generation, successor = storage.claim_job_execution(
            'A', 1, started_at=now(), session_id='producing-session', component=('A', 'B'),
            expected_restart=expected,
        )
        assert next_generation == generation + 1
        storage.finalize_job_execution('A', 1, next_generation, successor, 'done')
        if damaged:
            after_repair = _rows(storage)
            with pytest.raises(RuntimeError, match='pending'):
                storage.decide_execution_session_exit('producing-session', outcome='done', finished_at=now())
            assert _rows(storage) == after_repair
    finally:
        _close(storage)


def test_claimed_restart_classification_refuses_noncanonical_historical_predecessor(tmp_path):
    from micro_workflow_manager.models import now
    from tests.test_097_execution_producing_identity import _storage, _claim

    storage, _ = _storage(tmp_path, selected=False, pending=True)
    try:
        generation, execution_id = _claim(storage)[0]
        storage.finalize_job_execution('A', 1, generation, execution_id, 'failed')
        storage.request_owned_job_restarts(storage.plan_owned_job_restarts([('A', 1)]))
        storage.claim_job_execution('A', 1, started_at=now(), session_id='producing-session', component=('A', 'B'))
        connection = storage.db_connection()
        connection.execute('PRAGMA foreign_keys=OFF')
        try:
            connection.execute('UPDATE job_execution_owners SET component_key=? WHERE execution_id=?',
                               ('"AB"', execution_id))
        finally:
            connection.execute('PRAGMA foreign_keys=ON')
        before = _rows(storage)
        with pytest.raises(RuntimeError):
            storage.submit_db_mutation(lambda connection: storage._read_pending_component_work(
                connection, 'producing-session', ('A', 'B'),
            ))
        assert _rows(storage) == before
    finally:
        _close(storage)
