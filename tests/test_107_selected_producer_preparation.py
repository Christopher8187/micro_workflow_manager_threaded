from __future__ import annotations

from types import SimpleNamespace

import pytest

from micro_workflow_manager import MicroWorkflow
from micro_workflow_manager.cli.destructive import execute_destructive_command
from micro_workflow_manager.cli.run_selected import run_selected_jobs
from micro_workflow_manager.models import FAILED, Job, now
from micro_workflow_manager.storage import FileStorage
from tests.test_046_resume_restart_wait import _start_native_session
from tests.test_090_component_session_settlement import _close
from tests.test_104_producer_footprint_preparation import _files
from tests.test_105_preparation_receiver_guards import (
    _native_receiver_owner,
    _relevant_state,
)


def _selected_history(tmp_path):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'X'), ('X', 'A'), ('A', 'B'), ('X', 'B'), ('B', 'C')])
    publish, calls = [True], []

    @workflow.task('A')
    def producer(ctx, lineage, depth):
        calls.append(('A', ctx.job_id, lineage, depth))
        if not publish[0]:
            return lineage, depth
        ctx.write_output(f'{lineage}-{depth}.txt', f'A:{lineage}:{depth}')
        if depth == 0:
            ctx.node('X').add(lineage=lineage, depth=1)
        ctx.node('B').write_input(f'{lineage}-{depth}.txt', f'A:{lineage}:{depth}')
        ctx.node('B').add(lineage=lineage, depth=depth)
        return lineage, depth

    @workflow.task('X')
    def child(ctx, lineage, depth):
        calls.append(('X', ctx.job_id, lineage, depth))
        ctx.write_output(f'{lineage}.txt', lineage)
        ctx.node('A').add(lineage=lineage, depth=2)
        ctx.node('B').write_input(f'{lineage}-child.txt', f'X:{lineage}')
        return lineage

    @workflow.task('B')
    def excluded_receiver(ctx, lineage, depth):
        calls.append(('B', ctx.job_id, lineage, depth))
        ctx.write_output(f'{lineage}-{depth}.txt', f'B:{lineage}:{depth}')
        ctx.node('C').write_input(f'{lineage}-{depth}.txt', f'B:{lineage}:{depth}')
        ctx.node('C').add(lineage=lineage, depth=depth)
        return lineage, depth

    @workflow.task('C')
    def later_receiver(ctx, lineage, depth):
        calls.append(('C', ctx.job_id, lineage, depth))
        ctx.write_output(f'{lineage}-{depth}.txt', f'C:{lineage}:{depth}')
        return lineage, depth

    workflow.start('A', job_id=1, lineage='selected', depth=0)
    workflow.start('A', job_id=2, lineage='other', depth=0)
    workflow.run()
    publish[0] = False
    return workflow, calls


def _job_state(storage, node, job_id):
    return (
        storage.load_job(node, job_id), storage.read_job_status_data(node, job_id),
        storage.read_job_control(node, job_id), storage.read_job_current_owner(node, job_id),
        storage.read_job_events(node, job_id), _files(storage.job_base_dir(node, job_id)),
    )


def _run_selected(root, workflow, entry):
    if entry == 'reset':
        return execute_destructive_command(
            root, workflow, SimpleNamespace(command='reset', node='A', yes=True,
                                            job_mode='job', job_specs=['1']),
        )
    if entry == 'cli-run':
        return run_selected_jobs(root, workflow, 'A', [1])
    if entry == 'run_job':
        return workflow.run_job('A', 1)
    if entry == 'run_jobs':
        return workflow.run_jobs('A', [1])
    return workflow.run_node_jobs('A', [workflow.storage.load_job('A', 1)])


def _run_selected_ids(root, workflow, entry, job_ids):
    if entry == 'reset':
        return execute_destructive_command(
            root, workflow, SimpleNamespace(
                command='reset', node='A', yes=True,
                job_mode='jobs', job_specs=list(map(str, job_ids)),
            ),
        )
    return workflow.run_jobs('A', list(job_ids))


@pytest.mark.parametrize('entry', ['reset', 'cli-run', 'run_job', 'run_jobs', 'run_node_jobs'])
@pytest.mark.parametrize('clear_optional_provenance', [False, True])
def test_selected_preparation_removes_prior_same_component_work_and_preserves_quotient_history(
    tmp_path, entry, clear_optional_provenance,
):
    workflow, calls = _selected_history(tmp_path)
    storage = workflow.storage
    try:
        root_owner = storage.read_job_current_owner('A', 1)
        before_component = storage.get_component_state(('A', 'X'))
        before_b = storage.get_component_state(('B',))
        before_c = storage.get_component_state(('C',))
        assert before_component['lifecycle'] == before_b['lifecycle'] == before_c['lifecycle'] == 'done'
        assert not before_component['misaligned'] and not before_b['misaligned'] and not before_c['misaligned']
        selected, preserved = {}, {}
        for node in ('A', 'X', 'B', 'C'):
            for job_id in storage.list_job_ids(node):
                job = storage.load_job(node, job_id)
                if job.params['lineage'] == 'selected' and node != 'C' and (node, job_id) != ('A', 1):
                    selected[node, job_id] = storage.read_job_current_owner(node, job_id)
                elif (node, job_id) != ('A', 1):
                    preserved[node, job_id] = None
        assert {node for node, _ in selected} == {'A', 'X', 'B'}
        selected_executions = {root_owner['execution_id']} | {
            owner['execution_id'] for (node, _), owner in selected.items() if node in ('A', 'X')
        }
        for owner in selected.values():
            assert owner['created_by_execution_id'] in selected_executions
        if clear_optional_provenance:
            storage.submit_db_mutation(lambda connection: connection.execute('UPDATE jobs SET parent_json=NULL'))
            for node in ('A', 'X', 'B', 'C'):
                storage.clear_job_events(node, storage.list_job_ids(node))
        preserved = {key: _job_state(storage, *key) for key in preserved}
        output = {node: _files(storage.node_output_dir(node)) for node in ('A', 'X', 'B', 'C')}
        c_files = _files(tmp_path / 'node' / 'C')
        incoming = storage.node_input_dir('A') / 'project.txt'
        incoming.write_bytes(b'project input')
        root_instance = storage.read_job_instance_id('A', 1)
        before_calls = list(calls)
        before_raw = {name: storage.get_node_status(name) for name in ('A', 'X', 'B', 'C')}
        removed_inputs = {
            relative: storage.read_node_input_owner('B', relative)
            for relative in ('A/selected-0.txt', 'A/selected-2.txt', 'X/selected-child.txt')
        }
        assert all(owner['execution_id'] in selected_executions for owner in removed_inputs.values())
        b_other_inputs = {
            relative: (path.read_bytes(), storage.read_node_input_owner('B', relative))
            for relative in ('A/other-0.txt', 'A/other-2.txt', 'X/other-child.txt')
            for path in [storage.node_input_dir('B') / relative]
        }

        _run_selected(tmp_path, workflow, entry)

        assert calls == before_calls + ([] if entry == 'reset' else [('A', 1, 'selected', 0)])
        assert storage.read_job_instance_id('A', 1) == root_instance
        assert storage.load_job('A', 1).params == {'lineage': 'selected', 'depth': 0}
        assert storage.get_job_status('A', 1) == ('queued' if entry == 'reset' else 'done')
        for key in selected:
            assert not storage.job_exists(*key)
            assert not storage.job_base_dir(*key).exists()
        assert {key: _job_state(storage, *key) for key in preserved} == preserved
        for relative in ('A/selected-0.txt', 'A/selected-2.txt', 'X/selected-child.txt'):
            assert not (storage.node_input_dir('B') / relative).exists()
            assert storage.read_node_input_owner('B', relative) is None
        assert {
            relative: ((storage.node_input_dir('B') / relative).read_bytes(),
                       storage.read_node_input_owner('B', relative))
            for relative in b_other_inputs
        } == b_other_inputs
        assert {node: _files(storage.node_output_dir(node)) for node in output} == output
        assert _files(tmp_path / 'node' / 'C') == c_files
        assert incoming.read_bytes() == b'project input'
        assert storage.get_component_state(('A', 'X')) == before_component
        if entry == 'reset':
            assert {name: storage.get_node_status(name) for name in before_raw} == dict(before_raw, A='queued')
        assert storage.get_component_state(('B',)) == dict(before_b, misaligned=True)
        assert storage.get_component_state(('C',)) == before_c
        assert storage.read_component_misalignment_causes(('A', 'X')) == []
        assert storage.read_component_misalignment_causes(('C',)) == []
        cause, = storage.read_component_misalignment_causes(('B',))
        if cause['affected_kind'] == 'managed-input':
            assert cause['path'] in removed_inputs
            producer = removed_inputs[cause['path']]
            affected = {'affected_kind': 'managed-input', 'path': cause['path']}
        else:
            assert cause['affected_kind'] == 'managed-job'
            removed_owner = selected['B', cause['job_id']]
            producer = storage.get_job_execution_owner(removed_owner['created_by_execution_id'])
            affected = {'affected_kind': 'managed-job', 'job_id': cause['job_id'],
                        'job_instance_id': removed_owner['job_instance_id']}
        assert cause == dict(
            affected, component=('B',), receiver_node='B',
            alignment_generation=before_b['alignment_generation'],
            producer_node=producer['node_name'], producer_job_id=producer['job_id'],
            preparation_kind='preparation-removal', action='delete',
            operation='reset' if entry == 'reset' else 'run',
        )
        saved_cause = storage.db_connection().execute(
            'SELECT producer_execution_id FROM component_misalignment_causes '
            'WHERE receiver_node=? AND alignment_generation=?', ('B', before_b['alignment_generation']),
        ).fetchone()
        assert saved_cause['producer_execution_id'] == producer['execution_id']
        assert saved_cause['producer_execution_id'] in selected_executions
    finally:
        _close(storage)


@pytest.mark.parametrize('entry', ['reset', 'run_jobs'])
@pytest.mark.parametrize('depth_order', [(0, 2), (2, 0)])
def test_overlapping_selected_roots_are_retained_independently_of_requested_order(
    tmp_path, entry, depth_order,
):
    workflow, calls = _selected_history(tmp_path)
    storage = workflow.storage
    try:
        roots = {
            storage.load_job('A', job_id).params['depth']: job_id
            for job_id in storage.list_job_ids('A')
            if storage.load_job('A', job_id).params['lineage'] == 'selected'
        }
        assert set(roots) == {0, 2}
        job_ids = tuple(roots[depth] for depth in depth_order)
        root_instances = {
            job_id: storage.read_job_instance_id('A', job_id) for job_id in job_ids
        }
        selected_nonroots = {
            (node, job_id)
            for node in ('A', 'X', 'B')
            for job_id in storage.list_job_ids(node)
            if storage.load_job(node, job_id).params['lineage'] == 'selected'
            and not (node == 'A' and job_id in job_ids)
        }
        preserved = {
            (node, job_id): _job_state(storage, node, job_id)
            for node in ('A', 'X', 'B', 'C')
            for job_id in storage.list_job_ids(node)
            if storage.load_job(node, job_id).params['lineage'] == 'other'
        }
        component_before = storage.get_component_state(('A', 'X'))
        output_before = _files(storage.node_output_dir('A'))
        calls_before = list(calls)

        result = _run_selected_ids(tmp_path, workflow, entry, job_ids)

        if entry == 'reset':
            assert result == 0
            assert calls == calls_before
        else:
            assert result == [('selected', depth) for depth in depth_order]
            assert calls == calls_before + [
                ('A', roots[depth], 'selected', depth) for depth in depth_order
            ]
        for job_id in job_ids:
            assert storage.read_job_instance_id('A', job_id) == root_instances[job_id]
            assert storage.get_job_status('A', job_id) == (
                'queued' if entry == 'reset' else 'done'
            )
        assert all(not storage.job_exists(*key) for key in selected_nonroots)
        assert {
            key: _job_state(storage, *key) for key in preserved
        } == preserved
        assert storage.get_component_state(('A', 'X')) == component_before
        assert _files(storage.node_output_dir('A')) == output_before
    finally:
        _close(storage)


def _multiple_generation_root_history(tmp_path):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'X'), ('X', 'A'), ('A', 'B')])
    storage = workflow.storage

    @workflow.task('A')
    def root(ctx, value):
        return value

    @workflow.task('X')
    def same_component_child(ctx, generation):
        return generation

    @workflow.task('B')
    def excluded_child(ctx, generation):
        return generation

    session_id = 'selected-history-owner'
    component = ('A', 'X')
    _start_native_session(workflow, session_id, component)
    storage.create_job(Job(node_name='A', job_id=1, params={'value': 'root'}))
    root_instance = storage.read_job_instance_id('A', 1)
    executions = []
    for expected_generation in range(3):
        generation, execution_id = storage.claim_job_execution(
            'A', 1, started_at=now(), session_id=session_id, component=component,
        )
        assert generation == expected_generation
        executions.append(execution_id)
        storage.create_job(
            Job(
                node_name='X', job_id=10 + generation,
                params={'generation': generation},
                parent={'from_node': 'A', 'from_job_id': 1},
                producer_component=component, job_kind='component',
            ),
            producer_execution_id=execution_id,
        )
        storage.create_job(
            Job(
                node_name='B', job_id=20 + generation,
                params={'generation': generation},
                parent={'from_node': 'A', 'from_job_id': 1},
                producer_component=component, job_kind='dag',
            ),
            producer_execution_id=execution_id,
        )
        storage.finalize_job_execution(
            'A', 1, generation, execution_id,
            'done' if generation == 2 else FAILED,
        )
        if generation < 2:
            restart = storage.request_job_restart('A', 1)
            assert restart['generation'] == generation + 1
    storage.release_execution_components(session_id)
    storage.finish_execution_session(session_id, outcome='done', finished_at=now())
    return workflow, root_instance, tuple(executions)


@pytest.mark.parametrize('entry', ['reset', 'run_jobs'])
def test_selected_preparation_uses_every_historical_generation_of_the_exact_root(
    tmp_path, entry,
):
    workflow, root_instance, executions = _multiple_generation_root_history(tmp_path)
    storage = workflow.storage
    try:
        rows = storage.db_connection().execute(
            'SELECT generation, execution_id FROM job_execution_owners '
            "WHERE node_name='A' AND job_id=1 ORDER BY generation, execution_id"
        ).fetchall()
        assert {row['generation'] for row in rows} == {0, 1, 2}
        assert {row['execution_id'] for row in rows} == set(executions)
        children = {
            (node, job_id)
            for node in ('X', 'B')
            for job_id in storage.list_job_ids(node)
        }
        assert children == {
            ('X', 10), ('X', 11), ('X', 12),
            ('B', 20), ('B', 21), ('B', 22),
        }
        component_before = storage.get_component_state(('A', 'X'))
        output_path = storage.node_output_dir('A') / 'aggregate.txt'
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(b'preserved aggregate output')

        result = _run_selected_ids(tmp_path, workflow, entry, (1,))

        assert result == (0 if entry == 'reset' else ['root'])
        assert storage.read_job_instance_id('A', 1) == root_instance
        assert storage.get_job_status('A', 1) == (
            'queued' if entry == 'reset' else 'done'
        )
        assert all(not storage.job_exists(*key) for key in children)
        expected_component = component_before if entry == 'reset' else {
            **component_before, 'lifecycle': 'done', 'stability': 'stable',
        }
        assert storage.get_component_state(('A', 'X')) == expected_component
        assert output_path.read_bytes() == b'preserved aggregate output'
    finally:
        _close(storage)


@pytest.mark.parametrize('entry', ['reset', 'run_jobs'])
def test_second_selected_preparation_preserves_the_excluded_receivers_first_cause(
    tmp_path, entry,
):
    workflow, calls = _selected_history(tmp_path)
    storage = workflow.storage
    try:
        _run_selected_ids(tmp_path, workflow, 'reset', (1,))
        first_cause = storage.read_component_misalignment_causes(('B',))
        assert len(first_cause) == 1
        assert first_cause[0]['preparation_kind'] == 'preparation-removal'
        other_root = next(
            job_id for job_id in storage.list_job_ids('A')
            if storage.load_job('A', job_id).params == {
                'lineage': 'other', 'depth': 0,
            }
        )
        component_before = storage.get_component_state(('A', 'X'))
        receiver_before = storage.get_component_state(('B',))
        output_before = _files(storage.node_output_dir('A'))
        calls_before = list(calls)

        result = _run_selected_ids(tmp_path, workflow, entry, (other_root,))

        if entry == 'reset':
            assert result == 0
            assert calls == calls_before
        else:
            assert result == [('other', 0)]
            assert calls == calls_before + [('A', other_root, 'other', 0)]
        assert storage.read_component_misalignment_causes(('B',)) == first_cause
        assert storage.get_component_state(('B',)) == receiver_before
        assert storage.get_component_state(('A', 'X')) == component_before
        assert _files(storage.node_output_dir('A')) == output_before
        assert not any(
            storage.load_job(node, job_id).params.get('lineage') == 'other'
            for node in ('X', 'B')
            for job_id in storage.list_job_ids(node)
        )
    finally:
        _close(storage)


@pytest.mark.parametrize('entry', ['reset', 'run_jobs'])
def test_selected_preparation_refuses_an_owned_excluded_receiver_before_mutation(
    tmp_path, entry,
):
    workflow, calls = _selected_history(tmp_path)
    storage = workflow.storage
    try:
        receiver_session = _native_receiver_owner(workflow, running=False)
        state_before = _relevant_state(storage, tmp_path)
        selected_component_before = storage.get_component_state(('A', 'X'))
        output_before = _files(storage.node_output_dir('A'))
        calls_before = list(calls)

        with pytest.raises(RuntimeError) as caught:
            _run_selected_ids(tmp_path, workflow, entry, (1,))

        assert receiver_session in str(caught.value)
        assert _relevant_state(storage, tmp_path) == state_before
        assert storage.get_component_state(('A', 'X')) == selected_component_before
        assert _files(storage.node_output_dir('A')) == output_before
        assert calls == calls_before
        assert storage.get_component_reservation(('B',))['session_id'] == receiver_session
        assert storage.get_component_reservation(('A', 'X')) is None
    finally:
        _close(storage)


@pytest.mark.parametrize('entry', ['reset', 'run_jobs'])
def test_selecting_a_generated_root_preserves_its_creator_and_ancestors(tmp_path, entry):
    workflow, calls = _selected_history(tmp_path)
    storage = workflow.storage
    try:
        root = next(job_id for job_id in storage.list_job_ids('A')
                    if storage.load_job('A', job_id).params == {'lineage': 'selected', 'depth': 2})
        removed = next(job_id for job_id in storage.list_job_ids('B')
                       if storage.load_job('B', job_id).params == {'lineage': 'selected', 'depth': 2})
        owner = storage.read_job_current_owner('A', root)
        creator = storage.get_job_execution_owner(owner['created_by_execution_id'])
        assert creator['node_name'] == 'X'
        ancestor = storage.get_job_execution_owner(creator['created_by_execution_id'])
        assert ancestor['node_name'] == 'A' and ancestor['job_id'] == 1
        removed_owner = storage.read_job_current_owner('B', removed)
        assert removed_owner['created_by_execution_id'] == owner['execution_id']
        preserved = {
            (node, job_id): _job_state(storage, node, job_id)
            for node in ('A', 'X', 'B', 'C') for job_id in storage.list_job_ids(node)
            if (node, job_id) not in {('A', root), ('B', removed)}
        }
        assert ('X', creator['job_id']) in preserved and ('A', ancestor['job_id']) in preserved
        aggregate = {node: _files(storage.node_output_dir(node)) for node in ('A', 'X', 'B', 'C')}
        a_state = storage.get_component_state(('A', 'X'))
        b_state = storage.get_component_state(('B',))
        c_state = storage.get_component_state(('C',))
        c_files = _files(tmp_path / 'node' / 'C')
        incoming = 'A/selected-2.txt'
        incoming_owner = storage.read_node_input_owner('B', incoming)
        assert incoming_owner['execution_id'] == owner['execution_id']
        other_inputs = {
            path.relative_to(storage.node_input_dir('B')).as_posix():
                (path.read_bytes(), storage.read_node_input_owner(
                    'B', path.relative_to(storage.node_input_dir('B')).as_posix()))
            for path in storage.node_input_dir('B').rglob('*') if path.is_file()
            and path != storage.node_input_dir('B') / incoming
        }
        before_calls = list(calls)

        result = _run_selected_ids(tmp_path, workflow, entry, (root,))

        assert result == (0 if entry == 'reset' else [('selected', 2)])
        assert calls == before_calls + ([] if entry == 'reset' else [('A', root, 'selected', 2)])
        assert storage.read_job_instance_id('A', root) == owner['job_instance_id']
        assert storage.get_job_status('A', root) == ('queued' if entry == 'reset' else 'done')
        assert not storage.job_exists('B', removed)
        assert not storage.job_base_dir('B', removed).exists()
        assert {key: _job_state(storage, *key) for key in preserved} == preserved
        assert not (storage.node_input_dir('B') / incoming).exists()
        assert storage.read_node_input_owner('B', incoming) is None
        assert {relative: ((storage.node_input_dir('B') / relative).read_bytes(),
                           storage.read_node_input_owner('B', relative)) for relative in other_inputs} == other_inputs
        assert {node: _files(storage.node_output_dir(node)) for node in aggregate} == aggregate
        assert storage.get_component_state(('A', 'X')) == a_state
        assert storage.get_component_state(('B',)) == dict(b_state, misaligned=True)
        assert storage.get_component_state(('C',)) == c_state
        assert _files(tmp_path / 'node' / 'C') == c_files
        cause, = storage.read_component_misalignment_causes(('B',))
        if cause['affected_kind'] == 'managed-input':
            assert cause['path'] == incoming
            affected = {'affected_kind': 'managed-input', 'path': incoming}
        else:
            assert cause['affected_kind'] == 'managed-job'
            assert (cause['job_id'], cause['job_instance_id']) == (
                removed, removed_owner['job_instance_id'],
            )
            affected = {
                'affected_kind': 'managed-job', 'job_id': removed,
                'job_instance_id': removed_owner['job_instance_id'],
            }
        assert cause == dict(
            affected, component=('B',), receiver_node='B',
            alignment_generation=b_state['alignment_generation'],
            producer_node='A', producer_job_id=root,
            preparation_kind='preparation-removal', action='delete',
            operation='reset' if entry == 'reset' else 'run',
        )
        saved_cause = storage.db_connection().execute(
            'SELECT producer_execution_id FROM component_misalignment_causes '
            'WHERE receiver_node=? AND alignment_generation=?',
            ('B', b_state['alignment_generation']),
        ).fetchone()
        assert saved_cause['producer_execution_id'] == owner['execution_id']
    finally:
        _close(storage)


def _run_selected_with_trace(root, workflow, entry):
    if entry == 'reset':
        return execute_destructive_command(
            root, workflow, SimpleNamespace(
                command='reset', node='A', yes=True,
                job_mode='job', job_specs=['1'], keeptrace=True,
            ),
        )
    return run_selected_jobs(root, workflow, 'A', [1], keep_trace=True)


@pytest.mark.parametrize('entry', ['reset', 'cli-run'])
def test_selected_preparation_keep_trace_retains_root_and_removed_descendant_history(
    tmp_path, entry,
):
    workflow, calls = _selected_history(tmp_path)
    storage = workflow.storage
    try:
        root = ('A', 1)
        root_instance = storage.read_job_instance_id(*root)
        root_job = storage.load_job(*root)
        root_control = storage.read_job_control(*root)
        root_owner = storage.read_job_current_owner(*root)
        root_events = storage.read_job_events(*root)
        root_input = storage.input_file(*root).read_bytes()
        removed = {
            (node, job_id): storage.read_job_events(node, job_id)
            for node in ('A', 'X', 'B')
            for job_id in storage.list_job_ids(node)
            if storage.load_job(node, job_id).params['lineage'] == 'selected'
            and (node, job_id) != root
        }
        assert root_events and removed and all(removed.values())
        preserved = {
            (node, job_id): _job_state(storage, node, job_id)
            for node in ('A', 'X', 'B', 'C')
            for job_id in storage.list_job_ids(node)
            if (node, job_id) != root and (node, job_id) not in removed
        }
        component_states = {
            component: storage.get_component_state(component)
            for component in (('A', 'X'), ('B',), ('C',))
        }
        aggregate_outputs = {
            node: _files(storage.node_output_dir(node))
            for node in ('A', 'X', 'B', 'C')
        }
        calls_before = list(calls)

        result = _run_selected_with_trace(tmp_path, workflow, entry)

        assert result == 0
        assert calls == calls_before + ([] if entry == 'reset' else [('A', 1, 'selected', 0)])
        assert storage.read_job_instance_id(*root) == root_instance
        assert storage.load_job(*root) == root_job
        assert storage.read_job_control(*root) == root_control
        assert storage.input_file(*root).read_bytes() == root_input
        assert storage.get_job_status(*root) == ('queued' if entry == 'reset' else 'done')
        after_owner = storage.read_job_current_owner(*root)
        if entry == 'reset':
            assert after_owner is None
        else:
            assert after_owner['execution_id'] != root_owner['execution_id']
            assert after_owner['job_instance_id'] == root_instance
            assert after_owner['created_by_execution_id'] == root_owner['created_by_execution_id']
        after_events = storage.read_job_events(*root)
        assert after_events[:len(root_events)] == root_events
        assert len(after_events) > len(root_events)
        for key, events in removed.items():
            assert not storage.job_exists(*key)
            assert not storage.job_base_dir(*key).exists()
            assert storage.read_job_events(*key) == events
        assert {key: _job_state(storage, *key) for key in preserved} == preserved
        assert {
            node: _files(storage.node_output_dir(node))
            for node in aggregate_outputs
        } == aggregate_outputs
        assert storage.get_component_state(('A', 'X')) == component_states[('A', 'X')]
        assert storage.get_component_state(('B',)) == dict(component_states[('B',)], misaligned=True)
        assert storage.get_component_state(('C',)) == component_states[('C',)]
    finally:
        _close(storage)


def _replace_job_instance(connection, node, job_id):
    columns = (
        'node_name', 'job_id', 'parent_json', 'created_at', 'status', 'status_json',
        'generation', 'active_execution_id', 'active_pid', 'active_thread_id',
        'active_started_at', 'restart_requested_at', 'restart_requested_by_pid',
        'restart_reason', 'runtime_json',
    )
    row = connection.execute(
        'SELECT * FROM jobs WHERE node_name=? AND job_id=?', (node, job_id),
    ).fetchone()
    assert row is not None
    connection.execute('DELETE FROM jobs WHERE node_name=? AND job_id=?', (node, job_id))
    connection.execute(
        f"INSERT INTO jobs({','.join(columns)}) VALUES({','.join('?' for _ in columns)})",
        tuple(row[column] for column in columns),
    )
    replacement = connection.execute(
        'SELECT instance_id FROM job_instances WHERE node_name=? AND job_id=?',
        (node, job_id),
    ).fetchone()
    return replacement['instance_id']


def _preparation_rows(storage):
    return [dict(row) for row in storage.db_connection().execute(
        'SELECT * FROM preparation_receipts ORDER BY operation_id'
    )]


def _input_owners(storage, nodes):
    result = {}
    for node in nodes:
        directory = storage.node_input_dir(node)
        for path in directory.rglob('*'):
            if path.is_file():
                relative = path.relative_to(directory).as_posix()
                result[node, relative] = storage.read_node_input_owner(node, relative)
    return result


def test_selected_root_replacement_after_admission_refuses_before_preparation(
    tmp_path, monkeypatch,
):
    workflow, calls = _selected_history(tmp_path)
    storage = workflow.storage
    reserve = FileStorage.reserve_execution_components
    root_instance = storage.read_job_instance_id('A', 1)
    sessions_before = {item['session_id'] for item in storage.list_execution_sessions()}
    calls_before = list(calls)
    replacement = {}

    def replace_after_reservation(self, *args, **kwargs):
        result = reserve(self, *args, **kwargs)
        if self is storage and not replacement:
            replacement['instance'] = self.submit_db_mutation(
                lambda connection: _replace_job_instance(connection, 'A', 1)
            )
            assert self.read_job_current_owner('A', 1) is None
            assert self.get_job_status('A', 1) == 'done'
            replacement['jobs'] = {
                (node, job_id): _job_state(self, node, job_id)
                for node in ('A', 'X', 'B', 'C')
                for job_id in self.list_job_ids(node)
            }
            replacement['nodes'] = {
                node: self.get_node_status(node) for node in ('A', 'X', 'B', 'C')
            }
            replacement['components'] = {
                component: self.get_component_state(component)
                for component in (('A', 'X'), ('B',), ('C',))
            }
            replacement['causes'] = {
                component: self.read_component_misalignment_causes(component)
                for component in (('A', 'X'), ('B',), ('C',))
            }
            replacement['inputs'] = _input_owners(self, ('A', 'X', 'B', 'C'))
            replacement['files'] = _files(tmp_path / 'node')
            replacement['receipts'] = _preparation_rows(self)
        return result

    monkeypatch.setattr(FileStorage, 'reserve_execution_components', replace_after_reservation)
    try:
        with pytest.raises(
            RuntimeError,
            match=r'^Selected preparation job instance changed: A/1$',
        ):
            workflow.run_jobs('A', [1])

        assert replacement['instance'] != root_instance
        assert calls == calls_before
        assert {
            (node, job_id): _job_state(storage, node, job_id)
            for node in ('A', 'X', 'B', 'C')
            for job_id in storage.list_job_ids(node)
        } == replacement['jobs']
        assert {node: storage.get_node_status(node) for node in replacement['nodes']} == replacement['nodes']
        assert {
            component: storage.get_component_state(component)
            for component in replacement['components']
        } == replacement['components']
        assert {
            component: storage.read_component_misalignment_causes(component)
            for component in replacement['causes']
        } == replacement['causes']
        assert _input_owners(storage, ('A', 'X', 'B', 'C')) == replacement['inputs']
        assert _files(tmp_path / 'node') == replacement['files']
        assert _preparation_rows(storage) == replacement['receipts']
        added = [item for item in storage.list_execution_sessions()
                 if item['session_id'] not in sessions_before]
        assert len(added) == 1
        assert (added[0]['selection_kind'], added[0]['selected_jobs']) == (
            'jobs', [('A', 1)],
        )
        assert (added[0]['status'], added[0]['outcome']) == ('terminal', 'failed')
        selected_row, = storage.db_connection().execute(
            'SELECT job_instance_id FROM session_jobs WHERE session_id=?',
            (added[0]['session_id'],),
        ).fetchall()
        assert selected_row['job_instance_id'] == root_instance
        assert storage.get_live_main_session() is None
        assert storage.get_component_reservation(('A', 'X')) is None
    finally:
        _close(storage)
