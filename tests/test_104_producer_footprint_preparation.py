from __future__ import annotations

import os
import socket
from types import SimpleNamespace

import pytest

from micro_workflow_manager import MicroWorkflow
from micro_workflow_manager.cli.destructive import execute_destructive_command
from micro_workflow_manager.cli.run_commands import run_node
from micro_workflow_manager.models import now
from micro_workflow_manager.processes import process_identity
from tests.test_090_component_session_settlement import _close


def _files(directory):
    return {path.relative_to(directory).as_posix(): path.read_bytes()
            for path in directory.rglob('*') if path.is_file()}


@pytest.mark.parametrize('command', ['reset', 'run'])
@pytest.mark.parametrize('artifact', ['input', 'job'])
@pytest.mark.parametrize('clear_optional_provenance', [False, True])
def test_full_preparation_removes_owned_publications_and_preserves_excluded_receiver(
    tmp_path, command, artifact, clear_optional_provenance,
):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'B'), ('X', 'B')])
    storage = workflow.storage
    publish_a = True
    calls = []

    @workflow.task('A')
    def producer_a(ctx):
        calls.append('A')
        if publish_a:
            if artifact == 'input':
                ctx.node('B').write_input('owned.txt', 'from A')
            else:
                ctx.node('B').add(origin='from-A')

    @workflow.task('X')
    def producer_x(ctx):
        calls.append('X')
        ctx.node('B').write_input('owned.txt', 'from X')
        ctx.node('B').add(origin='from-X')

    @workflow.task('B')
    def receiver(ctx, origin):
        calls.append('B')
        ctx.write_output(origin + '.txt', origin)
        return origin

    for node in ('A', 'X'):
        workflow.start(node)
    workflow.start('B', origin='project')
    try:
        workflow.run()
        before_a = storage.get_component_state(('A',))
        before_b = storage.get_component_state(('B',))
        before_x = storage.get_component_state(('X',))
        assert before_b['lifecycle'] == 'done' and before_b['misaligned'] is False
        producer_a = storage.read_job_current_owner('A', 1)
        producer_x = storage.read_job_current_owner('X', 1)
        jobs = {storage.load_job('B', job_id).params['origin']: job_id
                for job_id in storage.list_job_ids('B')}
        assert storage.read_component_misalignment_causes(('B',)) == []
        if artifact == 'input':
            assert storage.read_node_input_owner('B', 'A/owned.txt') == producer_a
            assert storage.read_node_input_owner('B', 'X/owned.txt') == producer_x
            removed_instance = None
        else:
            publication_a = storage.read_job_current_owner('B', jobs['from-A'])
            publication_x = storage.read_job_current_owner('B', jobs['from-X'])
            publication_project = storage.read_job_current_owner('B', jobs['project'])
            assert publication_a['created_by_execution_id'] == producer_a['execution_id']
            assert publication_x['created_by_execution_id'] == producer_x['execution_id']
            assert publication_project['created_by_execution_id'] is None
            removed_instance = publication_a['job_instance_id']
        if clear_optional_provenance:
            storage.submit_db_mutation(lambda connection: connection.execute('UPDATE jobs SET parent_json=NULL'))
            for node in ('A', 'B', 'X'):
                storage.clear_job_events(node, storage.list_job_ids(node))
        preserved = {job_id: (
            storage.read_job_control('B', job_id), storage.read_job_current_owner('B', job_id),
            storage.read_job_events('B', job_id), _files(storage.job_base_dir('B', job_id)),
        ) for origin, job_id in jobs.items() if origin != 'from-A'}
        output = _files(storage.node_output_dir('B'))
        incoming = tmp_path / 'node' / 'A' / 'input' / 'project-owned.txt'
        incoming.write_bytes(b'preserved starting input')
        project_input = tmp_path / 'node' / 'B' / 'input' / 'project-owned.txt'
        project_input.write_bytes(b'preserved receiver input')
        x_input = tmp_path / 'node' / 'B' / 'input' / 'X' / 'owned.txt'
        x_owner = storage.read_node_input_owner('B', 'X/owned.txt')
        calls_before = list(calls)
        publish_a = False

        if command == 'reset':
            assert execute_destructive_command(
                tmp_path, workflow, SimpleNamespace(command='reset', node='A', yes=True),
            ) == 0
        else:
            assert run_node(tmp_path, workflow, 'A') == 0

        assert calls == calls_before + ([] if command == 'reset' else ['A'])
        if artifact == 'input':
            assert not (tmp_path / 'node' / 'B' / 'input' / 'A' / 'owned.txt').exists()
            assert storage.read_node_input_owner('B', 'A/owned.txt') is None
        else:
            assert not storage.job_exists('B', jobs['from-A'])
            assert not storage.job_base_dir('B', jobs['from-A']).exists()
        assert storage.list_job_ids('B') == sorted(preserved)
        for job_id, expected in preserved.items():
            assert (
                storage.read_job_control('B', job_id), storage.read_job_current_owner('B', job_id),
                storage.read_job_events('B', job_id), _files(storage.job_base_dir('B', job_id)),
            ) == expected
        assert _files(storage.node_output_dir('B')) == output
        assert storage.get_component_state(('B',)) == dict(before_b, misaligned=True)
        assert storage.get_component_state(('X',)) == before_x
        after_a = storage.get_component_state(('A',))
        assert after_a['alignment_generation'] == before_a['alignment_generation'] + 1
        assert after_a['lifecycle'] == ('queued' if command == 'reset' else 'done')
        assert incoming.read_bytes() == b'preserved starting input'
        assert project_input.read_bytes() == b'preserved receiver input'
        assert x_input.read_text() == 'from X'
        assert storage.read_node_input_owner('B', 'X/owned.txt') == x_owner
        assert storage.get_job_execution_owner(producer_a['execution_id']) == producer_a
        cause = {
            'component': ('B',), 'receiver_node': 'B',
            'alignment_generation': before_b['alignment_generation'],
            'producer_node': 'A', 'producer_job_id': 1,
            'preparation_kind': 'preparation-removal', 'operation': command,
            'action': 'delete', 'affected_kind': f'managed-{artifact}',
        }
        if artifact == 'input':
            cause['path'] = 'A/owned.txt'
        else:
            cause.update(job_id=jobs['from-A'], job_instance_id=removed_instance)
        assert storage.read_component_misalignment_causes(('B',)) == [cause]
    finally:
        _close(storage)


def test_reset_notification_failure_keeps_committed_preparation_files_removed(tmp_path, monkeypatch):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'A')])
    storage = workflow.storage

    @workflow.task('A')
    def work(ctx):
        ctx.write_output('result.txt', 'established result')
        return 'established result'

    workflow.start('A')
    try:
        workflow.run()
        before = storage.get_component_state(('A',))
        generation = before['alignment_generation']
        node_output = storage.node_output_dir('A') / 'result.txt'
        job_output = storage.job_base_dir('A', 1) / 'output.json'
        assert node_output.exists() and job_output.exists()
        original = OSError('notification failed after committed preparation')
        notify = storage.notify_state_change
        injected = False

        def fail_once_after_commit():
            nonlocal injected
            state = storage.get_component_state(('A',))
            if not injected and state['alignment_generation'] == generation + 1:
                injected = True
                raise original
            return notify()

        monkeypatch.setattr(storage, 'notify_state_change', fail_once_after_commit)
        with pytest.raises(OSError) as caught:
            execute_destructive_command(
                tmp_path, workflow, SimpleNamespace(command='reset', node='A', yes=True),
            )
        assert caught.value is original
        assert injected
        assert storage.get_component_state(('A',)) == dict(
            before, lifecycle='queued', stability=None, instability_origin=None,
            misaligned=False, alignment_generation=generation + 1,
        )
        assert storage.get_job_status('A', 1) == 'queued'
        assert not node_output.exists()
        assert not job_output.exists()
    finally:
        _close(storage)


@pytest.mark.parametrize('keep_trace', [False, True])
def test_preparation_clears_only_owned_orphan_trace_in_excluded_receivers(tmp_path, keep_trace):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'B'), ('X', 'C')])
    storage = workflow.storage

    @workflow.task('A')
    def producer_a(ctx):
        ctx.node('B').add()

    @workflow.task('X')
    def producer_x(ctx):
        ctx.node('C').add()

    @workflow.task('B')
    @workflow.task('C')
    def receiver(ctx):
        ctx.trace('retained receiver trace', content=ctx.current_node)
        ctx.write_output('retained.txt', ctx.current_node)

    workflow.start('A')
    workflow.start('X')
    try:
        workflow.run()
        for node in ('B', 'C'):
            assert storage.read_job_current_owner(node, 1)['created_by_execution_id'] is not None
            storage.delete_job(node, 1, preserve_events=True)
        events = {node: storage.read_job_events(node, 1) for node in ('B', 'C')}
        assert all(events.values())
        states = {node: storage.get_component_state((node,)) for node in ('B', 'C')}
        files = {node: _files(tmp_path / 'node' / node) for node in ('B', 'C')}
        assert execute_destructive_command(
            tmp_path, workflow, SimpleNamespace(command='reset', node='A', yes=True, keeptrace=keep_trace),
        ) == 0
        assert storage.read_job_events('B', 1) == (events['B'] if keep_trace else [])
        assert storage.read_job_events('C', 1) == events['C']
        assert {node: storage.get_component_state((node,)) for node in ('B', 'C')} == states
        assert {node: _files(tmp_path / 'node' / node) for node in ('B', 'C')} == files
    finally:
        _close(storage)


@pytest.mark.parametrize('keep_trace', [False, True])
def test_keep_trace_does_not_guard_an_orphan_only_excluded_receiver(
    tmp_path, keep_trace,
):
    workflow = MicroWorkflow(tmp_path, runner='direct', persist_graph=False)
    workflow.graph([('A', 'B')])
    storage = workflow.storage
    publish = True

    @workflow.task('A')
    def producer(ctx):
        if publish:
            ctx.node('B').add()

    @workflow.task('B')
    def receiver(ctx):
        ctx.trace('retained receiver trace')
        ctx.write_output('retained.txt', 'B')

    workflow.start('A')
    try:
        workflow.run()
        assert storage.read_job_current_owner('B', 1)['created_by_execution_id'] is not None
        storage.delete_job('B', 1, preserve_events=True)
        publish = False
        session_id = 'b' * 32
        identity = process_identity(os.getpid())
        assert identity
        snapshot = workflow.topology.snapshot()
        storage.create_execution_session(
            session_id, session_kind='interrupt', command='resume',
            start_component=('B',), selected_components=[('B',)], started_at=now(),
            hostname=socket.gethostname(), pid=os.getpid(), process_identity=identity,
        )
        storage.reserve_execution_components(session_id, expected_shape=snapshot.shape_json)
        before_a = (
            storage.get_component_state(('A',)), storage.read_job_control('A', 1),
            storage.read_job_events('A', 1), _files(tmp_path / 'node' / 'A'),
        )
        before_b = (
            storage.get_component_state(('B',)), storage.read_job_events('B', 1),
            _files(tmp_path / 'node' / 'B'), storage.get_execution_session(session_id),
            storage.get_component_reservation(('B',)),
        )

        if keep_trace:
            assert run_node(tmp_path, workflow, 'A', keep_trace=True) == 0
        else:
            with pytest.raises(RuntimeError) as caught:
                run_node(tmp_path, workflow, 'A')
            assert 'B' in str(caught.value) and session_id in str(caught.value)
            assert (
                storage.get_component_state(('A',)), storage.read_job_control('A', 1),
                storage.read_job_events('A', 1), _files(tmp_path / 'node' / 'A'),
            ) == before_a
        assert (
            storage.get_component_state(('B',)), storage.read_job_events('B', 1),
            _files(tmp_path / 'node' / 'B'), storage.get_execution_session(session_id),
            storage.get_component_reservation(('B',)),
        ) == before_b
        assert storage.db_connection().execute(
            'SELECT 1 FROM receiver_mutation_guards WHERE receiver_node=?', ('B',),
        ).fetchone() is None
    finally:
        _close(storage)
