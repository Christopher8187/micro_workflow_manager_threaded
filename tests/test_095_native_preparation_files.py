from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

from micro_workflow_manager import cli
from micro_workflow_manager.cli.project import load_workflow
from micro_workflow_manager.cli.run_commands import run_node
from micro_workflow_manager.models import Job
from micro_workflow_manager.storage import preparation_files
from micro_workflow_manager.storage.job_preparation import NodeJobPreparation, PreparationJob, read_job_preparation
from micro_workflow_manager.storage.preparation_staging import PreparationStaging
from tests.test_036_hoeflein_scheduling import make_project
from tests.test_076_component_state_transitions import _session
from tests.test_090_component_session_settlement import _close, _rows
from tests.test_094_native_fresh_preparation import _fresh_owner


def _plan(*, clear_output=True, jobs=()):
    return NodeJobPreparation('A', jobs, (), (), tuple(job.job_id for job in jobs), (), clear_output, True)


def _job(job_id):
    return PreparationJob(job_id, None, 'done', 0, None, f'{job_id:032x}', None)


@pytest.mark.parametrize('command', ['reset', 'resetfrom'])
def test_fresh_preparation_preserves_unattributed_receiver_job_files(tmp_path, monkeypatch, command):
    make_project(
        tmp_path, monkeypatch, edges="EDGES = [('A', 'B')]",
        files={
            'A': '''
                from micro_workflow_manager import NodeRouter
                router = NodeRouter('A')
                router.create_job(number=1)
                @router.task
                def run(ctx):
                    ctx.node('B').add(value='selected producer')
            ''',
            'B': '''
                from micro_workflow_manager import NodeRouter
                router = NodeRouter('B')
                @router.task
                def run(ctx, value):
                    return value
            ''',
        },
    )
    assert cli.main(['runfrom', 'A']) == 0
    workflow = load_workflow(tmp_path)
    storage = workflow.storage
    try:
        assert storage.list_job_ids('B') == [1]
        jobs = tmp_path / 'node' / 'B' / 'jobs'
        retained = {jobs / '900' / 'unattributed.txt': b'uncaptured directory contents',
                    jobs / 'operator.txt': b'uncaptured file'}
        for path, content in retained.items():
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(content)
        assert cli.main([command, 'A', '--yes']) == 0
        assert storage.list_job_ids('B') == []
        assert not (jobs / '1').exists()
        assert {path: path.read_bytes() for path in retained} == retained
        assert storage.get_component_state(('A',))['lifecycle'] == 'queued'
        assert storage.get_component_state(('B',))['lifecycle'] == ('queued' if command == 'resetfrom' else 'done')
    finally:
        _close(storage)


def test_restoration_failure_preserves_original_error_and_recovery_location(tmp_path, monkeypatch):
    output = tmp_path / 'node' / 'A' / 'output'
    output.mkdir(parents=True)
    (output / 'retained.txt').write_bytes(b'original result')

    class PreparationError(RuntimeError):
        def add_note(self, note):
            raise AttributeError('add_note is unavailable on supported Python 3.10')

    original = PreparationError('database preparation failed')
    rename = Path.rename
    reached = []

    def fail_restoration(path, target):
        if path.name == '0' and path.parent.parent.name == 'preparation-trash' and target == output:
            saved = path / 'retained.txt'
            assert saved.read_bytes() == b'original result'
            assert not output.exists()
            reached.append((path, target))
            raise OSError('restoration blocked by a file handle')
        return rename(path, target)

    monkeypatch.setattr(Path, 'rename', fail_restoration)
    with pytest.raises(PreparationError) as caught:
        with preparation_files.stage_preparation_files(tmp_path, [_plan()]):
            raise original
    assert caught.value is original
    assert len(reached) == 1 and reached[0][1] == output
    assert any('Preparation files require recovery at' in note for note in original.__notes__)
    saved, = (tmp_path / '.mwf' / 'preparation-trash').glob('*/0/retained.txt')
    assert saved.read_bytes() == b'original result'


def test_committed_preparation_continues_when_retired_files_cannot_be_deleted(tmp_path, monkeypatch, caplog):
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
    assert cli.main(['run', 'A']) == 0
    workflow = load_workflow(tmp_path)
    storage = workflow.storage
    notify = storage.notify_queue_changes
    notifications = []
    retained_cleanup = []
    generation = storage.get_component_state(('A',))['alignment_generation']

    def record(nodes):
        notifications.append((tuple(nodes), storage.get_component_state(('A',))['lifecycle']))
        return notify(nodes)

    def retain_private_trash(files, relative, expected):
        if not retained_cleanup and (files.root / Path(relative)).exists():
            assert relative.startswith(files.relative + '/')
            retained_cleanup.append(relative)
            raise PermissionError('retired file is still open')
        return remove_recorded(files, relative, expected)

    monkeypatch.setattr(storage, 'notify_queue_changes', record)
    remove_recorded = PreparationStaging._remove_recorded_tree
    monkeypatch.setattr(PreparationStaging, '_remove_recorded_tree', retain_private_trash)
    try:
        assert run_node(tmp_path, workflow, 'A') == 0
        assert storage.get_component_state(('A',))['alignment_generation'] == generation + 1
        assert storage.get_component_state(('A',))['lifecycle'] == 'done'
        assert storage.get_job_status('A', 1) == 'done'
        assert (('A',), 'queued') in notifications
        assert (tmp_path / 'node' / 'A' / 'output' / 'ran.txt').read_bytes() == b'A'
        assert all(session['outcome'] == 'done' for session in storage.list_execution_sessions())
        assert len(retained_cleanup) == 1
        assert 'temporary files remain' in caplog.text
        saved, = (tmp_path / '.mwf' / 'preparation-trash').glob('*/*/ran.txt')
        assert saved.read_bytes() == b'A'
    finally:
        _close(storage)


@pytest.mark.parametrize('fail_during_staging', [False, True])
def test_preparation_restores_every_moved_path_on_failure(tmp_path, monkeypatch, fail_during_staging):
    paths = [tmp_path / 'node' / 'A' / 'output' / 'aggregate.txt',
             tmp_path / 'node' / 'A' / 'jobs' / '1' / 'output.json',
             tmp_path / 'node' / 'A' / 'jobs' / '2' / 'output.json']
    for number, path in enumerate(paths):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(str(number).encode())
    before = {path: path.read_bytes() for path in paths}
    rename, moved = Path.rename, []
    original = OSError('injected preparation failure')

    def fail_second_move(path, target):
        if Path(target).parent.parent.name == 'preparation-trash':
            if fail_during_staging and len(moved) == 1:
                raise original
            moved.append(path)
        return rename(path, target)

    monkeypatch.setattr(Path, 'rename', fail_second_move)
    with pytest.raises(OSError) as caught:
        with preparation_files.stage_preparation_files(tmp_path, [_plan(jobs=(_job(1), _job(2)))]):
            assert not fail_during_staging
            assert len(moved) == 3
            raise original
    assert caught.value is original
    assert {path: path.read_bytes() for path in paths} == before
    assert not (tmp_path / '.mwf' / 'preparation-trash').exists()


def test_preparation_refuses_parent_links_into_an_unselected_node(tmp_path):
    jobs = tmp_path / 'node' / 'B' / 'jobs'
    (jobs / '1').mkdir(parents=True)
    original = jobs / '1' / 'output.json'
    original.write_bytes(b'unselected result')
    link = tmp_path / 'node' / 'A' / 'jobs'
    link.parent.mkdir(parents=True)
    if os.name == 'nt':
        result = subprocess.run(['cmd', '/c', 'mklink', '/J', str(link), str(jobs)],
                                capture_output=True, text=True, creationflags=subprocess.CREATE_NO_WINDOW)
        assert result.returncode == 0, result.stdout + result.stderr
    else:
        link.symlink_to(jobs, target_is_directory=True)
    with pytest.raises(ValueError, match='Unsafe preparation path'):
        with preparation_files.stage_preparation_files(tmp_path, [_plan(clear_output=False, jobs=(_job(1),))]):
            pytest.fail('Preparation followed a parent link into an unselected node')
    assert original.read_bytes() == b'unselected result'


def test_preparation_restoration_preserves_a_new_file_at_the_original_path(tmp_path):
    output = tmp_path / 'node' / 'A' / 'jobs' / '1' / 'output.json'
    output.parent.mkdir(parents=True)
    output.write_bytes(b'original result')
    original = RuntimeError('job changed during preparation')
    with pytest.raises(RuntimeError) as caught:
        with preparation_files.stage_preparation_files(tmp_path, [_plan(clear_output=False, jobs=(_job(1),))]):
            output.write_bytes(b'newer result')
            raise original
    assert caught.value is original
    assert output.read_bytes() == b'newer result'
    assert any('restoration target changed' in note for note in original.__notes__)
    saved, = (tmp_path / '.mwf' / 'preparation-trash').glob('*/0')
    assert saved.read_bytes() == b'original result'


@pytest.mark.parametrize('change', ['replace-job', 'new-job', 'orphan-created-event'])
def test_component_preparation_rechecks_job_instances_and_orphan_identity(tmp_path, change):
    storage, snapshot = _fresh_owner(tmp_path)
    try:
        _session(storage, 'creator-interrupt', 'interrupt', ('B',), snapshot)
        storage.reserve_execution_components('creator-interrupt', expected_shape=snapshot.shape_json)
        storage.create_job(Job(node_name='B', job_id=1, params={}))
        _, producer_execution = storage.claim_job_execution(
            'B', 1, started_at='2026-09-06T12:00:00+00:00',
            session_id='creator-interrupt', component=('B',),
        )
        storage.create_job(Job(node_name='A', job_id=1, params={'original': True},
                               parent={'from_node': 'B', 'from_job': 1}, producer_component=('B',),
                               job_kind='dag'), producer_execution_id=producer_execution)
        if change == 'orphan-created-event':
            generation, execution = storage.claim_job_execution(
                'A', 1, started_at='2026-09-06T12:00:01+00:00',
                session_id='fresh-main', component=('A',),
            )
            storage.finalize_job_execution('A', 1, generation, execution, 'done')
            storage.delete_job('A', 1, preserve_events=True)
        expected = storage.read_component_fresh_preparation('fresh-main', ('A',), expected_shape=snapshot.shape_json)
        plan = read_job_preparation(storage, ['A'], {('B',)}, reset_retained=True, preserve_external=False)
        if change == 'replace-job':
            assert plan[0].delete_ids == (1,)
            storage.delete_job('A', 1)
            storage.create_job(Job(node_name='A', job_id=1, params={'newer': True}))
        elif change == 'new-job':
            storage.create_job(Job(node_name='A', job_id=2, params={'newer': True}))
        else:
            assert plan[0].orphan_ids == (1,)
            storage.submit_db_mutation(lambda connection: connection.execute(
                'INSERT INTO job_events(node_name, job_id, time, event, data_json) '
                'SELECT node_name, job_id, time, event, data_json FROM job_events '
                "WHERE node_name='A' AND job_id=1 AND event='created' ORDER BY event_id DESC LIMIT 1",
            ))
        before = _rows(storage)
        with pytest.raises(RuntimeError, match='Jobs changed during full preparation'):
            storage.complete_component_fresh_preparation('fresh-main', ('A',), expected_state=expected, job_preparation=plan)
        assert _rows(storage) == before
    finally:
        _close(storage)
