"""Exercise cooperative holds and API admission races with native owners."""

from __future__ import annotations

import subprocess
import time

import pytest

from micro_workflow_manager.models import Job, now
from micro_workflow_manager.storage import FileStorage
from micro_workflow_manager.storage.job_sources import RefreshableQueuedJobObjectSource
from micro_workflow_manager.workflow.admission_sources import ClaimedQueuedJobSource
from tests.test_036_hoeflein_scheduling import make_project
from tests.test_154_interrupt_scope_transfers import _admit_interrupt, _begin_and_claim, _close, _fixture, _session
from tests.test_155_explicit_interrupt_execution import _start, _wait_until


@pytest.mark.parametrize('runner', ['direct', 'threaded', 'api', 'process'])
def test_cooperative_checkpoint_does_not_acknowledge_while_publication_fence_is_held(
    tmp_path, monkeypatch, runner,
):
    make_project(
        tmp_path, monkeypatch, edges="EDGES = [('P', 'I')]", runner=runner,
        files={
            'P': '''
                import time
                from micro_workflow_manager import NodeRouter
                router = NodeRouter('P')
                router.create_job(params={})
                @router.task
                def work(ctx):
                    root = ctx.system.storage.project_dir
                    deadline = time.monotonic() + 40
                    count = 0
                    with ctx.side_effects():
                        (root / 'fence-active').write_text(ctx.execution_id, encoding='utf-8')
                        while not (root / 'fence-release').exists():
                            assert time.monotonic() < deadline
                            ctx.checkpoint('inside publication fence')
                            count += 1
                            (root / 'fence-passes').write_text(str(count), encoding='utf-8')
                            time.sleep(0.01)
                    ctx.checkpoint('outside publication fence')
                    (root / 'after-pause').write_text(ctx.execution_id, encoding='utf-8')
                    return 'parent complete'
            ''',
            'I': '''
                import time
                from micro_workflow_manager import NodeRouter
                router = NodeRouter('I', interrupt=True)
                router.create_job(params={})
                @router.task
                def work(ctx):
                    root = ctx.system.storage.project_dir
                    (root / 'target-active').write_text(ctx.execution_id, encoding='utf-8')
                    deadline = time.monotonic() + 40
                    while not (root / 'target-release').exists():
                        assert time.monotonic() < deadline
                        ctx.checkpoint('target frozen')
                        time.sleep(0.01)
                    return 'target complete'
            ''',
        },
    )
    parent = _start(tmp_path, ['run', 'P', '--runner', runner])
    child = storage = None

    def fence_passes():
        try:
            return int((tmp_path / 'fence-passes').read_text(encoding='utf-8'))
        except (FileNotFoundError, ValueError):
            return 0

    try:
        _wait_until(lambda: fence_passes() > 0 or parent.poll() is not None)
        assert parent.poll() is None, parent.communicate(timeout=5)
        storage = FileStorage(tmp_path)
        owner = storage.read_job_current_owner('P', 1)
        child = _start(tmp_path, ['run', 'I', '--interrupt', '--runner', runner])

        def pause_requested():
            return storage.db_connection().execute(
                'SELECT child_session_id FROM interrupt_pause_requests WHERE owner_session_id=?',
                (owner['session_id'],),
            ).fetchone()

        _wait_until(lambda: child.poll() is not None or pause_requested() is not None)
        assert child.poll() is None, child.communicate(timeout=5)
        child_id = pause_requested()[0]
        count = fence_passes()
        _wait_until(lambda: fence_passes() > count + 1 or parent.poll() is not None)
        assert parent.poll() is None, parent.communicate(timeout=5)
        assert not (tmp_path / 'target-active').exists()
        assert storage.get_component_holds(('I',)) == []
        assert storage.db_connection().execute(
            'SELECT COUNT(*) FROM interrupt_paused_executions '
            'WHERE child_session_id=? AND acknowledged_at IS NOT NULL',
            (child_id,),
        ).fetchone()[0] == 0

        (tmp_path / 'fence-release').touch()
        _wait_until(lambda: (tmp_path / 'target-active').exists() or child.poll() is not None)
        assert child.poll() is None, child.communicate(timeout=5)
        assert not (tmp_path / 'after-pause').exists()
        assert parent.poll() is None
        paused = storage.db_connection().execute(
            'SELECT execution_id, generation FROM interrupt_paused_executions '
            'WHERE child_session_id=? AND acknowledged_at IS NOT NULL',
            (child_id,),
        ).fetchall()
        assert [tuple(row) for row in paused] == [(owner['execution_id'], owner['generation'])]
        assert storage.read_job_current_owner('I', 1)['session_id'] == child_id
        (tmp_path / 'target-release').touch()
        child_out, child_err = child.communicate(timeout=30)
        parent_out, parent_err = parent.communicate(timeout=30)
        assert child.returncode == 0, child_out + child_err
        assert parent.returncode == 0, parent_out + parent_err
        assert (tmp_path / 'after-pause').read_text(encoding='utf-8') == owner['execution_id']
        assert storage.get_component_holds(('I',)) == []
        assert storage.get_job_status('P', 1) == 'done'
        assert storage.get_job_status('I', 1) == 'done'
    finally:
        (tmp_path / 'fence-release').touch(exist_ok=True)
        (tmp_path / 'target-release').touch(exist_ok=True)
        for process in (child, parent):
            if process is not None:
                try:
                    process.communicate(timeout=20)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.communicate(timeout=10)
                    raise
        if storage is not None:
            _close(storage)


def test_api_claim_source_retains_pulled_jobs_when_interrupt_arrives_before_claim(tmp_path, monkeypatch):
    storage, topology = _fixture(tmp_path)
    _session(storage, topology, 'parent', kind='main', selected=[('P',), ('I',), ('D',)])
    _begin_and_claim(storage, topology, 'parent', ('P',), 'P')
    for job_id in (2, 3):
        storage.create_job(Job(node_name='P', job_id=job_id, params={'id': job_id}))
    source = RefreshableQueuedJobObjectSource(storage, 'P')
    original_pull = source.pull
    pulled = []

    def interrupt_after_pull(max_items):
        jobs = original_pull(max_items)
        pulled.extend(job.job_id for job in jobs)
        _admit_interrupt(storage, topology, 'child', selected=[('I',)], predecessors=[('P',)])
        return jobs

    monkeypatch.setattr(source, 'pull', interrupt_after_pull)
    claimed = ClaimedQueuedJobSource(
        storage,
        'P',
        source,
        session_id='parent',
        component=('P',),
        task_started_data={
            'task': 'work',
            'task_role': 'main',
            'attempt': 1,
            'repeat_index': 1,
            'previous_error': None,
        },
        required_params={'id'},
        allowed_params={'id'},
    )
    try:
        assert claimed.pull(2) == []
        assert pulled == [2, 3]
        assert source.remaining_hint() == 0
        assert claimed.remaining_hint() == 2
        for job_id in (2, 3):
            assert storage.get_job_status('P', job_id) == 'queued'
            assert storage.read_job_current_owner('P', job_id) is None
            names = [event['event'] for event in storage.read_job_events('P', job_id)]
            assert 'started' not in names
            assert 'task_started' not in names
        storage.decide_execution_session_exit('child', outcome='done', finished_at=now())
        assert claimed.wait_for_change(timeout=0) is True
        first = claimed.pull(1)
        second = claimed.pull(8)
        assert [item.job.job_id for item in first] == [2]
        assert [item.job.job_id for item in second] == [3]
        assert claimed.remaining_hint() == 0
        assert pulled == [2, 3]
        for item in first + second:
            assert item.task_started_recorded is True
            owner = storage.read_job_current_owner('P', item.job.job_id)
            assert owner['session_id'] == 'parent'
            assert owner['execution_id'] == item.execution_id
            assert owner['generation'] == item.generation == 0
            names = [
                event['event']
                for event in storage.read_job_events('P', item.job.job_id)
            ]
            assert names.count('started') == 1
            assert names.count('task_started') == 1
            assert names.index('started') < names.index('task_started')
    finally:
        claimed.close()
        _close(storage)


def test_handle_created_before_interrupt_acknowledges_before_its_next_publication(
    tmp_path, monkeypatch,
):
    make_project(
        tmp_path, monkeypatch, edges="EDGES = [('P', 'I')]", runner='direct',
        files={
            'P': '''
                import time
                from micro_workflow_manager import NodeRouter
                router = NodeRouter('P')
                router.create_job(params={})
                @router.task
                def work(ctx):
                    root = ctx.system.storage.project_dir
                    target = ctx.node('I')
                    (root / 'handle-ready').touch()
                    deadline = time.monotonic() + 40
                    while not (root / 'handle-release').exists():
                        assert time.monotonic() < deadline
                        time.sleep(0.01)
                    (root / 'handle-call').touch()
                    target.add()
                    (root / 'handle-published').touch()
                    return 'parent complete'
            ''',
            'I': '''
                import time
                from micro_workflow_manager import NodeRouter
                router = NodeRouter('I', interrupt=True)
                router.create_job(params={})
                @router.task
                def work(ctx):
                    root = ctx.system.storage.project_dir
                    (root / 'target-active').write_text(ctx.execution_id, encoding='utf-8')
                    deadline = time.monotonic() + 40
                    while not (root / 'target-release').exists():
                        assert time.monotonic() < deadline
                        ctx.checkpoint('target frozen')
                        time.sleep(0.01)
                    return 'target complete'
            ''',
        },
    )
    parent = _start(tmp_path, ['run', 'P', '--runner', 'direct'])
    child = storage = None
    try:
        _wait_until(lambda: (tmp_path / 'handle-ready').exists() or parent.poll() is not None)
        assert parent.poll() is None, parent.communicate(timeout=5)
        storage = FileStorage(tmp_path)
        owner = storage.read_job_current_owner('P', 1)
        child = _start(tmp_path, ['run', 'I', '--interrupt', '--runner', 'direct'])

        def pause_request():
            return storage.db_connection().execute(
                'SELECT child_session_id FROM interrupt_pause_requests WHERE owner_session_id=?',
                (owner['session_id'],),
            ).fetchone()

        _wait_until(lambda: child.poll() is not None or pause_request() is not None)
        assert child.poll() is None, child.communicate(timeout=5)
        child_id = pause_request()[0]
        (tmp_path / 'handle-release').touch()
        _wait_until(lambda: (tmp_path / 'target-active').exists() or child.poll() is not None)
        assert child.poll() is None, child.communicate(timeout=5)
        assert (tmp_path / 'handle-call').exists()
        assert not (tmp_path / 'handle-published').exists()
        acknowledged = storage.db_connection().execute(
            'SELECT execution_id, generation FROM interrupt_paused_executions '
            'WHERE child_session_id=? AND acknowledged_at IS NOT NULL',
            (child_id,),
        ).fetchall()
        assert [tuple(row) for row in acknowledged] == [
            (owner['execution_id'], owner['generation'])
        ]

        (tmp_path / 'target-release').touch()
        child_out, child_err = child.communicate(timeout=30)
        parent_out, parent_err = parent.communicate(timeout=30)
        assert child.returncode == 0, child_out + child_err
        assert parent.returncode == 0, parent_out + parent_err
        assert (tmp_path / 'handle-published').exists()
        assert storage.get_job_status('I', 1) == 'done'
        assert storage.get_job_status('I', 2) == 'queued'
        state = storage.get_component_state(('I',))
        assert state['lifecycle'] == 'done' and state['misaligned']
    finally:
        (tmp_path / 'handle-release').touch(exist_ok=True)
        (tmp_path / 'target-release').touch(exist_ok=True)
        for process in (child, parent):
            if process is not None:
                try:
                    process.communicate(timeout=20)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.communicate(timeout=10)
                    raise
        if storage is not None:
            _close(storage)
