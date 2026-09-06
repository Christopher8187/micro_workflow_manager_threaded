from __future__ import annotations

import contextvars
import time
from concurrent.futures import ThreadPoolExecutor
from threading import Event

import pytest
from greenlet import greenlet

from micro_workflow_manager import MicroWorkflow
from micro_workflow_manager.storage import FileStorage
from tests.test_090_component_session_settlement import _close


@pytest.mark.parametrize('second_storage', [False, True])
def test_advisory_reentrance_belongs_to_the_actual_greenlet(tmp_path, second_storage):
    storage = FileStorage(tmp_path)
    nested = FileStorage(tmp_path) if second_storage else storage
    try:
        with storage.interprocess_lock('receiver'):
            with nested.interprocess_lock('receiver', timeout=0):
                def contender():
                    with pytest.raises(TimeoutError, match='receiver'):
                        with storage.interprocess_lock('receiver', timeout=0):
                            pass

                child = greenlet(contender)
                child.gr_context = contextvars.copy_context()
                child.switch()
                assert child.dead
        with storage.interprocess_lock('receiver', timeout=0):
            pass
    finally:
        if nested is not storage:
            _close(nested)
        _close(storage)


@pytest.mark.parametrize('shared_name', [True, False])
def test_api_fibers_serialize_only_the_same_advisory_name(tmp_path, shared_name):
    workflow = MicroWorkflow(tmp_path, runner='api', persist_graph=False)
    workflow.graph([('A', 'B')])
    storage = workflow.storage
    active = 0
    peak = 0

    @workflow.task('A', runner='api', max_threads=2)
    def produce(ctx):
        nonlocal active, peak
        name = 'receiver' if shared_name else f'receiver-{ctx.job_id}'
        with storage.interprocess_lock(name, timeout=2):
            with storage.interprocess_lock(name, timeout=0):
                active += 1
                peak = max(peak, active)
                time.sleep(0.05)
                active -= 1

    try:
        workflow.start('A', job_id=1)
        workflow.start('A', job_id=2)
        workflow.run_node('A', ignore_readiness=True)
        assert peak == (1 if shared_name else 2)
        assert storage.get_job_status('A', 1) == 'done'
        assert storage.get_job_status('A', 2) == 'done'
    finally:
        _close(storage)


def test_copied_context_thread_waits_for_advisory_owner(tmp_path):
    storage = FileStorage(tmp_path)
    started = Event()
    entered = Event()

    def contender():
        started.set()
        with storage.interprocess_lock('receiver', timeout=2):
            entered.set()

    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            with storage.interprocess_lock('receiver'):
                future = executor.submit(contextvars.copy_context().run, contender)
                assert started.wait(2)
                assert not entered.wait(0.05)
            future.result(timeout=3)
            assert entered.is_set()
    finally:
        _close(storage)


def test_committed_advisory_acquisition_cannot_be_reclaimed_before_notification_returns(tmp_path, monkeypatch):
    storage = FileStorage(tmp_path)
    entered = []

    def contender():
        with pytest.raises(TimeoutError, match='receiver'):
            with storage.interprocess_lock('receiver', timeout=0):
                entered.append('wrong owner')

    def notify():
        monkeypatch.setattr(storage, 'notify_state_change', lambda: None)
        child = greenlet(contender)
        child.switch()
        assert child.dead

    monkeypatch.setattr(storage, 'notify_state_change', notify)
    try:
        with storage.interprocess_lock('receiver'):
            entered.append('right owner')
        assert entered == ['right owner']
    finally:
        _close(storage)
