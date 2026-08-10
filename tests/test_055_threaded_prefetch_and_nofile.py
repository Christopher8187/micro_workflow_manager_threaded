from __future__ import annotations

import threading

from micro_workflow_manager.cli import resource_limits
from micro_workflow_manager.runners.threaded import ThreadedRunner
from micro_workflow_manager.storage.job_sources import (
    PrefetchingQueuedJobObjectSource,
    QueuedJobObjectSource,
)


class _Storage:
    def __init__(self):
        self.loaded_on: list[str] = []
        self.closed_on: list[str] = []

    def validate_node_name(self, value):
        return value

    def load_jobs_batch(self, node_name, job_ids):
        self.loaded_on.append(threading.current_thread().name)
        return list(job_ids)

    def close_thread_connection(self):
        self.closed_on.append(threading.current_thread().name)


def test_queued_source_reserves_before_payload_loading():
    storage = _Storage()
    source = QueuedJobObjectSource(storage, "A", iter(range(1, 11)))
    reserved = source.reserve(4)
    assert reserved == [1, 2, 3, 4]
    assert storage.loaded_on == []
    assert source.load_reserved(reserved) == reserved
    assert storage.loaded_on == [threading.current_thread().name]


def test_threaded_prefetch_loads_payloads_off_worker_source_lock():
    storage = _Storage()
    source = QueuedJobObjectSource(storage, "A", iter(range(1, 129)))
    prefetched = PrefetchingQueuedJobObjectSource(
        storage,
        source,
        prefetch_size=16,
        prefetch_workers=2,
        prefetch_batches=2,
    )
    try:
        result = ThreadedRunner(max_threads=8, poll_interval=0.005).run_job_source(
            "A", prefetched, lambda item: item
        )
    finally:
        prefetched.close()
    assert result == list(range(1, 129))
    assert storage.loaded_on
    assert all(name.startswith("mwf-job-prefetch") for name in storage.loaded_on)


def test_threaded_runner_requests_bounded_prefetch():
    runner = ThreadedRunner(max_threads=50)
    assert runner.prefetches_job_bursts is True
    assert 1 <= runner.job_prefetch_workers() <= 32
    assert runner.job_prefetch_batches() >= runner.job_prefetch_workers()


def test_raise_open_file_limit_never_lowers_and_targets_65536(monkeypatch):
    class FakeResource:
        RLIMIT_NOFILE = 7
        RLIM_INFINITY = -1

        def __init__(self):
            self.limit = (1024, 131072)
            self.calls = []

        def getrlimit(self, _kind):
            return self.limit

        def setrlimit(self, _kind, value):
            self.calls.append(value)
            self.limit = value

    fake = FakeResource()
    monkeypatch.setattr(resource_limits.os, "name", "posix")
    import sys
    monkeypatch.setitem(sys.modules, "resource", fake)
    assert resource_limits.raise_open_file_limit() == 65536
    assert fake.calls == [(65536, 131072)]

    fake.calls.clear()
    fake.limit = (100000, 131072)
    assert resource_limits.raise_open_file_limit() == 100000
    assert fake.calls == []


def test_cli_execution_commands_raise_nofile_before_project_io(monkeypatch):
    import importlib
    module = importlib.import_module("micro_workflow_manager.cli.main")
    calls = []
    monkeypatch.setattr(module, "raise_open_file_limit", lambda: calls.append("raise"))
    monkeypatch.setattr(module, "find_root", lambda: (_ for _ in ()).throw(RuntimeError("stop")))
    for argv in (["run", "A"], ["runfrom", "A"], ["resume", "A"], ["resumefrom", "A"]):
        assert module.main(list(argv)) == 1
    assert calls == ["raise"] * 4
