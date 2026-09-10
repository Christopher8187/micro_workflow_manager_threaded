"""Relative aggregate API updates are one serialized native decision."""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor

from micro_workflow_manager import cli
from micro_workflow_manager.storage import FileStorage
from micro_workflow_manager.storage.thread_overrides import ThreadOverrideStorageMixin
from tests.test_149_interrupt_declarations import _make_interrupt_project


def test_concurrent_relative_api_total_updates_both_apply(
    tmp_path, monkeypatch,
):
    _make_interrupt_project(
        tmp_path,
        monkeypatch,
        edges=[("A", "A")],
        jobs={"A"},
    )
    original_read = ThreadOverrideStorageMixin.read_api_total_limit
    simultaneous_reads = threading.Barrier(2)

    def synchronized_read(storage):
        observed = original_read(storage)
        simultaneous_reads.wait(timeout=10)
        return observed

    monkeypatch.setattr(
        ThreadOverrideStorageMixin,
        "read_api_total_limit",
        synchronized_read,
    )
    simultaneous_commands = threading.Barrier(2)

    def increment(_):
        simultaneous_commands.wait(timeout=10)
        return cli.main(["threads", "--api-total", "+1"])

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(increment, range(2)))

    storage = FileStorage(tmp_path)
    try:
        assert sorted(results) == [0, 0]
        assert original_read(storage) == 3
        assert [tuple(row) for row in storage.db_connection().execute(
            "SELECT singleton, value FROM api_thread_limit"
        )] == [(1, 3)]
    finally:
        storage.close_database_connections()
