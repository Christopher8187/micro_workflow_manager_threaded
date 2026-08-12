from __future__ import annotations

import json
import os
import subprocess
import sys
import textwrap
import threading
import time
from pathlib import Path

import pytest

from micro_workflow_manager import cli
from micro_workflow_manager.models import Job
from micro_workflow_manager.storage import FileStorage


def test_job_event_cursor_and_local_commit_wakeup(tmp_path):
    storage = FileStorage(tmp_path)
    woke = threading.Event()
    unsubscribe = storage.subscribe_state_changes(woke.set)
    try:
        cursor = storage.latest_job_event_id()
        storage.create_job(Job(node_name="A", job_id=1, params={"value": 1}))
        assert woke.wait(1)
        events = storage.read_job_events_since(cursor, node_names=["A"])
    finally:
        unsubscribe()

    assert events
    assert events[-1]["node_name"] == "A"
    assert events[-1]["job_id"] == 1
    assert events[-1]["event"] == "created"
    assert events[-1]["event_id"] > cursor


def test_cross_process_commit_wakes_subscriber(tmp_path):
    storage = FileStorage(tmp_path)
    ready = tmp_path / "subscriber-ready"
    woke = tmp_path / "subscriber-woke"
    code = textwrap.dedent(
        """
        import sys
        import threading
        from pathlib import Path
        from micro_workflow_manager.storage import FileStorage

        root = Path(sys.argv[1])
        ready = Path(sys.argv[2])
        woke = Path(sys.argv[3])
        storage = FileStorage(root)
        event = threading.Event()
        unsubscribe = storage.subscribe_state_changes(
            event.set, local=False, cross_process=True
        )
        ready.write_text("ready", encoding="utf-8")
        try:
            if event.wait(3):
                woke.write_text("woke", encoding="utf-8")
        finally:
            unsubscribe()
        """
    )
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(Path(__file__).resolve().parents[1])
    child = subprocess.Popen(
        [sys.executable, "-c", code, str(tmp_path), str(ready), str(woke)],
        env=environment,
    )
    try:
        deadline = time.monotonic() + 3
        while not ready.exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        assert ready.exists()
        # Let the publisher's bounded subscriber cache observe the new record.
        time.sleep(0.30)
        storage.create_job(Job(node_name="A", job_id=1, params={}))
        deadline = time.monotonic() + 2
        while not woke.exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        assert woke.exists()
    finally:
        child.wait(timeout=5)


def _make_top_project(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    behavior = tmp_path / "src" / "node_behavior"
    behavior.mkdir(parents=True)
    (tmp_path / "src" / "graph.py").write_text(
        "EDGES = [('A', 'B')]\n", encoding="utf-8"
    )
    (behavior / "A.py").write_text(
        textwrap.dedent(
            """
            from micro_workflow_manager import NodeRouter
            router = NodeRouter("A", max_threads=4)
            router.create_job(number=4)
            @router.task
            def run(ctx):
                return ctx.job_id
            """
        ).strip(),
        encoding="utf-8",
    )
    (behavior / "B.py").write_text(
        textwrap.dedent(
            """
            from micro_workflow_manager import NodeRouter
            router = NodeRouter("B")
            @router.task
            def run(ctx):
                return None
            """
        ).strip(),
        encoding="utf-8",
    )
    assert cli.main(["init"]) == 0
    assert cli.main(["graph", "src/graph.py", "--runner", "direct"]) == 0


def test_top_once_json_exposes_event_and_writer_diagnostics(
    tmp_path, monkeypatch, capsys
):
    _make_top_project(tmp_path, monkeypatch)
    capsys.readouterr()

    assert cli.main(["run", "A", "--runner", "direct"]) == 0
    capsys.readouterr()
    assert cli.main(["top", "--once", "--json", "--events", "2"]) == 0
    snapshot = json.loads(capsys.readouterr().out)

    assert snapshot["event_driven"] is True
    assert snapshot["database"]["latest_event_id"] >= 8
    assert snapshot["mutation_writer"]["source"] in {
        "local",
        "active-process",
        "stale",
        "unavailable",
    }
    assert "durability_backlog" in snapshot["mutation_writer"]
    assert [row["node"] for row in snapshot["nodes"]] == ["A", "B"]
    assert snapshot["recent_events"]


def test_top_help_and_text_surface_htop_style_fields(capsys, tmp_path, monkeypatch):
    _make_top_project(tmp_path, monkeypatch)
    capsys.readouterr()

    assert cli.main(["top", "--once", "--events", "0"]) == 0
    output = capsys.readouterr().out
    assert "mwf top" in output
    assert "writer source=" in output
    assert "START/s" in output
    assert "TERM95" in output


def test_mutation_writer_retires_immediately_after_draining_queue(tmp_path):
    storage = FileStorage(tmp_path)
    storage.submit_db_mutation(lambda connection: connection.execute("SELECT 1").fetchone(), wait=True)

    deadline = time.monotonic() + 0.15
    while storage.mutation_writer_diagnostics()["writer_alive"] and time.monotonic() < deadline:
        time.sleep(0.002)

    assert storage.mutation_writer_diagnostics()["writer_alive"] is False


def test_job_event_append_uses_one_groupable_journal_mutation(tmp_path, monkeypatch):
    storage = FileStorage(tmp_path)
    captured = {}

    class RecordingConnection:
        def executemany(self, sql, rows):
            captured["sql"] = sql
            captured["rows"] = list(rows)

    def submit(group_key, item, operation, **options):
        captured["group_key"] = group_key
        captured["options"] = options
        captured["outcomes"] = operation(RecordingConnection(), [item])

    monkeypatch.setattr(storage, "submit_grouped_db_mutation", submit)
    storage.append_job_event("node", 7, "trace", name="batched")

    assert captured["group_key"] == ("job-event-appends",)
    assert captured["options"] == {"priority": 10, "collect_seconds": 0.001}
    assert captured["outcomes"] == [(True, None)]
    assert captured["rows"][0][0:2] == ("node", 7)
    assert captured["rows"][0][3] == "trace"
    assert json.loads(captured["rows"][0][4]) == {"name": "batched"}


def test_api_job_event_append_can_return_a_future_and_flush_in_order(tmp_path):
    storage = FileStorage(tmp_path)
    storage.create_job(Job(node_name="A", job_id=1, params={}))
    generation, execution_id = storage.claim_job_execution(
        "A", 1, started_at="2026-01-01T00:00:00"
    )

    first = storage.append_job_event(
        "A", 1, "trace",
        _wait=False,
        _execution_generation=generation,
        _execution_id=execution_id,
        sequence=1,
    )
    second = storage.append_job_event(
        "A", 1, "trace",
        _wait=False,
        _execution_generation=generation,
        _execution_id=execution_id,
        sequence=2,
    )

    first.result()
    second.result()
    events = [
        event for event in storage.read_job_events("A", 1)
        if event["event"] == "trace"
    ]
    assert [event["sequence"] for event in events] == [1, 2]


def test_api_job_event_append_rejects_superseded_execution(tmp_path):
    from micro_workflow_manager.errors import JobRestartedError

    storage = FileStorage(tmp_path)
    storage.create_job(Job(node_name="A", job_id=1, params={}))
    generation, execution_id = storage.claim_job_execution(
        "A", 1, started_at="2026-01-01T00:00:00"
    )
    storage.request_job_restart("A", 1, reason="test")

    future = storage.append_job_event(
        "A", 1, "trace",
        _wait=False,
        _execution_generation=generation,
        _execution_id=execution_id,
        stale=True,
    )

    with pytest.raises(JobRestartedError):
        future.result()
    assert not any(
        event.get("stale") for event in storage.read_job_events("A", 1)
    )
