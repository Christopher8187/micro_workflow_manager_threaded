from __future__ import annotations

import os
import textwrap
import time
from pathlib import Path

from micro_workflow_manager import cli
from micro_workflow_manager.models import Job
from micro_workflow_manager.storage import FileStorage
from micro_workflow_manager.storage.sqlite_state import SQLiteStateMixin
from micro_workflow_manager.system import MicroWorkflow


def _connection_count(storage: FileStorage) -> int:
    path = storage.state_database_path().resolve()
    return sum(
        1
        for key in SQLiteStateMixin._connection_registry
        if key[0] == path and key[1] == os.getpid()
    )


def test_checkpoint_runtime_is_one_database_write_without_advisory_lock(tmp_path, monkeypatch):
    storage = FileStorage(tmp_path)
    storage.create_job(Job(job_id=1, node_name="merge", params={}))

    calls = 0
    original = storage.db_transaction

    def counted_transaction(*args, **kwargs):
        nonlocal calls
        calls += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(storage, "db_transaction", counted_transaction)
    monkeypatch.setattr(
        storage,
        "interprocess_lock",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("checkpoint persistence must not use advisory locks")
        ),
    )

    running = {"watch_id": "watch-1", "state": "running", "checkpoint_name": "load"}
    storage.write_job_runtime("merge", 1, running)
    assert calls == 1
    assert storage.read_job_runtime("merge", 1)["checkpoint_name"] == "load"

    storage.write_job_runtime(
        "merge",
        1,
        {"watch_id": "watch-1", "state": "timed_out", "checkpoint_name": "load"},
    )
    storage.write_job_runtime(
        "merge",
        1,
        {"watch_id": "watch-1", "state": "running", "checkpoint_name": "late"},
    )
    assert storage.read_job_runtime("merge", 1)["state"] == "timed_out"


def test_async_runtime_completion_can_follow_terminal_publication(tmp_path):
    storage = FileStorage(tmp_path)
    storage.create_job(Job(job_id=1, node_name="merge", params={}))
    generation, execution_id = storage.claim_job_execution(
        "merge", 1, started_at="2026-07-21T12:00:00"
    )

    storage.write_job_runtime(
        "merge",
        1,
        {
            "watch_id": "watch-1",
            "state": "running",
            "generation": generation,
            "execution_id": execution_id,
        },
        wait=False,
        priority=20,
    )
    storage.finalize_job_execution(
        "merge",
        1,
        generation,
        execution_id,
        "done",
        generation=generation,
        execution_id=execution_id,
    )
    storage.write_job_runtime(
        "merge",
        1,
        {
            "watch_id": "watch-1",
            "state": "completed",
            "generation": generation,
            "execution_id": execution_id,
        },
        wait=False,
        priority=20,
    )
    storage.db_mutation_barrier()

    assert storage.get_job_status("merge", 1) == "done"
    assert storage.read_job_runtime("merge", 1)["state"] == "completed"

    storage.write_job_runtime(
        "merge",
        1,
        {
            "watch_id": "watch-1",
            "state": "running",
            "generation": generation,
            "execution_id": execution_id,
        },
    )
    assert storage.read_job_runtime("merge", 1)["state"] == "completed"


def test_pending_async_checkpoints_coalesce_to_latest_observation(tmp_path, monkeypatch):
    storage = FileStorage(tmp_path)
    storage.create_job(Job(job_id=1, node_name="merge", params={}))
    generation, execution_id = storage.claim_job_execution(
        "merge", 1, started_at="2026-07-21T12:00:00"
    )
    queued = []
    original = storage.submit_grouped_db_mutation

    def capture(group_key, item, operation, **options):
        if group_key == ("runtime", "merge") and options.get("wait") is False:
            from concurrent.futures import Future
            future = Future()
            queued.append((item, operation, future))
            return future
        return original(group_key, item, operation, **options)

    monkeypatch.setattr(storage, "submit_grouped_db_mutation", capture)
    common = {
        "watch_id": "watch-1",
        "state": "running",
        "generation": generation,
        "execution_id": execution_id,
    }
    first = storage.write_job_runtime(
        "merge", 1, {**common, "checkpoint_name": "load"},
        wait=False, priority=20,
    )
    second = storage.write_job_runtime(
        "merge", 1, {**common, "checkpoint_name": "write"},
        wait=False, priority=20,
    )

    assert first is second
    assert len(queued) == 1
    slot, operation, future = queued[0]
    with storage.db_transaction() as connection:
        outcomes = operation(connection, [slot])
    future.set_result(outcomes[0][1])
    assert storage.read_job_runtime("merge", 1)["checkpoint_name"] == "write"

def test_execution_fence_uses_filesystem_lock_not_sqlite_advisory_rows(tmp_path, monkeypatch):
    storage = FileStorage(tmp_path)
    storage.create_job(Job(job_id=1, node_name="merge", params={}))

    monkeypatch.setattr(
        storage,
        "interprocess_lock",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("restart-fenced file writes must bypass SQLite advisory locks")
        ),
    )
    generation, execution_id = storage.claim_job_execution(
        "merge", 1, started_at="2026-07-19T12:00:00"
    )
    target = tmp_path / "node" / "merge" / "jobs" / "1" / "files" / "command.txt"
    storage.run_guarded_job_side_effect(
        "merge",
        1,
        generation,
        execution_id,
        lambda: storage.atomic_write_text(target, "ok"),
    )
    assert target.read_text(encoding="utf-8") == "ok"
    assert storage.db_connection().execute(
        "SELECT COUNT(*) FROM advisory_locks WHERE name LIKE 'job-merge-1-%'"
    ).fetchone()[0] == 0


def test_repeated_api_rounds_release_worker_connections_and_do_not_slow_progressively(tmp_path):
    workflow = MicroWorkflow(project_dir=tmp_path, runner="api")
    workflow.graph([("merge", "sink")])

    @workflow.task(
        "merge",
        runner="api",
        max_threads=96,
        timeout=30,
        checkpoint_timeout=10,
    )
    def merge(ctx, round_number):
        ctx.checkpoint("loading")
        ctx.checkpoint("preparing")
        ctx.write("command.txt", f"{round_number}:{ctx.job_id}")
        ctx.checkpoint("writing")
        return ctx.job_id

    @workflow.task("sink")
    def sink(ctx):
        return None

    workflow.active_job_restart_enabled = True
    durations = []
    for round_number in (1, 2, 3):
        first = (round_number - 1) * 96 + 1
        for job_id in range(first, first + 96):
            workflow.start(
                "merge",
                job_id=job_id,
                autostart=False,
                round_number=round_number,
            )
        started = time.perf_counter()
        workflow.run_node("merge", ignore_readiness=True)
        durations.append(time.perf_counter() - started)
        workflow.storage.prune_dead_thread_connections()
        assert _connection_count(workflow.storage) <= 1
        assert workflow.storage.db_connection().execute("PRAGMA quick_check").fetchone()[0] == "ok"

    assert max(durations[1:]) <= durations[0] * 3 + 1.0


def test_cli_repeated_merge_runs_with_threads_override_and_monitor(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    behavior = tmp_path / "src" / "node_behavior"
    behavior.mkdir(parents=True)
    (tmp_path / "src" / "graph.py").write_text("EDGES = [('merge', 'sink')]\n", encoding="utf-8")
    (behavior / "merge.py").write_text(
        textwrap.dedent(
            """
            from micro_workflow_manager import NodeRouter

            router = NodeRouter("merge", runner="api", max_threads=16)
            router.create_job(number=48)

            @router.task(timeout=30, checkpoint_timeout=10)
            def run(ctx):
                ctx.checkpoint("merge: loading section")
                ctx.checkpoint("merge: preparing model decision")
                ctx.write("command.txt", str(ctx.job_id))
                ctx.checkpoint("merge: writing command")
                return ctx.job_id
            """
        ).strip()
        + "\n",
        encoding="utf-8",
    )
    (behavior / "sink.py").write_text(
        "from micro_workflow_manager import NodeRouter\n"
        "router = NodeRouter('sink')\n"
        "@router.task\n"
        "def run(ctx): return None\n",
        encoding="utf-8",
    )

    assert cli.main(["init"]) == 0
    assert cli.main(["graph", "src/graph.py", "--runner", "api"]) == 0
    assert cli.main(["threads", "merge", "64"]) == 0
    capsys.readouterr()

    for _ in range(2):
        assert cli.main(
            ["run", "merge", "--monitor", "--monitor-interval", "0.01"]
        ) == 0
        captured = capsys.readouterr()
        assert "last run: run merge | status=done" in captured.err
        assert "database is locked" not in captured.err
        debug_path = tmp_path / "node" / "merge" / "debug.txt"
        if debug_path.exists():
            assert "database is locked" not in debug_path.read_text(encoding="utf-8")

    storage = FileStorage(tmp_path)
    assert storage.node_job_summary("merge")["counts"]["done"] == 48
    storage.prune_dead_thread_connections()
    assert _connection_count(storage) <= 1
