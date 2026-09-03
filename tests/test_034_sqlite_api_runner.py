from __future__ import annotations

import hashlib
import json
import sqlite3
import textwrap
import threading
import time

import pytest
from pathlib import Path

from micro_workflow_manager import MicroWorkflow, NodeRouter, cli
from micro_workflow_manager.models import DONE
from micro_workflow_manager.storage import FileStorage
from micro_workflow_manager.cli.migration import migrate_command
from micro_workflow_manager.workflow.runner_config import normalize_workflow_runner


def _write_cli_project(root: Path, *, runner: str = "direct", max_threads: int = 1) -> None:
    behavior = root / "src" / "node_behavior"
    behavior.mkdir(parents=True)
    (root / "src" / "graph.py").write_text("EDGES = [('A', 'B')]\n", encoding="utf-8")
    (behavior / "A.py").write_text(
        textwrap.dedent(
            f"""
            import json
            import threading
            from micro_workflow_manager import NodeRouter

            router = NodeRouter("A", runner={runner!r}, max_threads={max_threads})
            router.create_job(number=1)

            @router.task
            def run(ctx):
                names = sorted(thread.name for thread in threading.enumerate())
                ctx.write_output("thread-topology.json", json.dumps({{
                    "handler": threading.current_thread().name,
                    "all_threads": names,
                }}))
                return "ok"
            """
        ).strip(),
        encoding="utf-8",
    )
    (behavior / "B.py").write_text(
        "from micro_workflow_manager import NodeRouter\n"
        "router = NodeRouter('B')\n"
        "@router.task\n"
        "def run(ctx):\n"
        "    return None\n",
        encoding="utf-8",
    )


def test_init_creates_sqlite_state_and_job_payloads_remain_files(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    _write_cli_project(tmp_path)

    assert cli.main(["init"]) == 0
    database = tmp_path / ".mwf" / "state.sqlite3"
    assert database.is_file()
    assert not (tmp_path / ".mwf" / "locks").exists()
    connection = sqlite3.connect(database)
    try:
        assert connection.execute("PRAGMA quick_check").fetchone()[0] == "ok"
    finally:
        connection.close()

    assert cli.main(["graph", "src/graph.py", "--runner", "direct"]) == 0
    assert cli.main(["run", "A", "--runner", "direct"]) == 0

    job_dir = tmp_path / "node" / "A" / "jobs" / "1"
    assert (job_dir / "input.json").is_file()
    assert (job_dir / "output.json").is_file()
    assert (tmp_path / "node" / "A" / "output" / "thread-topology.json").is_file()
    assert sorted(path.name for path in job_dir.iterdir()) == ["input.json", "output.json"]
    for legacy_name in ("job.json", "status.json", "execution.json", "runtime.json", "events.jsonl"):
        assert not (job_dir / legacy_name).exists()
    assert not (tmp_path / "node" / "A" / "queued").exists()
    assert not (tmp_path / "node" / "A" / "idempotency").exists()
    storage = FileStorage(tmp_path)
    assert storage.get_job_status("A", 1) == DONE
    storage.close_database_connections()


def test_cli_thread_topology_is_controller_to_one_handler(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    _write_cli_project(tmp_path, runner="threaded")
    assert cli.main(["init"]) == 0
    assert cli.main(["graph", "src/graph.py", "--runner", "threaded"]) == 0
    assert cli.main(["run", "A", "--runner", "threaded"]) == 0

    topology = json.loads(
        (tmp_path / "node" / "A" / "output" / "thread-topology.json").read_text()
    )
    assert topology["handler"].startswith("mwf-handler-A-1-")
    assert any(name.startswith("mwf-job-A-") for name in topology["all_threads"])
    assert not any(name.startswith("mwf-attempt-") for name in topology["all_threads"])


def test_api_runner_fills_max_threads_for_io_jobs(tmp_path):
    workflow = MicroWorkflow(project_dir=tmp_path, runner="api")
    workflow.graph([("A", "B")])
    lock = threading.Lock()
    active = 0
    peak = 0

    @workflow.task("A", runner="api", max_threads=4)
    def a(ctx):
        nonlocal active, peak
        with lock:
            active += 1
            peak = max(peak, active)
        time.sleep(0.04)
        with lock:
            active -= 1
        return ctx.job_id

    @workflow.task("B")
    def b(ctx):
        return None

    for job_id in range(1, 13):
        workflow.start("A", job_id=job_id)
    workflow.run_node("A", ignore_readiness=True)

    assert peak == 4
    assert workflow.storage.node_job_summary("A")["counts"][DONE] == 12
    workflow.storage.close_database_connections()


def test_legacy_metadata_is_imported_once_without_moving_payloads(tmp_path):
    job_dir = tmp_path / "node" / "A" / "jobs" / "1"
    job_dir.mkdir(parents=True)
    (tmp_path / "node" / "A" / "queued").mkdir()
    (tmp_path / "node" / "A" / "queued" / "1.queued").write_text("", encoding="utf-8")
    idem_dir = tmp_path / "node" / "A" / "idempotency"
    idem_dir.mkdir()
    key = "source:1:A"
    key_hash = hashlib.sha256(key.encode()).hexdigest()
    (idem_dir / f"{key_hash}.json").write_text(
        json.dumps({"key": key, "job_id": 1}), encoding="utf-8"
    )
    (tmp_path / "node" / "A" / "node_state.json").write_text(
        json.dumps({"status": "failed"}), encoding="utf-8"
    )
    (job_dir / "job.json").write_text(
        json.dumps({"job_id": 1, "node_name": "A", "parent": None, "created_at": "old"}),
        encoding="utf-8",
    )
    (job_dir / "status.json").write_text(
        json.dumps({"status": "failed", "error": "legacy"}), encoding="utf-8"
    )
    (job_dir / "events.jsonl").write_text(
        json.dumps({"time": "old", "event": "created"}) + "\n", encoding="utf-8"
    )
    (job_dir / "input.json").write_text('{"value": 7}', encoding="utf-8")
    (job_dir / "output.json").write_text('{"status": "failed"}', encoding="utf-8")

    storage = FileStorage(tmp_path)
    assert storage.get_node_status("A") == "failed"
    assert storage.get_job_status("A", 1) == "failed"
    assert storage.read_job_status_data("A", 1)["error"] == "legacy"
    assert storage.read_job_events("A", 1)[0]["event"] == "created"
    assert storage.lookup_idempotent_job("A", key).job_id == 1
    assert (job_dir / "input.json").read_text() == '{"value": 7}'
    assert (job_dir / "output.json").read_text() == '{"status": "failed"}'
    assert not (job_dir / "job.json").exists()
    assert not (job_dir / "status.json").exists()
    assert not (tmp_path / "node" / "A" / "queued").exists()
    assert not idem_dir.exists()
    storage.close_database_connections()


def test_init_removes_old_top_level_node_icon_association(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    settings = tmp_path / ".vscode" / "settings.json"
    settings.parent.mkdir(parents=True)
    settings.write_text(
        json.dumps({"material-icon-theme.folders.associations": {"node": "flow", "custom": "tools"}}),
        encoding="utf-8",
    )
    assert cli.main(["init"]) == 0
    updated = json.loads(settings.read_text(encoding="utf-8"))
    folders = updated["material-icon-theme.folders.associations"]
    assert "node" not in folders
    assert folders["custom"] == "tools"


def test_migrate_dry_run_does_not_initialize_sqlite(tmp_path, capsys):
    mwf_dir = tmp_path / ".mwf"
    mwf_dir.mkdir()
    (mwf_dir / "project.json").write_text(
        json.dumps({"version": 3, "graph_path": None, "runner": "threaded", "edges": []}),
        encoding="utf-8",
    )

    assert migrate_command(tmp_path, dry_run=True) == 0
    assert "Would initialize SQLite state database" in capsys.readouterr().out
    assert not (mwf_dir / "state.sqlite3").exists()


def test_api_runner_aliases_normalize_to_api():
    assert normalize_workflow_runner("api") == "api"
    assert normalize_workflow_runner("io") == "api"
    assert normalize_workflow_runner("network") == "api"


def test_newer_sqlite_schema_is_rejected_without_downgrade(tmp_path):
    database = tmp_path / ".mwf" / "state.sqlite3"
    database.parent.mkdir()
    connection = sqlite3.connect(database)
    try:
        connection.execute("CREATE TABLE metadata(key TEXT PRIMARY KEY, value TEXT NOT NULL)")
        connection.execute(
            "INSERT INTO metadata(key, value) VALUES('database_schema_version', '999')"
        )
        connection.commit()
    finally:
        connection.close()

    with pytest.raises(RuntimeError, match="newer MWF schema"):
        FileStorage(tmp_path)

    connection = sqlite3.connect(database)
    try:
        value = connection.execute(
            "SELECT value FROM metadata WHERE key='database_schema_version'"
        ).fetchone()[0]
    finally:
        connection.close()
    assert value == "999"


def test_doctor_reports_orphan_job_payload_folder(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    _write_cli_project(tmp_path)
    assert cli.main(["init"]) == 0
    assert cli.main(["graph", "src/graph.py", "--runner", "direct"]) == 0
    orphan = tmp_path / "node" / "A" / "jobs" / "99"
    orphan.mkdir(parents=True)
    (orphan / "input.json").write_text("{}", encoding="utf-8")
    capsys.readouterr()

    assert cli.main(["doctor"]) == 1
    assert "job payload folders without SQLite rows in A: 99" in capsys.readouterr().out


def test_cli_migrate_dry_run_preserves_legacy_lock_directory(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    mwf_dir = tmp_path / ".mwf"
    mwf_dir.mkdir()
    (mwf_dir / "project.json").write_text(
        json.dumps({"version": 3, "graph_path": None, "runner": "threaded", "edges": []}),
        encoding="utf-8",
    )
    locks = mwf_dir / "locks"
    locks.mkdir()
    (locks / "legacy.lock").write_text("0", encoding="utf-8")

    assert cli.main(["migrate", "--dry-run"]) == 0
    capsys.readouterr()
    assert (locks / "legacy.lock").is_file()
    assert not (mwf_dir / "state.sqlite3").exists()


def test_paste_rebuilds_legacy_payload_jobs_and_runs_immediately(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    _write_cli_project(tmp_path, runner="direct")
    assert cli.main(["init"]) == 0
    assert cli.main(["graph", "src/graph.py", "--runner", "direct"]) == 0

    clipboard = tmp_path / "clipboard" / "A"
    job = clipboard / "jobs" / "7"
    job.mkdir(parents=True)
    (job / "input.json").write_text("{}", encoding="utf-8")
    (clipboard / "input").mkdir()
    (clipboard / "output").mkdir()
    (clipboard / "schema.json").write_text(
        (tmp_path / "node" / "A" / "schema.json").read_text(encoding="utf-8"),
        encoding="utf-8",
    )

    assert cli.main(["paste", "A"]) == 0
    storage = FileStorage(tmp_path)
    assert storage.get_job_status("A", 7) == "queued"
    assert storage.get_node_status("A") == "queued"
    storage.close_database_connections()
    assert cli.main(["run", "A", "job", "7", "--runner", "direct"]) == 0
    storage = FileStorage(tmp_path)
    assert storage.get_job_status("A", 7) == DONE
    storage.close_database_connections()


def test_paste_requeues_running_snapshot_immediately(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    _write_cli_project(tmp_path, runner="direct")
    assert cli.main(["init"]) == 0
    assert cli.main(["graph", "src/graph.py", "--runner", "direct"]) == 0
    storage = FileStorage(tmp_path)
    storage.set_job_status("A", 1, "running")
    storage.close_database_connections()
    assert cli.main(["copy", "A"]) == 0
    assert cli.main(["paste", "A"]) == 0
    storage = FileStorage(tmp_path)
    assert storage.get_job_status("A", 1) == "queued"
    assert storage.get_node_status("A") == "queued"
    storage.close_database_connections()


def test_threads_update_refreshes_schema_from_node_behavior(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    _write_cli_project(tmp_path, runner="api", max_threads=4)
    assert cli.main(["init"]) == 0
    assert cli.main(["graph", "src/graph.py", "--runner", "threaded"]) == 0
    behavior = tmp_path / "src" / "node_behavior" / "A.py"
    behavior.write_text(behavior.read_text(encoding="utf-8").replace("max_threads=4", "max_threads=11"), encoding="utf-8")
    assert json.loads((tmp_path / "node" / "A" / "schema.json").read_text())["max_threads"] == 4
    assert cli.main(["threads", "--update"]) == 0
    schema = json.loads((tmp_path / "node" / "A" / "schema.json").read_text())
    assert schema["max_threads"] == 11
    assert schema["runner_override"] == "api"
    assert "A: 4 -> 11" in capsys.readouterr().out
