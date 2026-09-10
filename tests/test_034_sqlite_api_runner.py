from __future__ import annotations

import json
import sqlite3
import textwrap
import threading
import time

import pytest
from pathlib import Path

from micro_workflow_manager import MicroWorkflow, NodeRouter, cli
from micro_workflow_manager.cli.project import load_workflow
from micro_workflow_manager.models import DONE, now
from micro_workflow_manager.storage import FileStorage
from micro_workflow_manager.workflow.runner_config import normalize_workflow_runner
from tests.test_064_read_only_previews import (
    _live_execution_session_identity,
    _snapshot,
    _wait_writer,
)
from tests.test_090_component_session_settlement import _rows


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


def test_old_metadata_is_refused_without_moving_or_importing_payloads(tmp_path):
    (tmp_path / '.mwf').mkdir()
    (tmp_path / '.mwf' / 'project.json').write_text('{"version":4}', encoding='utf-8')
    job_dir = tmp_path / 'node' / 'A' / 'jobs' / '1'
    job_dir.mkdir(parents=True)
    (job_dir / 'job.json').write_text('{"job_id":1,"node_name":"A"}', encoding='utf-8')
    (job_dir / 'status.json').write_text('{"status":"failed"}', encoding='utf-8')
    (job_dir / 'input.json').write_text('{"value":7}', encoding='utf-8')
    (job_dir / 'output.json').write_text('{"status":"failed"}', encoding='utf-8')
    before = {path.relative_to(tmp_path): path.read_bytes() for path in tmp_path.rglob('*') if path.is_file()}
    with pytest.raises(RuntimeError, match='Unsupported MWF project format'):
        FileStorage(tmp_path)
    assert {path.relative_to(tmp_path): path.read_bytes() for path in tmp_path.rglob('*') if path.is_file()} == before
    assert not (tmp_path / '.mwf' / 'state.sqlite3').exists()


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


def test_api_runner_aliases_normalize_to_api():
    assert normalize_workflow_runner("api") == "api"
    assert normalize_workflow_runner("io") == "api"
    assert normalize_workflow_runner("network") == "api"


def test_newer_sqlite_schema_is_rejected_without_downgrade(tmp_path):
    storage = FileStorage(tmp_path)
    database = storage.state_database_path()
    storage.close_database_connections()
    connection = sqlite3.connect(database)
    try:
        connection.execute(
            "UPDATE metadata SET value='999' WHERE key='database_schema_version'"
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


def test_retired_migrate_command_preserves_old_lock_directory(tmp_path, monkeypatch, capsys):
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

    with pytest.raises(SystemExit) as error:
        cli.main(["migrate", "--dry-run"])
    assert error.value.code == 2
    assert "invalid choice: 'migrate'" in capsys.readouterr().err
    assert (locks / "legacy.lock").is_file()
    assert not (mwf_dir / "state.sqlite3").exists()


def test_paste_refuses_missing_native_snapshot_without_rebuilding_payload_jobs(tmp_path, monkeypatch):
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

    before = {path.relative_to(tmp_path / 'node' / 'A'): path.read_bytes()
              for path in (tmp_path / 'node' / 'A').rglob('*') if path.is_file()}
    assert cli.main(['paste', 'A']) == 1
    storage = FileStorage(tmp_path)
    assert not storage.job_exists('A', 7)
    assert {path.relative_to(tmp_path / 'node' / 'A'): path.read_bytes()
            for path in (tmp_path / 'node' / 'A').rglob('*') if path.is_file()} == before
    storage.close_database_connections()


def test_copy_refuses_a_live_owned_component_without_mutation(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    _write_cli_project(tmp_path, runner="direct")
    assert cli.main(["init"]) == 0
    assert cli.main(["graph", "src/graph.py", "--runner", "direct"]) == 0
    workflow = load_workflow(tmp_path, "direct")
    storage = workflow.storage
    snapshot = workflow.topology.snapshot()
    storage.register_component_topology(snapshot)
    component = ("A",)
    storage.create_execution_session(
        "clipboard-live", session_kind="main", command="run",
        start_component=component, selected_components=[component],
        expected_shape=snapshot.shape_json, **_live_execution_session_identity(),
    )
    storage.reserve_execution_components(
        "clipboard-live", expected_shape=snapshot.shape_json,
    )
    state = storage.get_component_state(component)
    storage.begin_queued_component_execution(
        "clipboard-live", component, expected_shape=snapshot.shape_json,
        expected_alignment_generation=state["alignment_generation"],
        successful_lineage=("stable", None), expected_parent_states={},
    )
    generation, execution_id = storage.claim_job_execution(
        "A", 1, started_at=now(), session_id="clipboard-live", component=component,
    )
    assert generation == 0
    assert storage.get_job_execution_owner(execution_id)["session_id"] == "clipboard-live"
    storage.db_mutation_barrier()
    _wait_writer(storage)
    before_rows = _rows(storage)
    before_files = _snapshot(tmp_path, mutable_existing_shm=True)
    capsys.readouterr()

    assert cli.main(["copy", "A"]) == 1
    assert "finish or stop first" in capsys.readouterr().err
    assert _rows(storage) == before_rows
    assert _snapshot(tmp_path, mutable_existing_shm=True) == before_files
    assert not (tmp_path / "clipboard" / "A").exists()
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
