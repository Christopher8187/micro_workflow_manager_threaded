from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

from micro_workflow_manager import cli
from micro_workflow_manager.cli.deploy import deploy_setup
from micro_workflow_manager.cli.project import load_workflow
from micro_workflow_manager.models import now
from micro_workflow_manager.storage import FileStorage
from tests.test_064_read_only_previews import (
    _live_execution_session_identity,
    _mark_execution_session_stale,
)


def _project(tmp_path: Path, monkeypatch, *, failing: bool = False):
    monkeypatch.chdir(tmp_path)
    behavior = tmp_path / "src" / "node_behavior"
    behavior.mkdir(parents=True)
    (tmp_path / "src" / "graph.py").write_text("EDGES = [('A', 'B')]\n", encoding="utf-8")
    body = "raise RuntimeError('boom')" if failing else "return ctx.job_id"
    (behavior / "A.py").write_text(
        f'''from micro_workflow_manager import NodeRouter
router = NodeRouter("A", max_threads=2)
router.create_job(number=1)
@router.task
def run(ctx):
    {body}
''',
        encoding="utf-8",
    )
    (behavior / "B.py").write_text(
        '''from micro_workflow_manager import NodeRouter
router = NodeRouter("B")
@router.task
def run(ctx):
    return None
''',
        encoding="utf-8",
    )
    assert cli.main(["init"]) == 0
    assert cli.main(["graph", "src/graph.py", "--runner", "threaded"]) == 0


def test_thread_override_is_consumed_by_one_run_and_removed(tmp_path, monkeypatch, capsys):
    _project(tmp_path, monkeypatch)
    capsys.readouterr()

    assert cli.main(["threads", "A", "5"]) == 0
    storage = FileStorage(tmp_path)
    assert storage.read_thread_override_observation("A") == {
        "node": "A", "value": 5, "session_id": None,
    }
    assert [tuple(row) for row in storage.db_connection().execute(
        "SELECT node_name, value FROM pending_node_thread_overrides ORDER BY node_name"
    )] == [("A", 5)]
    assert not (tmp_path / ".mwf" / "threads.json").exists()

    assert cli.main(["run", "A", "--runner", "threaded"]) == 0
    capsys.readouterr()
    assert storage.read_thread_override_observation("A") == {
        "node": "A", "value": None, "session_id": None,
    }
    assert storage.db_connection().execute(
        "SELECT 1 FROM pending_node_thread_overrides LIMIT 1"
    ).fetchone() is None
    assert storage.db_connection().execute(
        "SELECT 1 FROM node_thread_overrides LIMIT 1"
    ).fetchone() is None
    assert not (tmp_path / ".mwf" / "threads.json").exists()

    assert cli.main(["threads", "A"]) == 0
    output = capsys.readouterr().out
    assert "runtime override: (none)" in output
    assert "requested max_threads: 2" in output
    storage.close_database_connections()


def test_stale_session_owned_thread_override_is_recovered_and_cleared_by_next_run(
    tmp_path, monkeypatch, capsys,
):
    _project(tmp_path, monkeypatch)
    capsys.readouterr()
    workflow = load_workflow(tmp_path, require_synced=True)
    storage = workflow.storage
    snapshot = workflow.topology.snapshot()
    storage.register_component_topology(snapshot)
    expected = storage.read_thread_override_observation("A")
    storage.set_thread_override("A", 7, expected=expected)
    session_id = "stale-thread-" + uuid4().hex
    storage.create_execution_session(
        session_id,
        session_kind="main",
        command="run",
        start_component=("A",),
        selected_components=[("A",)],
        expected_shape=snapshot.shape_json,
        **_live_execution_session_identity(),
    )
    storage.reserve_execution_components(
        session_id, expected_shape=snapshot.shape_json,
    )
    storage.begin_queued_component_execution(
        session_id,
        ("A",),
        expected_shape=snapshot.shape_json,
        expected_alignment_generation=0,
        successful_lineage=("stable", None),
    )
    generation, execution_id = storage.claim_job_execution(
        "A", 1, started_at=now(), session_id=session_id, component=("A",),
    )
    assert storage.read_thread_override_observation("A") == {
        "node": "A", "value": 7, "session_id": session_id,
    }
    _mark_execution_session_stale(storage, session_id)
    assert [tuple(row) for row in storage.db_connection().execute(
        "SELECT node_name, session_id, value FROM node_thread_overrides "
        "ORDER BY node_name, session_id"
    )] == [("A", session_id, 7)]

    assert cli.main(["run", "A", "--runner", "threaded"]) == 0
    capsys.readouterr()
    assert storage.get_execution_session(session_id)["status"] == "terminal"
    assert storage.get_job_execution_owner(execution_id)["generation"] == generation
    assert storage.read_thread_override_observation("A") == {
        "node": "A", "value": None, "session_id": None,
    }
    assert storage.db_connection().execute(
        "SELECT 1 FROM node_thread_overrides LIMIT 1"
    ).fetchone() is None
    assert not (tmp_path / ".mwf" / "threads.json").exists()
    storage.close_database_connections()


def test_restart_refuses_failed_job_without_active_run(tmp_path, monkeypatch, capsys):
    _project(tmp_path, monkeypatch, failing=True)
    capsys.readouterr()
    assert cli.main(["run", "A", "--runner", "direct"]) == 1
    capsys.readouterr()

    storage = FileStorage(tmp_path)
    assert storage.get_job_status("A", 1) == "failed"
    assert cli.main(["restart", "A", "job", "1"]) == 1
    error = capsys.readouterr().err
    assert "cannot accept restart: no running sequence is recorded" in error
    assert storage.get_job_status("A", 1) == "failed"


def test_restart_refuses_done_job(tmp_path, monkeypatch, capsys):
    _project(tmp_path, monkeypatch)
    capsys.readouterr()
    assert cli.main(["run", "A", "--runner", "direct"]) == 0
    capsys.readouterr()
    assert cli.main(["restart", "A", "job", "1"]) == 1
    assert "cannot accept restart: no running sequence is recorded" in capsys.readouterr().err


def test_deploy_setup_prompts_for_nonstandard_port(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    assert cli.main(["init"]) == 0
    capsys.readouterr()
    replies = iter(["server.example", "chris", "22022", "key", ""])
    monkeypatch.setattr("builtins.input", lambda _prompt: next(replies))
    args = SimpleNamespace(
        host=None,
        user=None,
        port=None,
        auth=None,
        tool="openssh",
        key=None,
        pscp=None,
        plink=None,
        python_command=None,
    )
    assert deploy_setup(tmp_path, args) == 0
    config = json.loads((tmp_path / ".mwf" / "deploy" / "server.json").read_text(encoding="utf-8"))
    assert config["port"] == 22022
