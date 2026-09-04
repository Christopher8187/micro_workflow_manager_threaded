from __future__ import annotations

import json
import os
import socket
import zipfile
from datetime import datetime, timezone
from hashlib import sha256

import pytest

from micro_workflow_manager import cli
from micro_workflow_manager.cli.migration import migrate_command
from micro_workflow_manager.processes import process_identity


def _snapshot(root):
    return {
        path.relative_to(root): sha256(path.read_bytes()).hexdigest()
        if path.is_file() else "directory"
        for path in root.rglob("*")
    }


def _project_with_live_run(tmp_path, layout="current", other_host=False, legacy_locks=True):
    metadata = tmp_path / ".mwf"
    if layout == "current":
        metadata.mkdir()
    config = metadata / "project.json" if layout == "current" else metadata
    config.write_text(json.dumps({
        "version": 4, "edges": [["A", "B"]], "graph_path": "src/graph.py",
    }), encoding="utf-8")
    run = metadata / "run.json" if layout == "current" else tmp_path / ".mwf_run.json"
    run.write_text(json.dumps({
        "run_id": "legacy-main", "command": "runfrom", "status": "running",
        "pid": os.getpid(), "hostname": socket.gethostname() + "-other" if other_host else socket.gethostname(),
        "process_identity": process_identity(os.getpid()),
        "heartbeat_at": datetime.now(timezone.utc).isoformat(),
    }), encoding="utf-8")
    locks = metadata / "locks" if layout == "current" else tmp_path / ".mwf_locks"
    if legacy_locks:
        locks.mkdir()
        (locks / "held.lock").write_bytes(b"active legacy lock")
    behavior = tmp_path / "src" / "node_behavior"
    behavior.mkdir(parents=True)
    (tmp_path / "src" / "graph.py").write_text("EDGES = [('A', 'B')]\n", encoding="utf-8")
    for node in ("A", "B"):
        (tmp_path / "node" / node).mkdir(parents=True)
        (behavior / f"{node}.py").write_text(
            f'from micro_workflow_manager import NodeRouter\nrouter = NodeRouter("{node}")\n'
            '@router.task\ndef run(ctx):\n    return "done"\n', encoding="utf-8",
        )


@pytest.mark.parametrize("layout", ["current", "legacy"])
@pytest.mark.parametrize("other_host", [False, True])
def test_applied_migration_refuses_live_legacy_run_before_any_mutation(tmp_path, monkeypatch, capsys, layout, other_host):
    monkeypatch.chdir(tmp_path)
    _project_with_live_run(tmp_path, layout, other_host)
    before = _snapshot(tmp_path)

    assert cli.main(["migrate"]) == 1

    assert _snapshot(tmp_path) == before
    error = capsys.readouterr().err
    assert "legacy-main" in error
    assert "migration" in error.lower()


def test_applied_migration_checks_both_legacy_run_locations(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    _project_with_live_run(tmp_path)
    (tmp_path / ".mwf_run.json").write_text(
        json.dumps({"run_id": "older-finished", "status": "done"}), encoding="utf-8",
    )
    before = _snapshot(tmp_path)

    assert cli.main(["migrate"]) == 1

    assert _snapshot(tmp_path) == before
    assert "legacy-main" in capsys.readouterr().err


def test_direct_applied_migration_also_preserves_live_legacy_project(tmp_path):
    _project_with_live_run(tmp_path, "legacy")
    before = _snapshot(tmp_path)

    with pytest.raises(RuntimeError, match="legacy-main"):
        migrate_command(tmp_path)

    assert _snapshot(tmp_path) == before


@pytest.mark.parametrize("direct", [False, True])
def test_applied_migration_checks_live_run_without_legacy_layout_artifacts(tmp_path, monkeypatch, capsys, direct):
    monkeypatch.chdir(tmp_path)
    _project_with_live_run(tmp_path, legacy_locks=False)
    before = _snapshot(tmp_path)

    if direct:
        with pytest.raises(RuntimeError, match="legacy-main"):
            migrate_command(tmp_path)
    else:
        assert cli.main(["migrate"]) == 1
        assert "legacy-main" in capsys.readouterr().err

    assert _snapshot(tmp_path) == before


@pytest.mark.parametrize("status", ["done", "running"])
def test_finished_or_recycled_legacy_owner_does_not_prevent_migration(tmp_path, monkeypatch, capsys, status):
    monkeypatch.chdir(tmp_path)
    _project_with_live_run(tmp_path, "legacy")
    run_path = tmp_path / ".mwf_run.json"
    state = json.loads(run_path.read_text(encoding="utf-8"))
    state.update(status=status, process_identity="a-different-process-instance")
    run_path.write_text(json.dumps(state), encoding="utf-8")
    payload = tmp_path / "node" / "A" / "output" / "keep.txt"
    payload.parent.mkdir()
    payload.write_bytes(b"retained output")

    assert cli.main(["migrate"]) == 0

    assert (tmp_path / ".mwf" / "project.json").is_file()
    assert not run_path.exists()
    assert json.loads((tmp_path / ".mwf" / "run.json").read_text(encoding="utf-8"))["run_id"] == "legacy-main"
    assert payload.read_bytes() == b"retained output"
    assert "Migrated" in capsys.readouterr().out


def test_automatic_legacy_layout_migration_refuses_live_run_before_any_mutation(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    _project_with_live_run(tmp_path, "legacy")
    before = _snapshot(tmp_path)

    assert cli.main(["run", "A"]) == 1

    assert _snapshot(tmp_path) == before
    error = capsys.readouterr().err
    assert "legacy-main" in error
    assert "migration" in error.lower()


def test_init_refuses_live_run_before_extracting_deployment(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    _project_with_live_run(tmp_path, "legacy")
    with zipfile.ZipFile(tmp_path / "deployment.zip", "w") as archive:
        archive.writestr("archive-created.txt", "deployment contents")
    before = _snapshot(tmp_path)

    assert cli.main(["init"]) == 1

    assert _snapshot(tmp_path) == before
    assert "legacy-main" in capsys.readouterr().err


@pytest.mark.parametrize("command", [["migrate"], ["run", "A"]])
def test_non_object_run_state_refuses_migration_before_any_mutation(tmp_path, monkeypatch, capsys, command):
    monkeypatch.chdir(tmp_path)
    _project_with_live_run(tmp_path, "legacy")
    (tmp_path / ".mwf_run.json").write_text("[]", encoding="utf-8")
    before = _snapshot(tmp_path)

    assert cli.main(command) == 1

    assert _snapshot(tmp_path) == before
    assert "JSON object" in capsys.readouterr().err
