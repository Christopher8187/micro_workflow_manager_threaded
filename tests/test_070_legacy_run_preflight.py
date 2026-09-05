from __future__ import annotations

import json
import os
import socket
import sqlite3
import subprocess
import sys
import zipfile
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path

import pytest

from micro_workflow_manager import MicroWorkflow
from micro_workflow_manager.cli.active_run import refuse_live_legacy_migration
from micro_workflow_manager.cli.layout import ensure_runtime_layout
from micro_workflow_manager.cli.migration import migrate_command
from micro_workflow_manager.cli.node_clipboard import copy_node_to_clipboard, paste_node_from_clipboard
from micro_workflow_manager.cli.project import init_project, load_workflow, setup_graph
from micro_workflow_manager.processes import process_identity
from micro_workflow_manager.runners import process as process_runner
from micro_workflow_manager.storage import FileStorage


def _snapshot(root):
    return {path.relative_to(root).as_posix(): sha256(path.read_bytes()).hexdigest()
            if path.is_file() else "directory" for path in root.rglob("*")}


def _live_run():
    return {"run_id": "observed-legacy-owner", "status": "running", "command": "runfrom",
            "pid": os.getpid(), "hostname": socket.gethostname(),
            "process_identity": process_identity(os.getpid()),
            "heartbeat_at": datetime.now(timezone.utc).isoformat()}


def _write_run(root, location, state):
    path = root / (".mwf_run.json" if location == "root" else ".mwf/run.json")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state), encoding="utf-8")
    return path


def _dangling_directory_link(link, target):
    target.mkdir()
    if os.name == "nt":
        quote = lambda path: "'" + str(path).replace("'", "''") + "'"
        subprocess.run(["powershell", "-NoProfile", "-Command",
                        f"New-Item -ItemType Junction -Path {quote(link)} -Target {quote(target)} | Out-Null"],
                       check=True, capture_output=True, text=True)
    else:
        link.symlink_to(target, target_is_directory=True)
    target.rmdir()


@pytest.mark.parametrize("constructor", [FileStorage, MicroWorkflow])
def test_complete_storage_can_reopen_while_current_legacy_run_is_live(tmp_path, constructor):
    storage = FileStorage(tmp_path)
    storage._connection_finalizer()
    path = _write_run(tmp_path, "current", _live_run())
    before = path.read_bytes()

    opened = constructor(tmp_path)
    reopened = getattr(opened, "storage", opened)

    assert reopened.database_integrity_check() == "ok"
    assert path.read_bytes() == before
    reopened._connection_finalizer()


def test_complete_private_session_store_still_reopens_with_current_run_file(tmp_path):
    storage = FileStorage._create_new_project_state(tmp_path)
    storage._connection_finalizer()
    path = _write_run(tmp_path, "current", _live_run())
    before = path.read_bytes()

    reopened = FileStorage(tmp_path)

    assert reopened.list_execution_sessions() == []
    assert path.read_bytes() == before
    reopened._connection_finalizer()


@pytest.mark.parametrize("location", ["root", "current"])
def test_one_terminal_run_retains_existing_layout_conversion(tmp_path, location):
    path = _write_run(tmp_path, location, {"run_id": "old-finished", "status": "done"})
    before = path.read_bytes()

    ensure_runtime_layout(tmp_path)

    assert (tmp_path / ".mwf/run.json").read_bytes() == before
    assert not (tmp_path / ".mwf_run.json").exists()


def test_preflight_does_not_invent_required_legacy_fields(tmp_path):
    _write_run(tmp_path, "root", {})
    before = _snapshot(tmp_path)

    refuse_live_legacy_migration(tmp_path)

    assert _snapshot(tmp_path) == before


@pytest.mark.parametrize("raw_values", [
    (b"{", b"["),
    (b"[]", b"null"),
    (b"\xff", b"{"),
    (None, b"[]"),
])
def test_preflight_reports_both_invalid_run_sources_without_changes(tmp_path, raw_values):
    paths = [tmp_path / ".mwf_run.json", tmp_path / ".mwf/run.json"]
    for path, raw in zip(paths, raw_values):
        path.parent.mkdir(parents=True, exist_ok=True)
        if raw is None:
            path.mkdir()
        else:
            path.write_bytes(raw)
    before = _snapshot(tmp_path)

    with pytest.raises(RuntimeError) as error:
        refuse_live_legacy_migration(tmp_path)

    assert _snapshot(tmp_path) == before
    assert ".mwf_run.json" in str(error.value)
    assert ".mwf/run.json" in str(error.value).replace("\\", "/")


@pytest.mark.parametrize("action", ["reader", "layout", "migrate", "init"])
def test_preflight_refuses_dangling_run_link_and_checks_other_record(tmp_path, monkeypatch, action):
    root = tmp_path / "project"
    root.mkdir()
    target = tmp_path / "removed-target"
    link = root / ".mwf_run.json"
    _dangling_directory_link(link, target)
    current = _write_run(root, "current", [])
    before = _snapshot(root)
    identity = link.lstat()
    monkeypatch.chdir(root)
    invoke = {"reader": lambda: refuse_live_legacy_migration(root),
              "layout": lambda: ensure_runtime_layout(root),
              "migrate": lambda: migrate_command(root), "init": init_project}[action]

    with pytest.raises(RuntimeError) as error:
        invoke()

    assert _snapshot(root) == before
    assert link.lstat() == identity
    assert current.read_bytes() == b"[]"
    assert ".mwf_run.json" in str(error.value)
    assert ".mwf/run.json" in str(error.value).replace("\\", "/")


@pytest.mark.parametrize("action", ["layout", "migrate", "init"])
@pytest.mark.parametrize("pair", ["distinct-terminal", "identical", "current-live", "both-live", "same-id-conflict"])
def test_layout_changes_preserve_both_valid_raw_run_records(tmp_path, monkeypatch, action, pair):
    first = {"run_id": "first-owner", "status": "done"}
    second = {"run_id": "second-owner", "status": "done"}
    if pair == "identical":
        second = dict(first)
    elif pair == "current-live":
        second = _live_run()
    elif pair == "both-live":
        first, second = {**_live_run(), "run_id": "first-owner"}, {**_live_run(), "run_id": "second-owner"}
    elif pair == "same-id-conflict":
        second = {**_live_run(), "run_id": first["run_id"]}
    _write_run(tmp_path, "root", first)
    _write_run(tmp_path, "current", second)
    if action == "init":
        with zipfile.ZipFile(tmp_path / "deployment.zip", "w") as archive:
            archive.writestr("unpacked-sentinel.txt", "must not be extracted")
    before = _snapshot(tmp_path)
    monkeypatch.chdir(tmp_path)
    invoke = {"layout": lambda: ensure_runtime_layout(tmp_path),
              "migrate": lambda: migrate_command(tmp_path), "init": init_project}[action]

    with pytest.raises(RuntimeError) as error:
        invoke()

    assert _snapshot(tmp_path) == before
    assert ".mwf_run.json" in str(error.value)
    assert ".mwf/run.json" in str(error.value).replace("\\", "/")


@pytest.mark.parametrize("constructor", [FileStorage, MicroWorkflow])
def test_direct_storage_preserves_two_nonlive_records_without_layout_conversion(tmp_path, constructor):
    paths = [_write_run(tmp_path, location, {"run_id": location, "status": "done"})
             for location in ("root", "current")]
    before = [path.read_bytes() for path in paths]

    opened = constructor(tmp_path)
    storage = getattr(opened, "storage", opened)

    assert storage.database_integrity_check() == "ok"
    assert [path.read_bytes() for path in paths] == before
    storage._connection_finalizer()


def _graph_project(root):
    source = root / "src"
    source.mkdir()
    (source / "node_behavior").mkdir()
    (source / "graph.py").write_text(
        'from pathlib import Path\n'
        'Path(__file__).with_name("graph-imported.txt").write_text("loaded")\n'
        'EDGES = [("A", "B")]\n', encoding="utf-8",
    )
    (root / ".mwf").mkdir()
    (root / ".mwf/project.json").write_text(json.dumps({
        "version": 4, "schema_version": 2, "graph_path": "src/graph.py",
        "runner": "threaded", "edges": [["A", "B"]],
    }), encoding="utf-8")
    for name in ("A", "B"):
        (root / "node" / name).mkdir(parents=True)


def test_established_live_project_still_loads_its_workflow(tmp_path, monkeypatch):
    _graph_project(tmp_path)
    monkeypatch.chdir(tmp_path)
    storage = FileStorage(tmp_path)
    storage._connection_finalizer()
    path = _write_run(tmp_path, "current", _live_run())
    before = path.read_bytes()

    workflow = load_workflow(tmp_path)

    assert set(workflow.graph_obj.nodes) == {"A", "B"}
    assert (tmp_path / "src/graph-imported.txt").read_text() == "loaded"
    assert path.read_bytes() == before
    workflow.storage._connection_finalizer()


@pytest.mark.parametrize("constructor", [FileStorage, MicroWorkflow])
@pytest.mark.parametrize("location", ["root", "current"])
@pytest.mark.parametrize("other_host", [False, True])
def test_direct_creation_refuses_observed_live_owner_before_any_state_write(tmp_path, constructor, location, other_host):
    state = _live_run()
    if other_host:
        state["hostname"] += "-other"
    _write_run(tmp_path, location, state)
    node = tmp_path / "node/A"
    node.mkdir(parents=True)
    (node / "node_state.json").write_text('{"status":"queued"}', encoding="utf-8")
    locks = tmp_path / ".mwf_locks"
    locks.mkdir()
    (locks / "held.lock").write_bytes(b"preserve legacy lock")
    before = _snapshot(tmp_path)

    with pytest.raises(RuntimeError, match="observed-legacy-owner"):
        constructor(tmp_path)

    assert _snapshot(tmp_path) == before


@pytest.mark.parametrize("constructor", [FileStorage, MicroWorkflow])
def test_direct_creation_reports_every_observed_live_run(tmp_path, constructor):
    _write_run(tmp_path, "root", {**_live_run(), "run_id": "first-owner"})
    _write_run(tmp_path, "current", {**_live_run(), "run_id": "second-owner"})
    before = _snapshot(tmp_path)

    with pytest.raises(RuntimeError) as error:
        constructor(tmp_path)

    assert _snapshot(tmp_path) == before
    for expected in ("first-owner", "second-owner", ".mwf_run.json", ".mwf/run.json"):
        assert expected in str(error.value).replace("\\", "/")


@pytest.mark.parametrize("operation", ["load", "setup"])
def test_graph_loading_refuses_before_user_code_or_configuration_writes(tmp_path, monkeypatch, operation):
    _graph_project(tmp_path)
    _write_run(tmp_path, "current", _live_run())
    monkeypatch.chdir(tmp_path)
    before = _snapshot(tmp_path)

    with pytest.raises(RuntimeError, match="observed-legacy-owner"):
        if operation == "load":
            load_workflow(tmp_path)
        else:
            setup_graph(tmp_path, "src/graph.py")

    assert _snapshot(tmp_path) == before


@pytest.mark.parametrize("directory", [False, True])
def test_layout_inspects_invalid_current_record_without_other_legacy_entries(tmp_path, directory):
    path = tmp_path / ".mwf/run.json"
    path.parent.mkdir()
    if directory:
        path.mkdir()
    else:
        path.write_bytes(b"[]")
    before = _snapshot(tmp_path)

    with pytest.raises(RuntimeError):
        ensure_runtime_layout(tmp_path)

    assert _snapshot(tmp_path) == before


@pytest.mark.parametrize("raw", [b"[]", b"{"])
def test_existing_database_still_validates_raw_records_before_sqlite_access(tmp_path, monkeypatch, raw):
    storage = FileStorage(tmp_path)
    storage._connection_finalizer()
    path = tmp_path / ".mwf/run.json"
    path.write_bytes(raw)
    before = _snapshot(tmp_path)

    def forbidden_connection(*args, **kwargs):
        raise AssertionError("Invalid raw state must refuse before SQLite access")

    monkeypatch.setattr(sqlite3, "connect", forbidden_connection)
    with pytest.raises(RuntimeError):
        FileStorage(tmp_path)

    assert _snapshot(tmp_path) == before


@pytest.mark.parametrize("constructor", [FileStorage, MicroWorkflow])
@pytest.mark.parametrize("location", ["root", "current"])
def test_direct_creation_retains_recycled_pid_classification(tmp_path, constructor, location):
    state = {**_live_run(), "process_identity": "different-process-instance"}
    path = _write_run(tmp_path, location, state)
    before = path.read_bytes()

    opened = constructor(tmp_path)
    storage = getattr(opened, "storage", opened)

    assert storage.database_integrity_check() == "ok"
    assert path.read_bytes() == before
    storage._connection_finalizer()


@pytest.mark.parametrize("entry", ["directory", "dangling-link"])
def test_nonfile_database_entry_does_not_bypass_live_owner_refusal(tmp_path, entry):
    root = tmp_path / "project"
    root.mkdir()
    _write_run(root, "current", _live_run())
    database = root / ".mwf/state.sqlite3"
    target = tmp_path / "missing-database-target"
    if entry == "directory":
        database.mkdir()
    else:
        _dangling_directory_link(database, target)
    before = _snapshot(root)

    with pytest.raises(RuntimeError, match="observed-legacy-owner"):
        FileStorage(root)

    assert _snapshot(root) == before
    assert not target.exists()


@pytest.mark.parametrize("pair", ["terminal-root", "live-current", "nonobject-root", "nonobject-current"])
def test_mixed_valid_and_invalid_run_records_remain_unchanged(tmp_path, pair):
    current_live = json.dumps(_live_run()).encode("utf-8")
    raw_values = {
        "terminal-root": (b'{"status":"done"}', b"{"),
        "live-current": (b"{", current_live),
        "nonobject-root": (b"[]", b"{}"),
        "nonobject-current": (b"{}", b"[]"),
    }[pair]
    for location, raw in zip(("root", "current"), raw_values):
        path = _write_run(tmp_path, location, {})
        path.write_bytes(raw)
    before = _snapshot(tmp_path)

    with pytest.raises(RuntimeError, match="Cannot inspect"):
        refuse_live_legacy_migration(tmp_path)

    assert _snapshot(tmp_path) == before


@pytest.mark.parametrize("location", ["root", "current"])
def test_direct_storage_does_not_invent_legacy_record_fields(tmp_path, location):
    path = _write_run(tmp_path, location, {})
    before = path.read_bytes()

    storage = FileStorage(tmp_path)

    assert storage.database_integrity_check() == "ok"
    assert path.read_bytes() == before
    storage._connection_finalizer()


@pytest.mark.parametrize("with_database", [False, True])
@pytest.mark.parametrize("records", ["live", "two-terminal"])
def test_init_rechecks_archive_records_before_post_extraction_effects(tmp_path, monkeypatch, with_database, records):
    root = tmp_path / "project"
    root.mkdir()
    contents = {
        ".mwf/project.json": b'{"version":4,"schema_version":2,"graph_path":null,"runner":"threaded","edges":[]}',
        ".mwf/run.json": json.dumps(_live_run() if records == "live" else {"run_id": "second", "status": "done"}).encode(),
        "node/A/node_state.json": b'{"status":"queued"}',
        "node/A/output/keep.txt": b"archived node output",
    }
    if records == "two-terminal":
        contents[".mwf_run.json"] = b'{"run_id":"first","status":"done"}'
    if with_database:
        seed = tmp_path / "seed"
        storage = FileStorage(seed)
        storage._connection_finalizer()
        contents[".mwf/state.sqlite3"] = (seed / ".mwf/state.sqlite3").read_bytes()
    with zipfile.ZipFile(root / "deployment.zip", "w") as archive:
        for name, raw in contents.items():
            archive.writestr(name, raw)
    expected = _snapshot(root)
    for name, raw in contents.items():
        expected[name] = sha256(raw).hexdigest()
        for parent in Path(name).parents:
            if parent != Path("."):
                expected[parent.as_posix()] = "directory"
    monkeypatch.chdir(root)

    with pytest.raises(RuntimeError):
        init_project()

    assert _snapshot(root) == expected


def _clipboard_project(root):
    for folder, raw in (("node/A/output", b"current output"), ("clipboard/A/output", b"clipboard output")):
        destination = root / folder
        destination.mkdir(parents=True)
        (destination / "keep.txt").write_bytes(raw)


@pytest.mark.parametrize("operation", [copy_node_to_clipboard, paste_node_from_clipboard])
def test_clipboard_refuses_before_changing_existing_trees(tmp_path, operation):
    _clipboard_project(tmp_path)
    _write_run(tmp_path, "current", _live_run())
    before = _snapshot(tmp_path)

    with pytest.raises(RuntimeError, match="observed-legacy-owner"):
        operation(tmp_path, "A")

    assert _snapshot(tmp_path) == before


@pytest.mark.parametrize("operation", [copy_node_to_clipboard, paste_node_from_clipboard])
def test_established_live_project_still_allows_clipboard_operation(tmp_path, operation):
    _clipboard_project(tmp_path)
    storage = FileStorage(tmp_path)
    storage._connection_finalizer()
    path = _write_run(tmp_path, "current", _live_run())
    before = path.read_bytes()

    assert operation(tmp_path, "A") == 0

    assert path.read_bytes() == before
    expected = b"current output" if operation is copy_node_to_clipboard else b"clipboard output"
    assert (tmp_path / "node/A/output/keep.txt").read_bytes() == expected
    assert (tmp_path / "clipboard/A/output/keep.txt").read_bytes() == expected


def test_process_worker_refuses_before_graph_import(tmp_path, monkeypatch):
    _graph_project(tmp_path)
    _write_run(tmp_path, "current", _live_run())
    monkeypatch.setattr(sys, "path", list(sys.path))
    monkeypatch.setattr(process_runner, "_PROCESS_WORKFLOW", None)
    search_paths = list(sys.path)
    before = _snapshot(tmp_path)

    with pytest.raises(RuntimeError, match="observed-legacy-owner"):
        process_runner._init_process_worker(str(tmp_path), str(tmp_path / "src/graph.py"), None, "startup")

    assert _snapshot(tmp_path) == before
    assert sys.path == search_paths
    assert process_runner._PROCESS_WORKFLOW is None


def test_established_live_project_still_initializes_process_worker(tmp_path, monkeypatch):
    _graph_project(tmp_path)
    storage = FileStorage(tmp_path)
    storage._connection_finalizer()
    path = _write_run(tmp_path, "current", _live_run())
    before = path.read_bytes()
    monkeypatch.setattr(sys, "path", list(sys.path))
    monkeypatch.setattr(process_runner, "_PROCESS_WORKFLOW", None)

    process_runner._init_process_worker(str(tmp_path), str(tmp_path / "src/graph.py"), None, "startup")

    worker = process_runner._PROCESS_WORKFLOW
    assert set(worker.graph_obj.nodes) == {"A", "B"}
    assert (tmp_path / "src/graph-imported.txt").read_text() == "loaded"
    assert path.read_bytes() == before
    worker.storage._connection_finalizer()
