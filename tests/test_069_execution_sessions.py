from __future__ import annotations

import sqlite3
import os
import socket
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import networkx as nx
import pytest

from micro_workflow_manager.storage import FileStorage
from micro_workflow_manager.models import Job
from micro_workflow_manager.processes import process_identity
from micro_workflow_manager.topology import ComponentTopology


def _single_snapshot():
    graph = nx.DiGraph()
    graph.add_node('A')
    return ComponentTopology(graph, []).snapshot()


def _session(
    storage, session_id="main-1", *, kind="main", register_topology=True, **overrides,
):
    snapshot = _single_snapshot()
    if register_topology:
        storage.register_component_topology(snapshot)
    fields = {
        "session_kind": kind,
        "command": "run" if kind == "main" else "interrupt",
        "start_component": ("A",),
        "selected_components": [("A",)],
        "started_at": "2026-09-05T12:00:00+00:00",
        "hostname": "worker.example",
        "pid": 123,
        "process_identity": "process-instance-1",
        "expected_shape": snapshot.shape_json,
    }
    fields.update(overrides)
    return storage.create_execution_session(session_id, **fields)


def test_ordinary_reopen_keeps_native_schema_and_payloads(tmp_path):
    storage = FileStorage._create_new_project_state(tmp_path)
    storage.create_job(Job(node_name='A', job_id=1, params={'original': True}))
    input_path = storage.input_file('A', 1)
    input_bytes = input_path.read_bytes()
    instance = storage.read_job_instance_id('A', 1)
    storage.close_database_connections()

    reopened = FileStorage(tmp_path)
    assert reopened.load_job('A', 1).params == {'original': True}
    assert input_path.read_bytes() == input_bytes
    assert reopened.read_job_instance_id('A', 1) == instance
    with sqlite3.connect(tmp_path / '.mwf' / 'state.sqlite3') as connection:
        assert connection.execute(
            "SELECT value FROM metadata WHERE key='database_schema_version'"
        ).fetchone() == ('6',)
        assert connection.execute('SELECT COUNT(*) FROM execution_sessions').fetchone() == (0,)
    reopened.close_database_connections()


def test_terminal_session_keeps_exact_outcome_when_old_heartbeat_or_finish_arrives(tmp_path):
    storage = FileStorage._create_new_project_state(tmp_path)
    _session(storage)
    _session(storage, "child-1", kind="interrupt", parent_session_id="main-1")
    assert storage.heartbeat_execution_session("main-1", "2026-09-05T12:01:00+00:00")
    assert storage.get_execution_session("main-1")["heartbeat_at"] == "2026-09-05T12:01:00+00:00"
    failures = [{"node": "A", "job_id": 3, "error": "lost input"}, {"node": "A", "error": "cleanup"}]
    assert storage.finish_execution_session(
        "main-1", outcome="failed", finished_at="2026-09-05T12:02:00+00:00", failures=failures,
    )
    assert not storage.heartbeat_execution_session("main-1", "2026-09-05T12:03:00+00:00")
    assert not storage.finish_execution_session(
        "main-1", outcome="done", finished_at="2026-09-05T12:04:00+00:00",
    )
    assert not storage.heartbeat_execution_session("missing", "2026-09-05T12:03:00+00:00")
    storage.close_database_connections()

    reopened = FileStorage(tmp_path)
    main = reopened.get_execution_session("main-1")
    assert main["status"] == "terminal"
    assert main["outcome"] == "failed"
    assert main["failures"] == failures
    assert main["heartbeat_at"] == "2026-09-05T12:01:00+00:00"
    assert main["finished_at"] == "2026-09-05T12:02:00+00:00"
    child = reopened.get_execution_session("child-1")
    assert child["status"] == "running"
    assert child["parent_session_id"] == "main-1"
    assert child["heartbeat_at"] == "2026-09-05T12:00:00+00:00"
    assert not (tmp_path / ".mwf" / "run.json").exists()
    assert not (tmp_path / ".mwf_run.json").exists()
    reopened.close_database_connections()


def test_fresh_store_retains_exact_main_and_interrupt_sessions_after_reopen(tmp_path):
    storage = FileStorage._create_new_project_state(tmp_path)
    snapshot = ComponentTopology(nx.DiGraph([('A', 'B'), ('B', 'A'), ('B', 'C')]), []).snapshot()
    storage.register_component_topology(snapshot)
    for node, job_id in [("B", 9), ("A", 3)]:
        storage.create_job(Job(node_name=node, job_id=job_id, params={}))
    records = {}
    for session_id, kind, parent in (
        ("main-1", "main", None),
        ("interrupt-1", "interrupt", "main-1"),
        ("interrupt-2", "interrupt", None),
    ):
        records[session_id] = storage.create_execution_session(
            session_id,
            session_kind=kind,
            command="run" if kind == "main" else "interrupt",
            start_component=("A", "B"),
            selected_components=[("A", "B"), ("C",)],
            selected_jobs=[("B", 9), ("A", 3)],
            parent_session_id=parent,
            started_at="2026-09-05T12:00:00+00:00",
            hostname="worker.example",
            pid=123,
            process_identity="process-instance-1",
            details={"boundary": ["C"], "settings": {"sample": 0.5}},
            expected_shape=snapshot.shape_json,
        )
    storage.close_database_connections()

    reopened = FileStorage(tmp_path)
    assert reopened.get_execution_session("missing") is None
    for session_id, kind, parent in (
        ("main-1", "main", None),
        ("interrupt-1", "interrupt", "main-1"),
        ("interrupt-2", "interrupt", None),
    ):
        expected = {
            "session_id": session_id,
            "session_kind": kind,
            "parent_session_id": parent,
            "command": "run" if kind == "main" else "interrupt",
            "start_component": ("A", "B"),
            "selected_components": [("A", "B"), ("C",)],
            "selected_jobs": [("B", 9), ("A", 3)],
            "selection_kind": "jobs",
            "status": "running",
            "started_at": "2026-09-05T12:00:00+00:00",
            "heartbeat_at": "2026-09-05T12:00:00+00:00",
            "finished_at": None,
            "hostname": "worker.example",
            "pid": 123,
            "process_identity": "process-instance-1",
            "outcome": None,
            "failures": [],
            "details": {"boundary": ["C"], "settings": {"sample": 0.5}},
            "admitted_shape_id": 1,
            "partition_revision": 0,
        }
        assert records[session_id] == expected
        assert reopened.get_execution_session(session_id) == expected
    assert not (tmp_path / ".mwf" / "run.json").exists()
    assert reopened.database_integrity_check() == "ok"
    reopened.close_database_connections()


def test_separate_processes_cannot_create_two_running_main_sessions(tmp_path):
    storage = FileStorage._create_new_project_state(tmp_path)
    snapshot = _single_snapshot()
    storage.register_component_topology(snapshot)
    storage.create_job(Job(node_name="A", job_id=1, params={}))
    script = """
import sqlite3, sys, time
from pathlib import Path
from micro_workflow_manager.storage import FileStorage
root, session_id = Path(sys.argv[1]), sys.argv[2]
storage = FileStorage(root)
print('ready', flush=True)
deadline = time.monotonic() + 15
while not (root / 'start').exists():
    if time.monotonic() > deadline:
        raise RuntimeError('parent did not release creation')
    time.sleep(0.001)
try:
    storage.create_execution_session(
        session_id, session_kind='main', command='run', start_component=('A',),
        selected_components=[('A',)], selected_jobs=[('A', 1)],
        started_at='2026-09-05T12:00:00+00:00', hostname='worker.example',
        pid=123, process_identity='old-process', expected_shape=sys.argv[3],
    )
except sqlite3.IntegrityError:
    print('refused', flush=True)
else:
    print('created', flush=True)
finally:
    storage.close_database_connections()
"""
    processes = [subprocess.Popen(
        [sys.executable, "-c", script, str(tmp_path), session_id, snapshot.shape_json],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    ) for session_id in ("main-1", "main-2")]
    try:
        for process in processes:
            assert process.stdout.readline().strip() == "ready"
        (tmp_path / "start").touch()
        outcomes = []
        for process in processes:
            stdout, stderr = process.communicate(timeout=20)
            assert process.returncode == 0, stderr
            outcomes.append(stdout.strip())
        assert sorted(outcomes) == ["created", "refused"]
    finally:
        for process in processes:
            if process.poll() is None:
                process.kill()
                process.communicate(timeout=5)

    sessions = [storage.get_execution_session(name) for name in ("main-1", "main-2")]
    winner = next(session for session in sessions if session is not None)
    assert sum(session is not None for session in sessions) == 1
    # A stale persisted running row still occupies the main slot until recovery
    # records its terminal outcome. Creation must never silently replace it.
    with pytest.raises(sqlite3.IntegrityError):
        _session(storage, "main-3")
    _session(storage, "interrupt-1", kind="interrupt")
    _session(storage, "interrupt-2", kind="interrupt")
    assert storage.finish_execution_session(
        winner["session_id"], outcome="abandoned", finished_at="2026-09-05T12:05:00+00:00",
    )
    _session(storage, "main-3")
    assert storage.get_execution_session("main-3")["status"] == "running"
    with sqlite3.connect(tmp_path / ".mwf" / "state.sqlite3") as connection:
        assert connection.execute("SELECT COUNT(*) FROM session_jobs").fetchone() == (1,)
    storage.close_database_connections()


@pytest.mark.parametrize("relative", [
    ".mwf", ".mwf/project.json", ".mwf/run.json", ".mwf_run.json",
    ".mwf_threads.json", ".mwf_locks/owner", "node/A/node_state.json",
    "node/A/default_jobs.json", "node/A/job_index.json", "node/A/job_index.dirty",
    "node/A/queued/1.queued", "node/A/idempotency/old.json", "node/A/schema.json",
    "node/A/jobs/1/job.json", "node/A/jobs/1/status.json", "node/A/jobs/1/execution.json",
    "node/A/jobs/1/runtime.json", "node/A/jobs/1/events.jsonl", "node/A/jobs/1/input.json",
])
def test_fresh_session_storage_refuses_existing_runtime_without_changing_files(tmp_path, relative):
    existing = tmp_path / relative
    existing.parent.mkdir(parents=True, exist_ok=True)
    existing.write_bytes(b'{"status":"done","run_id":"retained"}')
    before = {path.relative_to(tmp_path).as_posix(): path.read_bytes() if path.is_file() else None
              for path in tmp_path.rglob("*")}
    with pytest.raises((RuntimeError, FileExistsError)):
        FileStorage._create_new_project_state(tmp_path)
    after = {path.relative_to(tmp_path).as_posix(): path.read_bytes() if path.is_file() else None
             for path in tmp_path.rglob("*")}
    assert after == before


def test_named_live_readers_use_main_then_single_interrupt_and_report_ambiguity(tmp_path):
    storage = FileStorage._create_new_project_state(tmp_path)
    assert storage.get_live_main_session() is None
    assert storage.get_live_execution_session() is None
    assert storage.list_live_execution_sessions() == []
    current = {
        "hostname": socket.gethostname(), "pid": os.getpid(),
        "process_identity": process_identity(os.getpid()),
        "started_at": datetime.now(timezone.utc).isoformat(),
    }
    _session(storage, "stale", kind="interrupt", hostname=socket.gethostname() + "-other",
             started_at="2020-01-01T00:00:00+00:00")
    _session(storage, "terminal", kind="interrupt", **current)
    storage.finish_execution_session("terminal", outcome="stopped", finished_at=current["started_at"])
    _session(storage, "recycled", kind="interrupt", **dict(current, process_identity="old-instance"))
    assert storage.get_live_execution_session() is None
    assert storage.get_execution_session("stale")["status"] == "running"
    assert storage.get_execution_session("terminal")["outcome"] == "stopped"
    assert storage.get_execution_session("recycled")["status"] == "running"

    _session(storage, "interrupt-z", kind="interrupt", **current)
    assert storage.get_live_main_session() is None
    assert storage.get_live_execution_session()["session_id"] == "interrupt-z"
    _session(storage, "interrupt-a", kind="interrupt", **current)
    with pytest.raises(RuntimeError, match="interrupt-a.*interrupt-z"):
        storage.get_live_execution_session()
    _session(storage, "main-1", **current)
    assert storage.get_live_main_session()["session_id"] == "main-1"
    assert storage.get_live_execution_session()["session_id"] == "main-1"
    assert [session["session_id"] for session in storage.list_live_execution_sessions()] == [
        "interrupt-a", "interrupt-z", "main-1",
    ]
    assert [session["session_id"] for session in storage.list_execution_sessions()] == [
        "interrupt-a", "interrupt-z", "main-1", "recycled", "stale", "terminal",
    ]
    storage.finish_execution_session("main-1", outcome="done", finished_at=current["started_at"])
    with pytest.raises(RuntimeError, match="interrupt-a.*interrupt-z"):
        storage.get_live_execution_session()
    storage.finish_execution_session("interrupt-z", outcome="done", finished_at=current["started_at"])
    assert storage.get_live_execution_session()["session_id"] == "interrupt-a"
    storage.close_database_connections()


@pytest.mark.parametrize("fields", [
    {"session_id": ""}, {"session_kind": "worker"}, {"command": ""},
    {"start_component": ()}, {"selected_components": []},
    {"start_component": ("B",)}, {"selected_components": [("A",), ("A", "B")]},
    {"selected_jobs": [("B", 1)]}, {"selected_jobs": [("A", 0)]},
    {"selected_jobs": [("A", True)]}, {"pid": True}, {"pid": 0},
    {"hostname": ""}, {"process_identity": 7}, {"started_at": "not a time"},
    {"details": ["not an object"]}, {"selected_components": ["A"]},
    {"selected_components": {("A",)}}, {"selected_components": frozenset({("A",)})},
    {"selected_jobs": {("A", 1)}}, {"selected_jobs": frozenset({("A", 1)})},
])
def test_session_creation_rejects_invalid_identity_or_scope_without_partial_rows(tmp_path, fields):
    storage = FileStorage._create_new_project_state(tmp_path)
    with pytest.raises(ValueError):
        _session(storage, **fields)
    assert storage.list_execution_sessions() == []
    with sqlite3.connect(tmp_path / ".mwf" / "state.sqlite3") as connection:
        assert connection.execute("SELECT COUNT(*) FROM session_components").fetchone() == (0,)
        assert connection.execute("SELECT COUNT(*) FROM session_jobs").fetchone() == (0,)
    _session(storage)
    assert storage.get_execution_session("main-1")["status"] == "running"
    storage.close_database_connections()


def test_session_mutations_share_the_writer_and_rollback_partial_children(tmp_path, monkeypatch):
    storage = FileStorage._create_new_project_state(tmp_path)
    storage.create_job(Job(node_name="A", job_id=1, params={}))
    storage.register_component_topology(_single_snapshot())
    original = storage.submit_db_mutation
    mutations = []

    def submit(operation, *, wait=True, priority=10):
        mutations.append((wait, priority))
        return original(operation, wait=wait, priority=priority)

    monkeypatch.setattr(storage, "submit_db_mutation", submit)
    with pytest.raises(sqlite3.IntegrityError):
        _session(storage, register_topology=False, selected_jobs=[("A", 1), ("A", 1)])
    assert storage.get_execution_session("main-1") is None
    with sqlite3.connect(tmp_path / ".mwf" / "state.sqlite3") as connection:
        assert connection.execute("SELECT COUNT(*) FROM session_components").fetchone() == (0,)
        assert connection.execute("SELECT COUNT(*) FROM session_jobs").fetchone() == (0,)
    _session(storage, register_topology=False, selected_jobs=[("A", 1)])
    storage.heartbeat_execution_session("main-1", "2026-09-05T12:01:00+00:00")
    storage.finish_execution_session("main-1", outcome="done", finished_at="2026-09-05T12:02:00+00:00")
    assert mutations == [(True, 0)] * 4
    assert storage.get_execution_session("main-1")["selected_jobs"] == [("A", 1)]
    storage.close_database_connections()


def test_session_creation_requires_an_actual_parent_and_keeps_failed_request_atomic(tmp_path):
    storage = FileStorage._create_new_project_state(tmp_path)
    with pytest.raises(sqlite3.IntegrityError):
        _session(storage, "child-1", kind="interrupt", parent_session_id="absent")
    assert storage.list_execution_sessions() == []
    _session(storage)
    _session(storage, "child-1", kind="interrupt", parent_session_id="main-1")
    assert storage.get_execution_session("child-1")["parent_session_id"] == "main-1"
    storage.close_database_connections()


def test_failed_schema_creation_does_not_publish_a_partial_version(tmp_path, monkeypatch):
    source = tmp_path / "readme.txt"
    source.write_bytes(b"keep this source")
    original = FileStorage._create_execution_session_tables

    def fail_after_tables(connection):
        original(connection)
        raise OSError("schema publication interrupted")

    with monkeypatch.context() as patch:
        patch.setattr(FileStorage, "_create_execution_session_tables", staticmethod(fail_after_tables))
        with pytest.raises(OSError, match="schema publication interrupted"):
            FileStorage._create_new_project_state(tmp_path)
    assert not (tmp_path / ".mwf").exists()
    assert source.read_bytes() == b"keep this source"
    storage = FileStorage._create_new_project_state(tmp_path)
    with sqlite3.connect(tmp_path / ".mwf" / "state.sqlite3") as connection:
        assert connection.execute(
            "SELECT value FROM metadata WHERE key='database_schema_version'"
        ).fetchone() == ("6",)
    _session(storage)
    assert storage.get_execution_session("main-1")["status"] == "running"
    assert storage.database_integrity_check() == "ok"
    storage.close_database_connections()


def test_session_operations_refuse_changed_schema_without_mutation(tmp_path):
    from tests.test_090_component_session_settlement import _rows, _close

    storage = FileStorage(tmp_path)
    try:
        storage.submit_db_mutation(lambda connection: connection.execute(
            "UPDATE metadata SET value='4' WHERE key='database_schema_version'",
        ))
        before = _rows(storage)
        for operation in (
            lambda: _session(
                storage, expected_shape=_single_snapshot().shape_json, register_topology=False,
            ),
            lambda: storage.get_execution_session('main-1'),
            storage.list_execution_sessions,
            lambda: storage.heartbeat_execution_session('main-1', '2026-09-05T12:01:00+00:00'),
            lambda: storage.finish_execution_session('main-1', outcome='done', finished_at='2026-09-05T12:02:00+00:00'),
        ):
            with pytest.raises(RuntimeError, match='session-capable'):
                operation()
            assert _rows(storage) == before
    finally:
        _close(storage)


def test_fresh_session_storage_keeps_user_source_and_output_files(tmp_path):
    source = tmp_path / "src" / "graph.py"
    source.parent.mkdir()
    source.write_bytes(b"EDGES = []\n")
    output = tmp_path / "node" / "A" / "output" / "result.txt"
    output.parent.mkdir(parents=True)
    output.write_bytes(b"user-owned result")
    storage = FileStorage._create_new_project_state(tmp_path)
    _session(storage)
    assert source.read_bytes() == b"EDGES = []\n"
    assert output.read_bytes() == b"user-owned result"
    assert (tmp_path / ".mwf" / "project.json").is_file()
    assert not (tmp_path / ".mwf" / "run.json").exists()
    storage.close_database_connections()


@pytest.mark.parametrize("update", [
    "bad-heartbeat", "bad-finish-time", "empty-outcome", "failure-object",
])
def test_invalid_session_updates_preserve_the_running_record(tmp_path, update):
    storage = FileStorage._create_new_project_state(tmp_path)
    original = _session(storage)
    with pytest.raises(ValueError):
        if update == "bad-heartbeat":
            storage.heartbeat_execution_session("main-1", "unknown")
        else:
            storage.finish_execution_session(
                "main-1", outcome="" if update == "empty-outcome" else "failed",
                finished_at="unknown" if update == "bad-finish-time" else "2026-09-05T12:01:00+00:00",
                failures={"error": "unordered"} if update == "failure-object" else [],
            )
    assert storage.get_execution_session("main-1") == original
    storage.close_database_connections()


@pytest.mark.parametrize("damage", ["main-uniqueness", "child-shape", "shape-immutability", "schema-marker", "extra-constraint"])
def test_incomplete_session_schema_refuses_reopen_without_importing_unrelated_files(tmp_path, damage):
    storage = FileStorage._create_new_project_state(tmp_path)
    storage.close_database_connections()
    with sqlite3.connect(tmp_path / ".mwf" / "state.sqlite3") as connection:
        if damage == "main-uniqueness":
            connection.execute("DROP INDEX one_running_main_session")
        elif damage == "child-shape":
            connection.execute("DROP TABLE session_jobs")
            connection.execute("CREATE TABLE session_jobs(session_id TEXT, job_id INTEGER)")
        elif damage == "shape-immutability":
            connection.execute("DROP TRIGGER prevent_graph_shape_update")
        elif damage == "schema-marker":
            connection.execute("DELETE FROM metadata WHERE key='database_schema_version'")
        else:
            connection.execute("CREATE UNIQUE INDEX only_one_interrupt ON execution_sessions(session_kind) "
                               "WHERE session_kind='interrupt' AND status='running'")
    legacy = tmp_path / "node" / "A" / "node_state.json"
    legacy.parent.mkdir(parents=True)
    legacy.write_bytes(b'{"status":"failed"}')
    script = """
import sys
from micro_workflow_manager.storage import FileStorage
try:
    FileStorage(sys.argv[1])
except RuntimeError as error:
    assert 'Incomplete SQLite execution-session schema' in str(error), str(error)
else:
    raise AssertionError('Damaged session schema was accepted')
"""
    result = subprocess.run([sys.executable, "-c", script, str(tmp_path)],
                            capture_output=True, text=True, timeout=20)
    assert result.returncode == 0, result.stderr
    assert legacy.read_bytes() == b'{"status":"failed"}'


def test_fresh_creation_works_without_python_312_path_methods(tmp_path, monkeypatch):
    monkeypatch.delattr(Path, "is_junction", raising=False)
    storage = FileStorage._create_new_project_state(tmp_path)
    _session(storage)
    assert storage.get_execution_session("main-1")["status"] == "running"
    storage.close_database_connections()


def test_failed_cleanup_preserves_unexpected_files_and_original_error_on_python_310(tmp_path, monkeypatch):
    class OlderError(OSError):
        def __getattribute__(self, name):
            if name == "add_note":
                raise AttributeError("Python 3.10 exceptions have no add_note")
            return super().__getattribute__(name)

    def fail(connection):
        (tmp_path / ".mwf" / "keep.txt").write_bytes(b"unexpected user file")
        raise OlderError("schema interrupted")

    monkeypatch.setattr(FileStorage, "_create_execution_session_tables", staticmethod(fail))
    with pytest.raises(OlderError, match="schema interrupted"):
        FileStorage._create_new_project_state(tmp_path)
    assert (tmp_path / ".mwf" / "keep.txt").read_bytes() == b"unexpected user file"


@pytest.mark.parametrize("relative, dangling", [(".mwf", True), (".mwf_run.json", True), ("node", False), ("node/A", False)])
def test_fresh_creation_refuses_real_directory_links_without_python_312_methods(tmp_path, monkeypatch, relative, dangling):
    target = tmp_path / "target"
    target.mkdir()
    link = tmp_path / relative
    link.parent.mkdir(parents=True, exist_ok=True)
    if os.name == "nt":
        quoted_link = str(link).replace("'", "''")
        quoted_target = str(target).replace("'", "''")
        command = f"New-Item -ItemType Junction -Path '{quoted_link}' -Target '{quoted_target}' -ErrorAction Stop | Out-Null"
        result = subprocess.run(["powershell.exe", "-NoProfile", "-Command", command],
                                capture_output=True, text=True, timeout=20)
        assert result.returncode == 0, result.stderr
    else:
        link.symlink_to(target, target_is_directory=True)
    if dangling:
        target.rmdir()
    else:
        (target / "keep.txt").write_bytes(b"linked source")
    monkeypatch.delattr(Path, "is_junction", raising=False)
    with pytest.raises(RuntimeError, match="existing MWF runtime state"):
        FileStorage._create_new_project_state(tmp_path)
    assert os.path.lexists(link)
    if not dangling:
        assert (target / "keep.txt").read_bytes() == b"linked source"
        assert not (tmp_path / ".mwf").exists()


@pytest.mark.parametrize("kind,parent", [("interrupt", "child-1"), ("main", "parent-1")])
def test_session_parent_must_be_a_distinct_actual_parent_of_an_interrupt(tmp_path, kind, parent):
    storage = FileStorage._create_new_project_state(tmp_path)
    _session(storage, "parent-1")
    storage.finish_execution_session("parent-1", outcome="done", finished_at="2026-09-05T12:01:00+00:00")
    with pytest.raises(ValueError, match="parent"):
        _session(storage, "child-1", kind=kind, parent_session_id=parent)
    assert storage.get_execution_session("child-1") is None
    storage.close_database_connections()


@pytest.mark.parametrize('other_process', [False, True])
def test_fresh_creator_preserves_a_native_store_that_initialized_first(tmp_path, monkeypatch, other_process):
    original = FileStorage._refuse_existing_runtime
    competed = False

    def check(root):
        nonlocal competed
        original(root)
        if competed:
            return
        competed = True
        if other_process:
            result = subprocess.run([
                sys.executable, '-c',
                'import sys; from micro_workflow_manager.storage import FileStorage; '
                'from micro_workflow_manager.models import Job; '
                's=FileStorage(sys.argv[1]); s.create_job(Job(node_name="A", job_id=1, params={"competing":True})); '
                's.close_database_connections()', str(root),
            ], capture_output=True, text=True, timeout=20)
            assert result.returncode == 0, result.stderr
        else:
            ordinary = FileStorage(root)
            ordinary.create_job(Job(node_name='A', job_id=1, params={'competing': True}))
            ordinary.close_database_connections()

    with monkeypatch.context() as patch:
        patch.setattr(FileStorage, '_refuse_existing_runtime', staticmethod(check))
        with pytest.raises(FileExistsError):
            FileStorage._create_new_project_state(tmp_path)
    storage = FileStorage(tmp_path)
    try:
        assert storage.load_job('A', 1).params == {'competing': True}
        assert storage.list_execution_sessions() == []
        assert storage.database_integrity_check() == 'ok'
        assert storage.db_connection().execute(
            "SELECT value FROM metadata WHERE key='database_schema_version'",
        ).fetchone()[0] == '6'
    finally:
        storage.close_database_connections()


def test_concurrent_open_refuses_unpublished_native_configuration_without_changing_state(tmp_path):
    fresh_script = """
import sys, time
from pathlib import Path
from micro_workflow_manager.storage import FileStorage
root = Path(sys.argv[1])
original = FileStorage._init_job_execution_state
def pause(storage):
    original(storage)
    print('fresh-ready', flush=True)
    deadline = time.monotonic() + 20
    while not (root / 'release').exists():
        if time.monotonic() > deadline: raise RuntimeError('release missing')
        time.sleep(0.001)
FileStorage._init_job_execution_state = pause
storage = FileStorage._create_new_project_state(root)
storage.close_database_connections()
"""
    ordinary_script = """
import sys
from micro_workflow_manager.storage import FileStorage
try:
    FileStorage(sys.argv[1])
except RuntimeError:
    print('refused', flush=True)
else:
    raise AssertionError('Unpublished native project was accepted')
"""
    fresh = subprocess.Popen([sys.executable, '-c', fresh_script, str(tmp_path)],
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        assert fresh.stdout.readline().strip() == 'fresh-ready'
        before = {p.name: p.read_bytes() for p in (tmp_path / '.mwf').iterdir() if p.is_file()}
        ordinary = subprocess.run([sys.executable, '-c', ordinary_script, str(tmp_path)],
                                  capture_output=True, text=True, timeout=20)
        assert ordinary.returncode == 0 and ordinary.stdout.strip() == 'refused', ordinary.stderr
        assert {p.name: p.read_bytes() for p in (tmp_path / '.mwf').iterdir() if p.is_file()} == before
        (tmp_path / 'release').touch()
        stdout, stderr = fresh.communicate(timeout=25)
        assert fresh.returncode == 0, stderr
    finally:
        (tmp_path / 'release').touch()
        if fresh.poll() is None:
            fresh.kill()
        fresh.communicate(timeout=5)
    storage = FileStorage(tmp_path)
    try:
        _session(storage)
        assert storage.get_execution_session('main-1')['status'] == 'running'
        assert storage.database_integrity_check() == 'ok'
    finally:
        storage.close_database_connections()


def test_fresh_creation_retries_after_its_schema_commits_but_object_setup_fails(tmp_path, monkeypatch):
    def fail(storage):
        raise OSError("object setup interrupted after commit")

    with monkeypatch.context() as patch:
        patch.setattr(FileStorage, "_init_job_execution_state", fail)
        with pytest.raises(OSError, match="object setup interrupted"):
            FileStorage._create_new_project_state(tmp_path)
    assert not (tmp_path / ".mwf").exists()
    storage = FileStorage._create_new_project_state(tmp_path)
    _session(storage)
    assert storage.get_execution_session("main-1")["status"] == "running"
    assert storage.database_integrity_check() == "ok"
    storage.close_database_connections()
