"""Public native clipboard component isolation and project provenance."""

from __future__ import annotations

import shutil
import time

import networkx as nx
import pytest

from micro_workflow_manager import cli
from micro_workflow_manager.cli.node_clipboard import copy_node_to_clipboard, paste_node_from_clipboard
from micro_workflow_manager.storage import FileStorage
from micro_workflow_manager.storage.clipboard_files import ClipboardFiles
from micro_workflow_manager.storage.component_membership import read_active_component_for_node
from micro_workflow_manager.topology import ComponentTopology
from tests.test_036_hoeflein_scheduling import make_project
from tests.test_064_read_only_previews import _live_execution_session_identity
from tests.test_090_component_session_settlement import _close, _rows
from tests.test_117_execution_sampling import _files


def _component_project(root, monkeypatch):
    make_project(
        root,
        monkeypatch,
        edges="EDGES = [('A', 'B'), ('B', 'A')]",
        runner="direct",
        files={
            "A": '''
                from pathlib import Path
                from micro_workflow_manager import NodeRouter
                router = NodeRouter("A", runner="direct")
                router.create_job(params={"role": "A"})
                @router.task
                def run(ctx, role):
                    assert role == "A"
                    return "A"
            ''',
            "B": '''
                from micro_workflow_manager import NodeRouter
                router = NodeRouter("B", runner="direct")
                router.create_job(params={"role": "B"})
                @router.task
                def run(ctx, role):
                    assert role == "B"
                    return "B"
            ''',
        },
    )
    return FileStorage(root)


def _node_rows(connection, node):
    result = {}
    for table in (
        "nodes", "jobs", "job_instances", "job_events", "idempotency",
        "default_job_specs", "job_sequences", "managed_input_files",
        "managed_input_producers",
    ):
        column = "receiver_node" if table.startswith("managed_input_") else "node_name"
        result[table] = tuple(dict(row) for row in connection.execute(
            f'SELECT * FROM "{table}" WHERE {column}=? ORDER BY rowid', (node,),
        ))
    return result


def _settle_writer(storage):
    storage.db_mutation_barrier()
    deadline = time.monotonic() + 10
    while storage.mutation_writer_diagnostics()["writer_alive"]:
        assert time.monotonic() < deadline
        time.sleep(0.01)
    deadline = time.monotonic() + 10
    subscribers = storage.project_dir / ".mwf" / "state_subscribers"
    while any(subscribers.glob("*.json")):
        assert time.monotonic() < deadline, "Finished setup retained a state subscriber"
        time.sleep(0.01)


@pytest.mark.parametrize(
    "operation", (copy_node_to_clipboard, paste_node_from_clipboard),
    ids=("copy", "paste"),
)
def test_clipboard_preflight_refuses_admitted_raw_node_membership_overlap(
    tmp_path, monkeypatch, operation,
):
    storage = FileStorage(tmp_path)
    try:
        old = ComponentTopology(nx.DiGraph([("A", "B"), ("B", "A")]), []).snapshot()
        split = ComponentTopology(nx.DiGraph([("A", "B")]), []).snapshot()
        storage.register_component_topology(old)
        storage.register_component_topology(split)
        assert read_active_component_for_node(storage.db_connection(), "A") == ("A",)
        (tmp_path / "node" / "A").mkdir(parents=True)
        assert copy_node_to_clipboard(tmp_path, "A") == 0
        storage.create_execution_session(
            "overlapping-membership-repair", session_kind="main", command="run",
            start_component=("A", "B"), selected_components=[("A", "B")],
            expected_shape=old.shape_json, **_live_execution_session_identity(),
        )
        assert storage.reserve_execution_components(
            "overlapping-membership-repair", expected_shape=old.shape_json,
        ) is True
        assert storage.get_component_reservation(("A", "B")) == {
            "members": ("A", "B"), "session_id": "overlapping-membership-repair",
        }
        assert storage.db_connection().execute(
            "SELECT scope_admitted FROM execution_sessions WHERE session_id=?",
            ("overlapping-membership-repair",),
        ).fetchone()["scope_admitted"] == 1
        _settle_writer(storage)
        before_rows = _rows(storage)
        before_files = _files(tmp_path)

        with pytest.raises(RuntimeError, match="finish or stop first"):
            operation(tmp_path, "A")

        assert _rows(storage) == before_rows
        assert _files(tmp_path) == before_files
    finally:
        _close(storage)


def test_paste_restores_only_the_named_member_and_preserves_its_peer(
    tmp_path, monkeypatch, capsys,
):
    storage = _component_project(tmp_path, monkeypatch)
    try:
        capsys.readouterr()
        assert cli.main(["run", "A", "--runner", "direct"]) == 0
        capsys.readouterr()
        assert cli.main(["copy", "A"]) == 0
        capsys.readouterr()
        saved_a_rows = _node_rows(storage.db_connection(), "A")
        saved_a_files = _files(tmp_path / "node" / "A")
        peer_rows = _node_rows(storage.db_connection(), "B")
        peer_files = _files(tmp_path / "node" / "B")
        changed = tmp_path / "node" / "A" / "output" / "later.txt"
        changed.write_bytes(b"later A material")

        assert cli.main(["paste", "A"]) == 0
        capsys.readouterr()

        assert _node_rows(storage.db_connection(), "A") == saved_a_rows
        assert _files(tmp_path / "node" / "A") == saved_a_files
        assert _node_rows(storage.db_connection(), "B") == peer_rows
        assert _files(tmp_path / "node" / "B") == peer_files
        assert not changed.exists()
    finally:
        _close(storage)


def test_paste_refuses_changed_peer_work_before_any_clipboard_mutation(
    tmp_path, monkeypatch, capsys,
):
    storage = _component_project(tmp_path, monkeypatch)
    try:
        capsys.readouterr()
        assert cli.main(["run", "A", "--runner", "direct"]) == 0
        capsys.readouterr()
        assert cli.main(["copy", "A"]) == 0
        capsys.readouterr()
        storage.set_job_status("B", 1, "queued")
        _settle_writer(storage)
        before_rows = _rows(storage)
        before_files = _files(tmp_path)

        assert cli.main(["paste", "A"]) == 1
        assert "peer work" in capsys.readouterr().err.lower()
        assert _rows(storage) == before_rows
        assert _files(tmp_path) == before_files
    finally:
        _close(storage)


def test_paste_refuses_saved_snapshot_sidecars_without_copying_them(
    tmp_path, monkeypatch, capsys,
):
    storage = _component_project(tmp_path, monkeypatch)
    try:
        capsys.readouterr()
        assert cli.main(["run", "A", "--runner", "direct"]) == 0
        capsys.readouterr()
        assert cli.main(["copy", "A"]) == 0
        capsys.readouterr()
        sidecar = tmp_path / "clipboard" / "A" / ".mwf-node-state.sqlite3-wal"
        sidecar.write_bytes(b"independent journal material")
        _settle_writer(storage)
        before_rows = _rows(storage)
        before_files = _files(tmp_path)

        assert cli.main(["paste", "A"]) == 1
        assert "unexpected journal sidecars" in capsys.readouterr().err
        assert _rows(storage) == before_rows
        assert _files(tmp_path) == before_files
        assert sidecar.read_bytes() == b"independent journal material"
    finally:
        _close(storage)


def _single_project(root, monkeypatch):
    make_project(
        root,
        monkeypatch,
        edges="EDGES = [('A', 'A')]",
        runner="direct",
        files={
            "A": '''
                from micro_workflow_manager import NodeRouter
                router = NodeRouter("A", runner="direct")
                router.create_job()
                @router.task
                def run(ctx): return "A"
            ''',
        },
    )
    return FileStorage(root)


def test_paste_refuses_another_project_snapshot_before_state_or_file_mutation(
    tmp_path, monkeypatch, capsys,
):
    source_root = tmp_path / "source"
    target_root = tmp_path / "target"
    source_root.mkdir()
    target_root.mkdir()
    source = _single_project(source_root, monkeypatch)
    try:
        capsys.readouterr()
        assert cli.main(["run", "A", "--runner", "direct"]) == 0
        capsys.readouterr()
        assert cli.main(["copy", "A"]) == 0
        capsys.readouterr()
    finally:
        _close(source)

    target = _single_project(target_root, monkeypatch)
    try:
        capsys.readouterr()
        assert cli.main(["run", "A", "--runner", "direct"]) == 0
        capsys.readouterr()
        copied = target_root / "clipboard" / "A"
        copied.parent.mkdir()
        shutil.copytree(source_root / "clipboard" / "A", copied)
        _settle_writer(target)
        before_rows = _rows(target)
        before_files = _files(target_root)

        assert cli.main(["paste", "A"]) == 1
        assert "another project" in capsys.readouterr().err.lower()
        assert _rows(target) == before_rows
        assert _files(target_root) == before_files
    finally:
        _close(target)


def test_recover_restores_a_prepared_paste_after_process_loss(
    tmp_path, monkeypatch, capsys,
):
    storage = _component_project(tmp_path, monkeypatch)
    closed = False
    try:
        capsys.readouterr()
        assert cli.main(["run", "A", "--runner", "direct"]) == 0
        capsys.readouterr()
        assert cli.main(["copy", "A"]) == 0
        capsys.readouterr()
        changed = tmp_path / "node" / "A" / "output" / "after-copy.txt"
        changed.write_bytes(b"must be restored")
        before_rows = _node_rows(storage.db_connection(), "A")
        before_node_files = _files(tmp_path / "node" / "A")
        before_clipboard = _files(tmp_path / "clipboard" / "A")
        original_publish = ClipboardFiles.publish
        loss = RuntimeError("simulated clipboard process loss")

        def publish_and_lose(files):
            original_publish(files)
            raise loss

        def retain_prepared_operation(*_args, **_kwargs):
            raise loss

        with monkeypatch.context() as faults:
            faults.setattr(ClipboardFiles, "publish", publish_and_lose)
            faults.setattr(storage, "_abort_clipboard", retain_prepared_operation)
            with pytest.raises(RuntimeError) as raised:
                storage.restore_node_clipboard("A", tmp_path / "clipboard" / "A")
            assert raised.value is loss

        _settle_writer(storage)
        attempt = dict(storage.db_connection().execute(
            "SELECT * FROM clipboard_attempts WHERE operation='paste'"
        ).fetchone())
        assert attempt["operation"] == "paste"
        assert attempt["state"] == "prepared"
        assert dict(storage.db_connection().execute(
            "SELECT * FROM clipboard_receipts WHERE operation_id=?",
            (attempt["operation_id"],),
        ).fetchone())["state"] == "prepared"
        assert _files(tmp_path / "node" / "A") != before_node_files

        _settle_writer(storage)
        preview_rows = _rows(storage)
        preview_files = _files(tmp_path)
        assert cli.main(["recover", "--dry-run"]) == 0
        preview = capsys.readouterr().out
        assert attempt["operation_id"] in preview
        assert "clipboard paste operation" in preview
        assert "would restore its prior destination" in preview
        assert _rows(storage) == preview_rows
        assert _files(tmp_path) == preview_files
        assert cli.main(["doctor"]) == 0
        doctor = capsys.readouterr().out
        assert attempt["operation_id"] in doctor
        assert "mwf recover --dry-run" in doctor
        assert _rows(storage) == preview_rows
        assert _files(tmp_path) == preview_files

        _close(storage)
        closed = True
        assert cli.main(["recover"]) == 0
        capsys.readouterr()
        recovered = FileStorage(tmp_path)
        try:
            assert _node_rows(recovered.db_connection(), "A") == before_rows
            assert _files(tmp_path / "node" / "A") == before_node_files
            assert _files(tmp_path / "clipboard" / "A") == before_clipboard
            assert dict(recovered.db_connection().execute(
                "SELECT * FROM clipboard_attempts WHERE operation_id=?",
                (attempt["operation_id"],),
            ).fetchone())["state"] == "aborted"
            assert dict(recovered.db_connection().execute(
                "SELECT * FROM clipboard_receipts WHERE operation_id=?",
                (attempt["operation_id"],),
            ).fetchone())["state"] == "aborted"
            assert recovered.db_connection().execute(
                "SELECT 1 FROM receiver_mutation_guards WHERE operation_id=?",
                (attempt["operation_id"],),
            ).fetchone() is None
            assert not (tmp_path / ".mwf" / "clipboard-operations" /
                        attempt["operation_id"]).exists()
        finally:
            _close(recovered)
    finally:
        if not closed:
            _close(storage)
