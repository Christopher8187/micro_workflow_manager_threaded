"""A storage owner cannot close a concurrent owner's SQLite connection."""

from __future__ import annotations

import subprocess
import sys
import textwrap

from micro_workflow_manager import cli
from micro_workflow_manager.storage import FileStorage


def _initialize_native_storage(root):
    storage = FileStorage._create_new_project_state(root)
    storage.close_database_connections()


def test_closing_peer_storage_preserves_an_active_mutation_writer(
    tmp_path,
):
    _initialize_native_storage(tmp_path)
    source = textwrap.dedent(
        """
        import sys
        import threading

        from micro_workflow_manager.storage import FileStorage

        root = sys.argv[1]
        writer_owner = FileStorage(root)
        closing_peer = FileStorage(root)
        writer_owner.set_api_total_limit(1)
        entered = threading.Event()
        release = threading.Event()

        def held_write(connection):
            changed = connection.execute(
                "UPDATE api_thread_limit SET value=value+1 WHERE singleton=1 AND value=1"
            )
            if changed.rowcount != 1:
                raise RuntimeError("Initial held update did not reach its exact row")
            entered.set()
            if not release.wait(timeout=10):
                raise RuntimeError("Timed out waiting to finish the held update")
            changed = connection.execute(
                "UPDATE api_thread_limit SET value=value+1 WHERE singleton=1 AND value=2"
            )
            if changed.rowcount != 1:
                raise RuntimeError("Active writer connection did not remain usable")
            row = connection.execute(
                "SELECT value FROM api_thread_limit WHERE singleton=1"
            ).fetchone()
            if row is None or row["value"] != 3:
                raise RuntimeError("Held update readback failed")
            return row["value"]

        future = writer_owner.submit_db_mutation(held_write, wait=False, priority=0)
        if not entered.wait(timeout=10):
            raise RuntimeError("Mutation writer did not enter the held transaction")
        try:
            closing_peer.close_database_connections()
        finally:
            release.set()
        if future.result(timeout=10) != 3:
            raise RuntimeError("Mutation writer returned an unexpected result")
        writer_owner.close_database_connections()
        """
    )
    result = subprocess.run(
        [sys.executable, "-c", source, str(tmp_path)],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, (
        f"Concurrent storage close failed with {result.returncode}:\n"
        f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )

    storage = FileStorage(tmp_path)
    try:
        assert storage.read_api_total_limit() == 3
        assert [tuple(row) for row in storage.db_connection().execute(
            "SELECT singleton, value FROM api_thread_limit"
        )] == [(1, 3)]
    finally:
        storage.close_database_connections()


def test_explicit_close_is_repeatable_and_the_same_storage_can_reopen(
    tmp_path,
):
    _initialize_native_storage(tmp_path)
    storage = FileStorage(tmp_path)
    storage.set_api_total_limit(2)
    storage.close_database_connections()
    storage.close_database_connections()

    assert storage.read_api_total_limit() == 2
    assert storage.update_api_total_limit(3, relative=True) == 5
    storage.close_database_connections()
    storage.close_database_connections()

    observer = FileStorage(tmp_path)
    try:
        assert observer.read_api_total_limit() == 5
        assert [tuple(row) for row in observer.db_connection().execute(
            "SELECT singleton, value FROM api_thread_limit"
        )] == [(1, 5)]
    finally:
        observer.close_database_connections()


def test_init_and_graph_close_their_exact_storage_owners_on_every_exit(
    tmp_path, monkeypatch,
):
    monkeypatch.chdir(tmp_path)
    source = tmp_path / "src"
    source.mkdir()
    graph = source / "graph.py"
    graph.write_text("raise RuntimeError('graph import stopped')\n", encoding="utf-8")

    assert cli.main(["init"]) == 0
    assert not (tmp_path / ".mwf" / "state.sqlite3-wal").exists()
    assert not (tmp_path / ".mwf" / "state.sqlite3-shm").exists()

    assert cli.main(["graph", "src/graph.py", "--runner", "direct"]) == 1
    assert not (tmp_path / ".mwf" / "state.sqlite3-wal").exists()
    assert not (tmp_path / ".mwf" / "state.sqlite3-shm").exists()

    graph.write_text("EDGES = [('A', 'A')]\n", encoding="utf-8")
    behavior = source / "node_behavior"
    behavior.mkdir()
    (behavior / "A.py").write_text(
        "from micro_workflow_manager import NodeRouter\n"
        "router = NodeRouter('A', runner='direct')\n"
        "@router.task\n"
        "def work(ctx): return 'done'\n",
        encoding="utf-8",
    )
    assert cli.main(["graph", "src/graph.py", "--runner", "direct"]) == 0
    assert not (tmp_path / ".mwf" / "state.sqlite3-wal").exists()
    assert not (tmp_path / ".mwf" / "state.sqlite3-shm").exists()
