"""Process-wide SQLite connection cleanup must stay off healthy read paths."""

from __future__ import annotations

import subprocess
import sys
import textwrap


def _run_isolated(tmp_path, source: str) -> None:
    result = subprocess.run(
        [sys.executable, "-c", textwrap.dedent(source), str(tmp_path)],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, (
        f"Isolated connection-registry case failed with {result.returncode}:\n"
        f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )


def test_existing_connection_does_not_scan_historical_project_paths(tmp_path):
    _run_isolated(
        tmp_path,
        """
        import os
        import sys
        import threading
        from pathlib import Path

        from micro_workflow_manager.storage import FileStorage
        from micro_workflow_manager.storage.sqlite.connection import SQLiteConnectionMixin

        root = Path(sys.argv[1])
        storage = FileStorage._create_new_project_state(root)
        assert storage.read_api_total_limit() is None
        registry = SQLiteConnectionMixin._connection_registry
        owners = SQLiteConnectionMixin._connection_threads
        guard = SQLiteConnectionMixin._connection_registry_guard
        pid = os.getpid()

        class RetainedConnection:
            def __init__(self):
                self.closed = False

            def close(self):
                self.closed = True

        retained = []
        historical_paths = set()
        with guard:
            for offset in range(256):
                path = (root.parent / f"historical-project-{offset}" / ".mwf" / "state.sqlite3").resolve()
                key = (path, pid, 100_000 + offset)
                connection = RetainedConnection()
                registry[key] = connection
                owners[key] = threading.current_thread()
                retained.append((key, connection))
                historical_paths.add(path)

        original_exists = Path.exists
        scanned = []

        def observed_exists(path):
            if path in historical_paths:
                scanned.append(path)
                raise AssertionError(f"healthy read scanned historical path: {path}")
            return original_exists(path)

        Path.exists = observed_exists
        try:
            for _ in range(100):
                assert storage.read_api_total_limit() is None
        finally:
            Path.exists = original_exists
            with guard:
                for key, _connection in retained:
                    registry.pop(key, None)
                    owners.pop(key, None)
            storage.close_database_connections()

        assert scanned == []
        assert all(not connection.closed for _key, connection in retained)
        """,
    )


def test_connection_admission_still_prunes_dead_and_deleted_entries(tmp_path):
    _run_isolated(
        tmp_path,
        """
        import os
        import sys
        import threading
        from pathlib import Path

        from micro_workflow_manager.storage import FileStorage
        from micro_workflow_manager.storage.sqlite.connection import SQLiteConnectionMixin

        root = Path(sys.argv[1])
        storage = FileStorage._create_new_project_state(root)
        storage.db_connection()
        registry = SQLiteConnectionMixin._connection_registry
        owners = SQLiteConnectionMixin._connection_threads
        guard = SQLiteConnectionMixin._connection_registry_guard
        pid = os.getpid()
        current = threading.current_thread()

        class RetainedConnection:
            def __init__(self):
                self.closed = False

            def close(self):
                self.closed = True

        dead_owner = threading.Thread(target=lambda: None)
        dead_owner.start()
        dead_owner.join()
        dead_path = root / "dead-owner.sqlite3"
        dead_path.write_bytes(b"retained")
        deleted_path = root / "deleted-project" / ".mwf" / "state.sqlite3"
        dead_key = (dead_path.resolve(), pid, 200_001)
        deleted_key = (deleted_path.resolve(), pid, 200_002)
        dead_connection = RetainedConnection()
        deleted_connection = RetainedConnection()
        fillers = []
        with guard:
            registry[dead_key] = dead_connection
            owners[dead_key] = dead_owner
            registry[deleted_key] = deleted_connection
            owners[deleted_key] = current
            for offset in range(254):
                key = (storage.state_database_path(), pid, 300_000 + offset)
                connection = RetainedConnection()
                registry[key] = connection
                owners[key] = current
                fillers.append((key, connection))

        errors = []

        def open_new_thread_connection():
            try:
                connection = storage.db_connection()
                assert connection.execute("SELECT value FROM metadata WHERE key='database_schema_version'").fetchone() is not None
            except BaseException as error:
                errors.append(error)

        worker = threading.Thread(target=open_new_thread_connection)
        worker.start()
        worker.join(timeout=30)
        assert not worker.is_alive()
        assert not errors, errors
        assert dead_connection.closed
        assert deleted_connection.closed
        with guard:
            assert dead_key not in registry and dead_key not in owners
            assert deleted_key not in registry and deleted_key not in owners
            assert all(key in registry and key in owners for key, _connection in fillers)

        assert storage.prune_dead_thread_connections() == 1
        with guard:
            for key, _connection in fillers:
                registry.pop(key, None)
                owners.pop(key, None)
        assert all(not connection.closed for _key, connection in fillers)
        storage.close_database_connections()
        """,
    )


def test_reused_thread_identifier_replaces_the_prior_connection(tmp_path):
    _run_isolated(
        tmp_path,
        """
        import sys
        import threading
        from pathlib import Path

        from micro_workflow_manager.storage import FileStorage
        from micro_workflow_manager.storage.sqlite.connection import SQLiteConnectionMixin

        root = Path(sys.argv[1])
        storage = FileStorage._create_new_project_state(root)
        storage.close_thread_connection()
        registry = SQLiteConnectionMixin._connection_registry
        owners = SQLiteConnectionMixin._connection_threads
        guard = SQLiteConnectionMixin._connection_registry_guard
        key = storage._connection_key()

        class PriorConnection:
            def __init__(self):
                self.closed = False

            def close(self):
                self.closed = True

        prior_owner = threading.Thread(target=lambda: None)
        prior_owner.start()
        prior_owner.join()
        prior = PriorConnection()
        with guard:
            registry[key] = prior
            owners[key] = prior_owner

        current = storage.db_connection()
        assert prior.closed
        assert current is not prior
        with guard:
            assert registry[key] is current
            assert owners[key] is threading.current_thread()
        assert current.execute("SELECT value FROM metadata WHERE key='database_schema_version'").fetchone() is not None
        storage.close_database_connections()
        """,
    )
