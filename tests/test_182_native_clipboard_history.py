"""Public native clipboard history and component-state restoration."""

from __future__ import annotations

import json
import sqlite3

from micro_workflow_manager import cli
from micro_workflow_manager.component_identity import encode_component_key
from micro_workflow_manager.storage import FileStorage
from tests.test_036_hoeflein_scheduling import make_project
from tests.test_090_component_session_settlement import _close
from tests.test_117_execution_sampling import _component_result, _files


NODE_TABLES = (
    "nodes", "jobs", "job_instances", "job_events", "idempotency",
    "default_job_specs", "job_sequences", "managed_input_files",
    "managed_input_producers",
)


def _node_rows(connection, node):
    result = {}
    for table in NODE_TABLES:
        column = "receiver_node" if table.startswith("managed_input_") else "node_name"
        result[table] = tuple(
            dict(row) for row in connection.execute(
                f'SELECT * FROM "{table}" WHERE {column}=? ORDER BY rowid', (node,),
            )
        )
    return result


def _history_rows(connection, component):
    key = encode_component_key(component)
    return {
        "owners": tuple(dict(row) for row in connection.execute(
            "SELECT * FROM job_execution_owners WHERE component_key=? ORDER BY execution_id", (key,),
        )),
        "results": tuple(dict(row) for row in connection.execute(
            "SELECT * FROM component_successful_results WHERE component_key=? "
            "ORDER BY shape_id, alignment_generation", (key,),
        )),
        "sessions": tuple(dict(row) for row in connection.execute(
            "SELECT session.* FROM execution_sessions AS session WHERE EXISTS("
            "SELECT 1 FROM session_components AS selected WHERE selected.session_id=session.session_id "
            "AND selected.component_key=?) ORDER BY session.session_id", (key,),
        )),
    }


def _project(tmp_path, monkeypatch, *, jobs=100):
    make_project(
        tmp_path,
        monkeypatch,
        edges="EDGES = [('A', 'A')]",
        runner="direct",
        files={
            "A": f'''
                from micro_workflow_manager import NodeRouter
                router = NodeRouter("A", runner="direct")
                router.create_job(number={jobs}, params={{"role": "clipboard"}})
                @router.task
                def run(ctx, role):
                    assert role == "clipboard"
                    return f"A/{{ctx.job_id}}"
            ''',
        },
    )
    return FileStorage(tmp_path)


def test_copy_reset_and_paste_restore_partial_jobs_and_exact_supporting_history(
    tmp_path, monkeypatch, capsys,
):
    storage = _project(tmp_path, monkeypatch)
    try:
        capsys.readouterr()
        assert cli.main([
            "run", "A", "sample", "80%", "--seed", "clipboard-80",
            "--runner", "direct",
        ]) == 0
        capsys.readouterr()
        statuses = [storage.get_job_status("A", job_id) for job_id in range(1, 101)]
        assert statuses.count("done") == 80
        assert statuses.count("queued") == 20
        saved_state = _component_result(storage, ("A",))
        assert saved_state["lifecycle"] == "sampled"
        saved_rows = _node_rows(storage.db_connection(), "A")
        saved_history = _history_rows(storage.db_connection(), ("A",))
        saved_files = _files(tmp_path / "node" / "A")

        assert cli.main(["copy", "A"]) == 0
        capsys.readouterr()
        snapshot_path = tmp_path / "clipboard" / "A" / ".mwf-node-state.sqlite3"
        assert snapshot_path.is_file()
        snapshot = sqlite3.connect(snapshot_path)
        snapshot.row_factory = sqlite3.Row
        try:
            assert _node_rows(snapshot, "A") == saved_rows
            assert _history_rows(snapshot, ("A",)) == saved_history
            assert snapshot.execute(
                "SELECT value FROM metadata WHERE key='project_id'"
            ).fetchone()[0] == storage.db_connection().execute(
                "SELECT value FROM metadata WHERE key='project_id'"
            ).fetchone()[0]
        finally:
            snapshot.close()

        monkeypatch.setattr("builtins.input", lambda _prompt: "reset")
        assert cli.main(["reset", "A"]) == 0
        capsys.readouterr()
        assert _node_rows(storage.db_connection(), "A") != saved_rows
        assert cli.main(["paste", "A"]) == 0
        capsys.readouterr()

        assert _node_rows(storage.db_connection(), "A") == saved_rows
        assert _files(tmp_path / "node" / "A") == saved_files
        restored = _component_result(storage, ("A",))
        assert restored == {
            **saved_state,
            "alignment_generation": restored["alignment_generation"],
        }
        assert restored["alignment_generation"] > saved_state["alignment_generation"]
        paste_decision = storage.db_connection().execute(
            "SELECT receipt.decision_json FROM clipboard_attempts AS attempt "
            "JOIN clipboard_receipts AS receipt ON receipt.operation_id=attempt.operation_id "
            "WHERE attempt.operation='paste' AND attempt.state='committed' "
            "ORDER BY attempt.started_at DESC LIMIT 1"
        ).fetchone()
        assert json.loads(paste_decision["decision_json"])["details"][
            "alignment_generation"
        ] == restored["alignment_generation"]
        after_history = _history_rows(storage.db_connection(), ("A",))
        assert after_history["owners"] == saved_history["owners"]
        assert after_history["sessions"] == saved_history["sessions"]
        assert after_history["results"][:len(saved_history["results"])] == saved_history["results"]
        assert after_history["results"][-1]["lifecycle"] == "sampled"
        assert (
            after_history["results"][-1]["stability"],
            after_history["results"][-1]["instability_origin"],
        ) == (saved_state["stability"], saved_state["instability_origin"])
        assert [storage.get_job_status("A", job_id) for job_id in range(1, 101)] == statuses
    finally:
        _close(storage)


def test_paste_refuses_a_snapshot_with_missing_successful_history_before_mutation(
    tmp_path, monkeypatch, capsys,
):
    storage = _project(tmp_path, monkeypatch, jobs=4)
    try:
        capsys.readouterr()
        assert cli.main([
            "run", "A", "sample", "50%", "--seed", "clipboard-damage",
            "--runner", "direct",
        ]) == 0
        capsys.readouterr()
        assert cli.main(["copy", "A"]) == 0
        capsys.readouterr()
        snapshot_path = tmp_path / "clipboard" / "A" / ".mwf-node-state.sqlite3"
        connection = sqlite3.connect(snapshot_path)
        try:
            connection.execute("PRAGMA foreign_keys=OFF")
            connection.execute("DELETE FROM component_successful_results")
            connection.commit()
        finally:
            connection.close()
        before_rows = _node_rows(storage.db_connection(), "A")
        before_files = _files(tmp_path / "node" / "A")
        before_attempts = storage.db_connection().execute(
            "SELECT COUNT(*) FROM clipboard_attempts"
        ).fetchone()[0]

        assert cli.main(["paste", "A"]) == 1
        assert "retained successful result" in capsys.readouterr().err.lower()
        assert _node_rows(storage.db_connection(), "A") == before_rows
        assert _files(tmp_path / "node" / "A") == before_files
        assert storage.db_connection().execute(
            "SELECT COUNT(*) FROM clipboard_attempts"
        ).fetchone()[0] == before_attempts
    finally:
        _close(storage)
