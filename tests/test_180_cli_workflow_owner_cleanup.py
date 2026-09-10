"""CLI-created writable workflows release their exact SQLite ownership."""

from __future__ import annotations

from micro_workflow_manager import cli
from tests.test_036_hoeflein_scheduling import make_project


def _source(*, failing=False):
    body = "raise RuntimeError('handler stopped')" if failing else "return ctx.job_id"
    return f'''
        from micro_workflow_manager import NodeRouter
        router = NodeRouter("A", runner="direct")
        router.create_job(params={{}})
        @router.task
        def work(ctx):
            {body}
    '''


def _make_project(tmp_path, monkeypatch, *, failing=False):
    make_project(
        tmp_path,
        monkeypatch,
        edges="EDGES = [('A', 'A')]",
        runner="direct",
        files={"A": _source(failing=failing)},
    )


def _assert_closed_database(root):
    assert not (root / ".mwf" / "state.sqlite3-wal").exists()
    assert not (root / ".mwf" / "state.sqlite3-shm").exists()


def test_cli_closes_loaded_workflow_after_success_and_post_load_refusal(
    tmp_path, monkeypatch,
):
    _make_project(tmp_path, monkeypatch)
    _assert_closed_database(tmp_path)

    assert cli.main(["inspect", "A"]) == 0
    _assert_closed_database(tmp_path)

    # The unknown node is checked only after a writable workflow is loaded.
    assert cli.main(["inspect", "Missing"]) == 1
    _assert_closed_database(tmp_path)


def test_load_workflow_closes_its_storage_when_user_source_import_fails(
    tmp_path, monkeypatch,
):
    _make_project(tmp_path, monkeypatch)
    behavior = tmp_path / "src" / "node_behavior" / "A.py"
    behavior.write_text("raise RuntimeError('node import stopped')\n", encoding="utf-8")

    assert cli.main(["inspect", "A"]) == 1
    _assert_closed_database(tmp_path)


def test_sample_execution_closes_loaded_workflow_after_success(
    tmp_path, monkeypatch,
):
    _make_project(tmp_path, monkeypatch)

    assert cli.main([
        "run", "A", "sample", "100%", "--seed", "owner-success",
        "--runner", "direct",
    ]) == 0
    _assert_closed_database(tmp_path)


def test_sample_execution_closes_loaded_workflow_after_handler_failure(
    tmp_path, monkeypatch,
):
    _make_project(tmp_path, monkeypatch, failing=True)

    assert cli.main([
        "run", "A", "sample", "100%", "--seed", "owner-failure",
        "--runner", "direct",
    ]) == 1
    _assert_closed_database(tmp_path)
