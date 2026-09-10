"""Diagnostics distinguish local API requests from the shared project limit."""

from __future__ import annotations

import json
import subprocess
import textwrap

from micro_workflow_manager import cli
from micro_workflow_manager.storage import FileStorage
from tests.test_036_hoeflein_scheduling import make_project
from tests.test_090_component_session_settlement import _close
from tests.test_155_explicit_interrupt_execution import _start, _wait_until


def _api_node(name):
    return textwrap.dedent(f"""
        import time
        from pathlib import Path
        from micro_workflow_manager import NodeRouter

        router = NodeRouter({name!r}, runner="api", max_threads=4, interrupt=True)
        router.create_job()

        @router.task
        def work(ctx):
            root = Path(ctx.system.storage.project_dir)
            (root / {name + '-active'!r}).write_text(ctx.execution_id, encoding="utf-8")
            deadline = time.monotonic() + 40
            while not (root / {name + '-release'!r}).exists():
                assert time.monotonic() < deadline
                ctx.checkpoint({name + ' waiting'!r})
                time.sleep(0.01)
            return {name + ' complete'!r}
    """)


def _finish(process):
    if process is None:
        return
    try:
        process.communicate(timeout=40)
    except subprocess.TimeoutExpired:
        process.kill()
        process.communicate(timeout=10)


def test_live_disjoint_api_sessions_report_requests_and_one_shared_capacity(
    tmp_path, monkeypatch, capsys,
):
    make_project(
        tmp_path, monkeypatch,
        edges="EDGES = [('A', 'Z'), ('B', 'Z')]",
        runner="api",
        files={
            "A": _api_node("A"),
            "B": _api_node("B"),
            "Z": '''
                from micro_workflow_manager import NodeRouter
                router = NodeRouter("Z", runner="direct")
                @router.task
                def work(ctx): return "unused"
            ''',
        },
    )
    storage = FileStorage(tmp_path)
    storage.set_api_total_limit(1)
    first = second = None
    try:
        first = _start(tmp_path, ["run", "A", "--interrupt", "--runner", "api"])
        _wait_until(lambda: (tmp_path / "A-active").exists() or first.poll() is not None)
        assert first.poll() is None, first.communicate(timeout=5)

        second = _start(tmp_path, ["run", "B", "--interrupt", "--runner", "api"])
        _wait_until(lambda: (
            len([session for session in storage.list_execution_sessions()
                 if session["status"] == "running"]) == 2
            and storage.get_component_state(("B",))["lifecycle"] == "running"
            and storage.get_job_status("B", 1) == "running"
        ) or second.poll() is not None)
        assert second.poll() is None, second.communicate(timeout=5)
        assert not (tmp_path / "B-active").exists()
        assert len(storage.read_api_execution_permits()) == 1

        capsys.readouterr()
        assert cli.main(["monitor", "--once", "--json"]) == 0
        monitor = json.loads(capsys.readouterr().out)
        assert monitor["api_runtime"] == {
            "mode": "cooperative",
            "aggregate_limit": 1,
            "requested_capacity": 8,
            "active_capacity": 1,
            "running": 2,
            "queued": 0,
            "completed_last_60_seconds": 0,
            "nodes": 2,
        }
        live_rows = {row["node"]: row for row in monitor["nodes"] if row["node"] in {"A", "B"}}
        assert {row["requested_max_threads"] for row in live_rows.values()} == {4}
        assert {row["max_parallel_jobs"] for row in live_rows.values()} == {4}

        assert cli.main(["inspect", "A"]) == 0
        inspected = capsys.readouterr().out
        assert "requested max_threads: 4" in inspected
        assert "project-wide API limit: 1" in inspected
        assert "effective max_threads" not in inspected

        assert cli.main(["threads", "A"]) == 0
        thread_status = capsys.readouterr().out
        assert "requested max_threads: 4" in thread_status
        assert "project-wide API limit: 1" in thread_status
        assert "effective max_threads" not in thread_status

        assert cli.main(["threads"]) == 0
        thread_listing = capsys.readouterr().out
        assert "Project-wide API limit: 1" in thread_listing
        assert "requested" in thread_listing
        assert "effective" not in thread_listing

        assert cli.main(["top", "--once", "--json"]) == 0
        top = json.loads(capsys.readouterr().out)
        assert top["api_total_limit"] == 1
        top_rows = {row["node"]: row for row in top["nodes"]}
        assert top_rows["A"]["requested_limit"] == 4
        assert top_rows["B"]["requested_limit"] == 4
        assert "effective_limit" not in top_rows["A"]

        assert cli.main(["top", "--once"]) == 0
        rendered = capsys.readouterr().out
        assert "REQUEST" in rendered
        assert "project API limit=1" in rendered
    finally:
        (tmp_path / "A-release").touch()
        (tmp_path / "B-release").touch()
        _finish(second)
        _finish(first)
        _close(storage)
