"""Fence only partial interrupt results and retain ordinarily ready transfers."""

from __future__ import annotations

import subprocess
import sys

import pytest

from micro_workflow_manager import cli
from micro_workflow_manager.storage import FileStorage
from tests.test_036_hoeflein_scheduling import make_project
from tests.test_090_component_session_settlement import _close
from tests.test_149_interrupt_declarations import _router_source
from tests.test_155_explicit_interrupt_execution import _command_environment, _start, _wait_until


@pytest.mark.parametrize("ready", [False, True], ids=["override", "ordinary-readiness"])
@pytest.mark.parametrize("selection,sampled", [
    ([], False),
    (["sample", "100%", "--seed", "full"], False),
    (["sample", "50%", "--seed", "partial"], True),
], ids=["full-component", "full-coverage-sample", "partial-sample"])
def test_fence_depends_on_effective_override_or_sampled_result(
    tmp_path, monkeypatch, capsys, ready, selection, sampled,
):
    make_project(
        tmp_path, monkeypatch, edges="EDGES = [('P', 'I')]",
        files={
            "P": _router_source("P", jobs=1),
            "I": _router_source("I", interrupt="True", jobs=2),
        },
    )
    if ready:
        assert cli.main(["run", "P", "--runner", "direct"]) == 0
    capsys.readouterr()

    assert cli.main(["run", "I", *selection, "--interrupt", "--runner", "direct"]) == 0

    storage = FileStorage(tmp_path)
    try:
        sessions = [session for session in storage.list_execution_sessions()
                    if session["session_kind"] == "interrupt"]
        session, = sessions
        assert session["status"] == "terminal" and session["outcome"] == "done"
        assert session["parent_session_ids"] == []
        state = storage.get_component_state(("I",))
        assert state["lifecycle"] == ("sampled" if sampled else "done")
        assert state["stability"] == ("stable" if ready else "unstable")
        assert state["instability_origin"] == (None if ready else session["session_id"])
        assert storage.get_component_state(("P",))["lifecycle"] == ("done" if ready else "queued")
        fences = list(storage.db_connection().execute(
            "SELECT source_session_id FROM post_interrupt_fences",
        ))
        assert [row[0] for row in fences] == ([session["session_id"]] if sampled or not ready else [])
        assert storage.get_component_holds(("I",)) == []
        assert storage.get_component_reservation(("I",)) is None
        owners = [storage.read_job_current_owner("I", job_id) for job_id in (1, 2)]
        assert sum(owner is not None for owner in owners) == (1 if sampled else 2)
        assert all(owner["session_id"] == session["session_id"] for owner in owners if owner)
    finally:
        _close(storage)


def test_ready_future_transfer_allows_main_to_continue_without_rerunning_child_result(
    tmp_path, monkeypatch, capsys,
):
    make_project(
        tmp_path, monkeypatch,
        edges="EDGES = [('R', 'A'), ('R', 'I'), ('I', 'D')]",
        files={
            "R": _router_source("R", jobs=1),
            "A": '''
                import time
                from micro_workflow_manager import NodeRouter
                router = NodeRouter('A', runner='direct')
                router.create_job(params={})
                @router.task
                def work(ctx):
                    root = ctx.system.storage.project_dir
                    (root / 'unrelated-active').write_text(ctx.execution_id, encoding='utf-8')
                    deadline = time.monotonic() + 45
                    while not (root / 'unrelated-release').exists():
                        assert time.monotonic() < deadline
                        ctx.checkpoint('unrelated branch remains active')
                        time.sleep(0.01)
                    return 'A'
            ''',
            "I": '''
                from micro_workflow_manager import NodeRouter
                router = NodeRouter('I', runner='direct', interrupt=True)
                router.create_job(params={})
                @router.task
                def work(ctx):
                    path = ctx.system.storage.project_dir / 'target-invocations'
                    with ctx.side_effects():
                        with path.open('a', encoding='utf-8') as stream:
                            stream.write(ctx.execution_id + '\\n')
                    return 'I'
            ''',
            "D": _router_source("D", jobs=1),
        },
    )
    capsys.readouterr()
    parent = _start(tmp_path, [
        "runfrom", "R", "--runner", "direct", "--interrupt-policy", "run-all",
    ])
    release = tmp_path / "unrelated-release"
    storage = None
    try:
        _wait_until(lambda: (tmp_path / "unrelated-active").exists() or parent.poll() is not None)
        assert parent.poll() is None, parent.communicate(timeout=5)
        storage = FileStorage(tmp_path)
        parent_owner = storage.read_job_current_owner("A", 1)["session_id"]
        assert storage.get_component_state(("R",))["lifecycle"] == "done"
        assert storage.get_job_status("I", 1) == "queued"
        assert not (tmp_path / "target-invocations").exists()

        child = subprocess.run(
            [sys.executable, "-m", "micro_workflow_manager", "run", "I", "--interrupt",
             "--runner", "direct"],
            cwd=tmp_path, env=_command_environment(), capture_output=True, text=True, timeout=25,
        )
        assert child.returncode == 0, child.stdout + child.stderr

        owner = storage.read_job_current_owner("I", 1)
        child_id = owner["session_id"]
        child_session = storage.get_execution_session(child_id)
        assert child_session["parent_session_ids"] == []
        assert child_session["status"] == "terminal" and child_session["outcome"] == "done"
        assert storage.get_component_reservation(("I",))["session_id"] == parent_owner
        assert storage.get_component_state(("I",))["stability"] == "stable"
        assert storage.db_connection().execute("SELECT COUNT(*) FROM post_interrupt_fences").fetchone()[0] == 0
        assert storage.read_job_current_owner("D", 1) is None
        control = storage.read_job_control("I", 1)
        events = storage.read_job_events("I", 1)
        assert (tmp_path / "target-invocations").read_text(encoding="utf-8").splitlines() == [owner["execution_id"]]

        release.write_text("continue", encoding="utf-8")
        stdout, stderr = parent.communicate(timeout=30)
        assert parent.returncode == 0, stdout + stderr
        assert storage.get_execution_session(parent_owner)["outcome"] == "done"
        assert storage.read_job_current_owner("I", 1) == owner
        assert storage.read_job_control("I", 1) == control
        assert storage.read_job_events("I", 1) == events
        assert storage.get_job_status("D", 1) == "done"
        assert storage.read_job_current_owner("D", 1)["session_id"] == parent_owner
        assert (tmp_path / "target-invocations").read_text(encoding="utf-8").splitlines() == [owner["execution_id"]]
        assert all(storage.get_component_reservation((node,)) is None for node in ("R", "A", "I", "D"))
    finally:
        release.write_text("continue", encoding="utf-8")
        try:
            parent.communicate(timeout=20)
        except subprocess.TimeoutExpired:
            parent.kill()
            parent.communicate(timeout=10)
            raise
        finally:
            if storage is not None:
                _close(storage)
