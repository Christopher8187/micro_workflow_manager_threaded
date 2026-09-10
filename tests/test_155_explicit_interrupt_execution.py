"""Public RED cases for cooperative explicit interruption and post-result fences."""

from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path

import pytest

from micro_workflow_manager.component_identity import encode_component_key
from micro_workflow_manager.models import Job
from micro_workflow_manager.storage import FileStorage
from tests.test_036_hoeflein_scheduling import make_project
from tests.test_090_component_session_settlement import _close


def _wait_until(predicate, *, timeout=30):
    deadline = time.monotonic() + timeout
    while not predicate():
        assert time.monotonic() < deadline
        time.sleep(0.02)


def _command_environment():
    return dict(os.environ, PYTHONPATH=str(Path(__file__).resolve().parents[1]))


def _start(tmp_path, arguments):
    return subprocess.Popen(
        [sys.executable, "-m", "micro_workflow_manager", *arguments],
        cwd=tmp_path,
        env=_command_environment(),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def _finish(process, release: Path):
    release.write_text("release", encoding="utf-8")
    try:
        return process.communicate(timeout=35)
    except subprocess.TimeoutExpired:
        process.kill()
        process.communicate(timeout=10)
        raise


def _make_cooperative_project(tmp_path, monkeypatch):
    make_project(
        tmp_path,
        monkeypatch,
        edges="EDGES = [('P', 'I'), ('I', 'D')]",
        runner="direct",
        files={
            "P": '''
                import time
                from micro_workflow_manager import NodeRouter
                router = NodeRouter('P', runner='direct', interrupt=True)
                router.create_job(params={})
                @router.task
                def work(ctx):
                    root = ctx.system.storage.project_dir
                    (root / 'parent-active').write_text(ctx.execution_id, encoding='utf-8')
                    deadline = time.monotonic() + 40
                    while not (root / 'target-completed').exists():
                        assert time.monotonic() < deadline
                        ctx.checkpoint('waiting for interrupt target')
                        time.sleep(0.01)
                    ctx.node('I').add(source='late-predecessor-publication')
                    (root / 'parent-resumed').write_text(ctx.execution_id, encoding='utf-8')
                    while not (root / 'parent-release').exists():
                        assert time.monotonic() < deadline
                        ctx.checkpoint('waiting for test release')
                        time.sleep(0.01)
                    return 'parent-done'
            ''',
            "I": '''
                import time
                from micro_workflow_manager import NodeRouter
                router = NodeRouter('I', runner='direct', interrupt=True)
                router.create_job(params={'source': 'initial'})
                @router.task
                def work(ctx, source):
                    root = ctx.system.storage.project_dir
                    (root / 'target-active').write_text(ctx.execution_id, encoding='utf-8')
                    deadline = time.monotonic() + 40
                    while not (root / 'target-release').exists():
                        assert time.monotonic() < deadline
                        ctx.checkpoint('target active')
                        time.sleep(0.01)
                    (root / 'target-completed').write_text(ctx.execution_id, encoding='utf-8')
                    return source
            ''',
            "D": '''
                from micro_workflow_manager import NodeRouter
                router = NodeRouter('D', runner='direct')
                router.create_job(params={})
                @router.task
                def work(ctx):
                    (ctx.system.storage.project_dir / 'descendant-ran').write_text('ran', encoding='utf-8')
                    return 'descendant'
            ''',
        },
    )


@pytest.mark.parametrize("parent_kind", ["main", "interrupt"])
def test_main_or_nested_interrupt_pauses_direct_predecessor_and_returns_remaining_scope(
    tmp_path, monkeypatch, capsys, parent_kind,
):
    _make_cooperative_project(tmp_path, monkeypatch)
    capsys.readouterr()
    parent_arguments = ["runfrom", "P", "--runner", "direct", "--interrupt-policy", "run-all"]
    if parent_kind == "interrupt":
        parent_arguments.insert(2, "--interrupt")
    parent = _start(tmp_path, parent_arguments)
    child = None
    parent_release = tmp_path / "parent-release"
    target_release = tmp_path / "target-release"
    try:
        _wait_until(lambda: (tmp_path / "parent-active").exists() or parent.poll() is not None)
        assert parent.poll() is None, parent.communicate(timeout=5)
        storage = FileStorage(tmp_path)
        parent_owner = storage.read_job_current_owner("P", 1)
        parent_session = storage.get_execution_session(parent_owner["session_id"])
        assert parent_session["session_kind"] == parent_kind
        assert storage.get_component_reservation(("I",))["session_id"] == parent_session["session_id"]

        child = _start(tmp_path, ["run", "I", "--interrupt", "--runner", "direct"])
        _wait_until(lambda: (tmp_path / "target-active").exists() or child.poll() is not None)
        assert child.poll() is None, child.communicate(timeout=5)
        target_owner = storage.read_job_current_owner("I", 1)
        child_session = storage.get_execution_session(target_owner["session_id"])
        assert child_session["session_kind"] == "interrupt"
        assert child_session["parent_session_ids"] == [parent_session["session_id"]]
        assert child_session["selected_components"] == [("I",)]
        assert storage.get_component_reservation(("P",))["session_id"] == parent_session["session_id"]
        assert storage.get_component_reservation(("I",))["session_id"] == child_session["session_id"]
        assert storage.get_component_reservation(("D",))["session_id"] == parent_session["session_id"]

        pause = dict(storage.db_connection().execute(
            "SELECT child_session_id, owner_session_id, component_key, state "
            "FROM interrupt_pause_requests WHERE child_session_id=?",
            (child_session["session_id"],),
        ).fetchone())
        assert pause == {
            "child_session_id": child_session["session_id"],
            "owner_session_id": parent_session["session_id"],
            "component_key": encode_component_key(("P",)),
            "state": "acknowledged",
        }
        paused = dict(storage.db_connection().execute(
            "SELECT child_session_id, execution_id, generation "
            "FROM interrupt_paused_executions WHERE child_session_id=?",
            (child_session["session_id"],),
        ).fetchone())
        assert paused == {
            "child_session_id": child_session["session_id"],
            "execution_id": parent_owner["execution_id"],
            "generation": parent_owner["generation"],
        }
        assert storage.get_component_holds(("I",)) == [{
            "session_id": child_session["session_id"],
            "count": 1,
            "heartbeat_at": child_session["heartbeat_at"],
        }]

        # An observer without the holding child execution cannot publish into I.
        input_path = storage.input_file("I", 99)
        with pytest.raises(RuntimeError, match="hold|interrupt"):
            storage.create_job(Job(node_name="I", job_id=99, params={"source": "unowned"}))
        assert not input_path.exists()
        assert 99 not in storage.list_job_ids("I")

        target_release.write_text("release", encoding="utf-8")
        child_stdout, child_stderr = child.communicate(timeout=35)
        assert child.returncode == 0, child_stdout + child_stderr
        _wait_until(lambda: (tmp_path / "parent-resumed").exists() or parent.poll() is not None)
        assert parent.poll() is None, parent.communicate(timeout=5)

        assert storage.get_component_holds(("I",)) == []
        assert storage.get_component_reservation(("I",))["session_id"] == parent_session["session_id"]
        assert storage.db_connection().execute(
            "SELECT state FROM interrupt_scope_transfers "
            "WHERE child_session_id=? AND component_key=?",
            (child_session["session_id"], encode_component_key(("I",))),
        ).fetchone()[0] == "returned"
        assert storage.get_execution_session(child_session["session_id"])["outcome"] == "done"
        assert storage.get_job_status("I", 1) == "done"
        assert storage.read_job_current_owner("I", 1)["session_id"] == child_session["session_id"]
        assert storage.get_job_status("I", 2) == "queued"
        assert storage.read_job_current_owner("I", 2) is None
        assert storage.get_component_state(("I",))["misaligned"] is True
        assert not (tmp_path / "descendant-ran").exists()
        fence = dict(storage.db_connection().execute(
            "SELECT component_key, source_session_id FROM post_interrupt_fences "
            "WHERE source_session_id=?",
            (child_session["session_id"],),
        ).fetchone())
        assert fence == {
            "component_key": encode_component_key(("I",)),
            "source_session_id": child_session["session_id"],
        }

        parent_stdout, parent_stderr = _finish(parent, parent_release)
        assert parent.returncode == 0, parent_stdout + parent_stderr
        assert storage.get_execution_session(parent_session["session_id"])["status"] == "terminal"
        assert storage.get_component_reservation(("P",)) is None
        assert storage.get_component_reservation(("I",)) is None
        assert storage.get_component_reservation(("D",)) is None
        assert storage.get_job_status("I", 2) == "queued"
        assert storage.read_job_current_owner("I", 2) is None
        assert storage.get_job_status("D", 1) == "queued"
        assert storage.read_job_current_owner("D", 1) is None
        assert not (tmp_path / "descendant-ran").exists()
        assert storage.db_connection().execute(
            "SELECT COUNT(*) FROM session_fence_authorizations "
            "WHERE session_id=? AND source_session_id=?",
            (parent_session["session_id"], child_session["session_id"]),
        ).fetchone()[0] == 0
        _close(storage)
    finally:
        target_release.write_text("release", encoding="utf-8")
        (tmp_path / "target-completed").touch(exist_ok=True)
        parent_release.write_text("release", encoding="utf-8")
        for process in (child, parent):
            if process is not None and process.poll() is None:
                try:
                    process.communicate(timeout=20)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.communicate(timeout=10)
                    raise


def test_active_target_claim_refuses_public_interrupt_before_child_or_file_mutation(
    tmp_path, monkeypatch, capsys,
):
    make_project(
        tmp_path,
        monkeypatch,
        edges="EDGES = [('I', 'I')]",
        runner="direct",
        files={
            "I": '''
                import time
                from micro_workflow_manager import NodeRouter
                router = NodeRouter('I', runner='direct', interrupt=True)
                router.create_job(params={'value': 'kept'})
                @router.task
                def work(ctx, value):
                    root = ctx.system.storage.project_dir
                    (root / 'active-target').write_text(ctx.execution_id, encoding='utf-8')
                    deadline = time.monotonic() + 30
                    while not (root / 'active-release').exists():
                        assert time.monotonic() < deadline
                        ctx.checkpoint('active target')
                        time.sleep(0.01)
                    return value
            ''',
        },
    )
    capsys.readouterr()
    parent = _start(tmp_path, ["run", "I", "--runner", "direct", "--interrupt-policy", "run-all"])
    release = tmp_path / "active-release"
    try:
        _wait_until(lambda: (tmp_path / "active-target").exists() or parent.poll() is not None)
        assert parent.poll() is None, parent.communicate(timeout=5)
        storage = FileStorage(tmp_path)
        owner = storage.read_job_current_owner("I", 1)
        before_control = storage.read_job_control("I", 1)
        before_events = storage.read_job_events("I", 1)
        before_input = storage.input_file("I", 1).read_bytes()
        before_sessions = storage.list_execution_sessions()
        before_reservation = storage.get_component_reservation(("I",))

        refused = subprocess.run(
            [sys.executable, "-m", "micro_workflow_manager", "run", "I", "--interrupt"],
            cwd=tmp_path,
            env=_command_environment(),
            capture_output=True,
            text=True,
            timeout=25,
        )
        assert refused.returncode == 1
        message = (refused.stdout + refused.stderr).lower()
        assert "active" in message and "i/1" in message
        assert owner == storage.read_job_current_owner("I", 1)
        assert before_control == storage.read_job_control("I", 1)
        assert before_events == storage.read_job_events("I", 1)
        assert before_input == storage.input_file("I", 1).read_bytes()
        after_sessions = storage.list_execution_sessions()
        assert len(before_sessions) == len(after_sessions) == 1
        before_parent, after_parent = before_sessions[0], after_sessions[0]
        assert {
            key: value for key, value in after_parent.items() if key != "heartbeat_at"
        } == {
            key: value for key, value in before_parent.items() if key != "heartbeat_at"
        }
        assert after_parent["heartbeat_at"] >= before_parent["heartbeat_at"]
        assert storage.get_component_reservation(("I",)) == before_reservation
        assert len([session for session in storage.list_execution_sessions()
                    if session["session_kind"] == "interrupt"]) == 0

        stdout, stderr = _finish(parent, release)
        assert parent.returncode == 0, stdout + stderr
        assert storage.read_job_current_owner("I", 1)["session_id"] == owner["session_id"]
        _close(storage)
    finally:
        release.write_text("release", encoding="utf-8")
        if parent.poll() is None:
            try:
                parent.communicate(timeout=20)
            except subprocess.TimeoutExpired:
                parent.kill()
                parent.communicate(timeout=10)
                raise
