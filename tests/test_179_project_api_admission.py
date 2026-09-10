"""Project-wide API permits compose with cooperative interrupt pauses."""

from __future__ import annotations

import subprocess
import time

from micro_workflow_manager.storage import FileStorage
from tests.test_036_hoeflein_scheduling import make_project
from tests.test_090_component_session_settlement import _close
from tests.test_155_explicit_interrupt_execution import _start, _wait_until


def _permit_identities(storage):
    return [
        tuple(row) for row in storage.db_connection().execute(
            "SELECT session_id, node_name, job_id, generation, execution_id "
            "FROM api_execution_permits ORDER BY session_id, node_name, job_id"
        )
    ]


def test_cap_one_parent_yields_to_interrupt_child_then_reacquires(
    tmp_path, monkeypatch,
):
    make_project(
        tmp_path,
        monkeypatch,
        edges="EDGES = [('P', 'I')]",
        runner="api",
        files={
            "P": '''
                import time
                from micro_workflow_manager import NodeRouter
                router = NodeRouter('P', runner='api')
                router.create_job(params={})
                @router.task
                def work(ctx):
                    root = ctx.system.storage.project_dir
                    (root / 'parent-active').write_text(ctx.execution_id, encoding='utf-8')
                    deadline = time.monotonic() + 40
                    while not (root / 'child-completed').exists():
                        assert time.monotonic() < deadline
                        ctx.checkpoint('parent waiting for interrupt child')
                        time.sleep(0.01)
                    (root / 'parent-resumed').write_text(ctx.execution_id, encoding='utf-8')
                    while not (root / 'parent-release').exists():
                        assert time.monotonic() < deadline
                        ctx.checkpoint('parent resumed with capacity')
                        time.sleep(0.01)
                    return 'parent complete'
            ''',
            "I": '''
                import time
                from micro_workflow_manager import NodeRouter
                router = NodeRouter('I', runner='api', interrupt=True)
                router.create_job(params={})
                @router.task
                def work(ctx):
                    root = ctx.system.storage.project_dir
                    (root / 'child-active').write_text(ctx.execution_id, encoding='utf-8')
                    deadline = time.monotonic() + 40
                    while not (root / 'child-release').exists():
                        assert time.monotonic() < deadline
                        ctx.checkpoint('interrupt child active')
                        time.sleep(0.01)
                    (root / 'child-completed').write_text(ctx.execution_id, encoding='utf-8')
                    return 'child complete'
            ''',
        },
    )
    storage = FileStorage(tmp_path)
    storage.set_api_total_limit(1)
    parent = child = None
    try:
        parent = _start(tmp_path, [
            "run", "P", "--runner", "api", "--interrupt-policy", "run-all",
        ])
        _wait_until(lambda: (tmp_path / "parent-active").exists() or parent.poll() is not None)
        assert parent.poll() is None, parent.communicate(timeout=5)
        parent_owner = storage.read_job_current_owner("P", 1)
        assert _permit_identities(storage) == [(
            parent_owner["session_id"], "P", 1, parent_owner["generation"],
            parent_owner["execution_id"],
        )]

        child = _start(tmp_path, ["run", "I", "--interrupt", "--runner", "api"])
        _wait_until(lambda: (tmp_path / "child-active").exists() or child.poll() is not None)
        assert child.poll() is None, child.communicate(timeout=5)
        child_owner = storage.read_job_current_owner("I", 1)
        assert child_owner["session_id"] != parent_owner["session_id"]
        assert _permit_identities(storage) == [(
            child_owner["session_id"], "I", 1, child_owner["generation"],
            child_owner["execution_id"],
        )]
        assert not (tmp_path / "parent-resumed").exists()
        pause = storage.db_connection().execute(
            "SELECT state FROM interrupt_pause_requests "
            "WHERE child_session_id=? AND owner_session_id=?",
            (child_owner["session_id"], parent_owner["session_id"]),
        ).fetchone()
        assert pause is not None and pause["state"] == "acknowledged"

        (tmp_path / "child-release").touch()
        child_stdout, child_stderr = child.communicate(timeout=35)
        assert child.returncode == 0, child_stdout + child_stderr
        _wait_until(lambda: (tmp_path / "parent-resumed").exists() or parent.poll() is not None)
        assert parent.poll() is None, parent.communicate(timeout=5)
        assert (tmp_path / "parent-resumed").read_text(encoding="utf-8") == (
            parent_owner["execution_id"]
        )
        assert _permit_identities(storage) == [(
            parent_owner["session_id"], "P", 1, parent_owner["generation"],
            parent_owner["execution_id"],
        )]

        (tmp_path / "parent-release").touch()
        parent_stdout, parent_stderr = parent.communicate(timeout=35)
        assert parent.returncode == 0, parent_stdout + parent_stderr
        assert _permit_identities(storage) == []
        assert storage.get_job_status("P", 1) == "done"
        assert storage.get_job_status("I", 1) == "done"
        assert storage.read_job_current_owner("P", 1) == parent_owner
        assert storage.read_job_current_owner("I", 1) == child_owner
        assert storage.db_connection().execute(
            "SELECT COUNT(*) FROM job_events WHERE event='task_started' "
            "AND ((node_name='P' AND job_id=1) OR (node_name='I' AND job_id=1))"
        ).fetchone()[0] == 2
    finally:
        (tmp_path / "child-release").touch(exist_ok=True)
        (tmp_path / "child-completed").touch(exist_ok=True)
        (tmp_path / "parent-release").touch(exist_ok=True)
        for process in (child, parent):
            if process is not None and process.poll() is None:
                try:
                    process.communicate(timeout=20)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.communicate(timeout=10)
                    raise
        _close(storage)


def test_cap_waiter_acknowledges_pause_without_owning_capacity(
    tmp_path, monkeypatch,
):
    make_project(
        tmp_path,
        monkeypatch,
        edges="EDGES = [('U', 'U'), ('P', 'I')]",
        runner="api",
        files={
            "U": '''
                import time
                from micro_workflow_manager import NodeRouter
                router = NodeRouter('U', runner='api', interrupt=True)
                router.create_job(params={})
                @router.task
                def work(ctx):
                    root = ctx.system.storage.project_dir
                    (root / 'unrelated-active').write_text(ctx.execution_id, encoding='utf-8')
                    deadline = time.monotonic() + 40
                    while not (root / 'unrelated-release').exists():
                        assert time.monotonic() < deadline
                        ctx.checkpoint('unrelated interrupt owns capacity')
                        time.sleep(0.01)
                    return 'unrelated complete'
            ''',
            "P": '''
                import time
                from micro_workflow_manager import NodeRouter
                router = NodeRouter('P', runner='api')
                router.create_job(params={})
                @router.task
                def work(ctx):
                    root = ctx.system.storage.project_dir
                    (root / 'waiting-parent-active').write_text(ctx.execution_id, encoding='utf-8')
                    deadline = time.monotonic() + 40
                    while not (root / 'waiting-parent-release').exists():
                        assert time.monotonic() < deadline
                        ctx.checkpoint('parent acquired after child')
                        time.sleep(0.01)
                    return 'parent complete'
            ''',
            "I": '''
                import time
                from micro_workflow_manager import NodeRouter
                router = NodeRouter('I', runner='api', interrupt=True)
                router.create_job(params={})
                @router.task
                def work(ctx):
                    root = ctx.system.storage.project_dir
                    (root / 'waiting-child-active').write_text(ctx.execution_id, encoding='utf-8')
                    deadline = time.monotonic() + 40
                    while not (root / 'waiting-child-release').exists():
                        assert time.monotonic() < deadline
                        ctx.checkpoint('child acquired yielded capacity')
                        time.sleep(0.01)
                    return 'child complete'
            ''',
        },
    )
    storage = FileStorage(tmp_path)
    storage.set_api_total_limit(1)
    unrelated = parent = child = None
    try:
        unrelated = _start(tmp_path, ["run", "U", "--interrupt", "--runner", "api"])
        _wait_until(
            lambda: (tmp_path / "unrelated-active").exists()
            or unrelated.poll() is not None
        )
        assert unrelated.poll() is None, unrelated.communicate(timeout=5)
        unrelated_owner = storage.read_job_current_owner("U", 1)
        assert _permit_identities(storage) == [(
            unrelated_owner["session_id"], "U", 1,
            unrelated_owner["generation"], unrelated_owner["execution_id"],
        )]

        parent = _start(tmp_path, [
            "run", "P", "--runner", "api", "--interrupt-policy", "run-all",
        ])

        def read_parent_owner():
            return storage.read_job_current_owner("P", 1)

        _wait_until(lambda: read_parent_owner() is not None or parent.poll() is not None)
        assert parent.poll() is None, parent.communicate(timeout=5)
        parent_owner = read_parent_owner()
        assert parent_owner is not None
        assert not (tmp_path / "waiting-parent-active").exists()

        child = _start(tmp_path, ["run", "I", "--interrupt", "--runner", "api"])

        def acknowledged_pause():
            return storage.db_connection().execute(
                "SELECT child_session_id, owner_session_id, state "
                "FROM interrupt_pause_requests WHERE owner_session_id=?",
                (parent_owner["session_id"],),
            ).fetchone()

        _wait_until(lambda: acknowledged_pause() is not None or child.poll() is not None)
        assert child.poll() is None, child.communicate(timeout=5)

        def pause_is_acknowledged():
            row = acknowledged_pause()
            return row is not None and row["state"] == "acknowledged"

        _wait_until(
            lambda: pause_is_acknowledged() or child.poll() is not None
        )
        assert child.poll() is None, child.communicate(timeout=5)
        pause = acknowledged_pause()
        assert pause["state"] == "acknowledged"
        assert pause["owner_session_id"] == parent_owner["session_id"]
        assert _permit_identities(storage) == [(
            unrelated_owner["session_id"], "U", 1,
            unrelated_owner["generation"], unrelated_owner["execution_id"],
        )]
        assert not (tmp_path / "waiting-child-active").exists()
        assert not (tmp_path / "waiting-parent-active").exists()

        (tmp_path / "unrelated-release").touch()
        unrelated_stdout, unrelated_stderr = unrelated.communicate(timeout=35)
        assert unrelated.returncode == 0, unrelated_stdout + unrelated_stderr
        _wait_until(
            lambda: (tmp_path / "waiting-child-active").exists()
            or child.poll() is not None
        )
        assert child.poll() is None, child.communicate(timeout=5)
        child_owner = storage.read_job_current_owner("I", 1)
        assert child_owner["session_id"] == pause["child_session_id"]
        assert _permit_identities(storage) == [(
            child_owner["session_id"], "I", 1,
            child_owner["generation"], child_owner["execution_id"],
        )]
        assert not (tmp_path / "waiting-parent-active").exists()

        (tmp_path / "waiting-child-release").touch()
        child_stdout, child_stderr = child.communicate(timeout=35)
        assert child.returncode == 0, child_stdout + child_stderr
        _wait_until(
            lambda: (tmp_path / "waiting-parent-active").exists()
            or parent.poll() is not None
        )
        assert parent.poll() is None, parent.communicate(timeout=5)
        assert (tmp_path / "waiting-parent-active").read_text(encoding="utf-8") == (
            parent_owner["execution_id"]
        )
        assert _permit_identities(storage) == [(
            parent_owner["session_id"], "P", 1,
            parent_owner["generation"], parent_owner["execution_id"],
        )]

        (tmp_path / "waiting-parent-release").touch()
        parent_stdout, parent_stderr = parent.communicate(timeout=35)
        assert parent.returncode == 0, parent_stdout + parent_stderr
        assert _permit_identities(storage) == []
    finally:
        for name in (
            "unrelated-release", "waiting-child-release", "waiting-parent-release",
        ):
            (tmp_path / name).touch(exist_ok=True)
        for process in (child, parent, unrelated):
            if process is not None and process.poll() is None:
                try:
                    process.communicate(timeout=20)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.communicate(timeout=10)
                    raise
        _close(storage)
