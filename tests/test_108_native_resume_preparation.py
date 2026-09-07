from __future__ import annotations

from micro_workflow_manager import cli
from micro_workflow_manager.cli.project import load_workflow
from micro_workflow_manager.storage import FileStorage
from tests.test_036_hoeflein_scheduling import make_project
from tests.test_090_component_session_settlement import _close, _rows
from tests.test_093_native_cli_readiness import _node_files


def _observe_recovery(monkeypatch):
    calls = []
    original = FileStorage.reconcile_terminal_outputs

    def observed(storage, nodes):
        calls.append(tuple(nodes))
        return original(storage, nodes)

    monkeypatch.setattr(FileStorage, "reconcile_terminal_outputs", observed)
    return calls


def test_resumefrom_refuses_misaligned_descendant_before_recovery_or_mutation(
    tmp_path, monkeypatch, capsys,
):
    make_project(
        tmp_path,
        monkeypatch,
        edges="EDGES = [('A', 'B')]",
        files={
            "A": '''
                from micro_workflow_manager import NodeRouter
                router = NodeRouter("A")
                router.create_job(number=1)
                @router.task
                def run(ctx):
                    ctx.write_output(f"a-{ctx.job_id}.txt", str(ctx.job_id))
                    ctx.node("B").write_input(f"a-{ctx.job_id}.txt", str(ctx.job_id))
                    ctx.node("B").add(value=ctx.job_id)
                    return ctx.job_id
            ''',
            "B": '''
                from micro_workflow_manager import NodeRouter
                router = NodeRouter("B")
                @router.task
                def run(ctx, value):
                    ctx.trace("receiver completed", value=value)
                    ctx.write_output(f"b-{value}.txt", str(value))
                    return value
            ''',
        },
    )
    capsys.readouterr()
    assert cli.main(["runfrom", "A"]) == 0
    capsys.readouterr()

    workflow = load_workflow(tmp_path)
    try:
        workflow.start("A", job_id=2)
        assert workflow.run_job("A", 2) == 2
    finally:
        _close(workflow.storage)

    storage = FileStorage(tmp_path)
    try:
        descendant = storage.get_component_state(("B",))
        assert descendant["lifecycle"] == "done"
        assert descendant["misaligned"] is True
        assert storage.get_job_status("B", 1) == "done"
        assert storage.get_job_status("B", 2) == "queued"
        before_rows = _rows(storage)
        before_files = _node_files(tmp_path)
        recovery_calls = _observe_recovery(monkeypatch)
        capsys.readouterr()

        assert cli.main(["resumefrom", "A"]) == 1

        output = capsys.readouterr()
        assert "misaligned" in (output.out + output.err).lower()
        assert recovery_calls == []
        assert _rows(storage) == before_rows
        assert _node_files(tmp_path) == before_files
        assert storage.get_component_reservation(("A",)) is None
        assert storage.get_component_reservation(("B",)) is None
    finally:
        _close(storage)


def test_resumefrom_refuses_incomplete_external_parent_before_recovery_or_mutation(
    tmp_path, monkeypatch, capsys,
):
    make_project(
        tmp_path,
        monkeypatch,
        edges="EDGES = [('P', 'A'), ('A', 'B')]",
        files={
            "P": '''
                from micro_workflow_manager import NodeRouter
                router = NodeRouter("P")
                router.create_job(number=1)
                @router.task
                def run(ctx):
                    ctx.write_output("p.txt", "P")
                    return "P"
            ''',
            "A": '''
                from micro_workflow_manager import NodeRouter
                router = NodeRouter("A")
                router.create_job(number=1)
                @router.task
                def run(ctx):
                    ctx.trace("successful parent work")
                    ctx.write_output("a.txt", "A")
                    return "A"
            ''',
            "B": '''
                from micro_workflow_manager import NodeRouter
                router = NodeRouter("B")
                router.create_job(number=1)
                @router.task
                def run(ctx):
                    ctx.trace("failed descendant work")
                    ctx.write_output("b.txt", "retain failed output")
                    raise RuntimeError("expected B failure")
            ''',
        },
    )
    capsys.readouterr()
    assert cli.main(["runfrom", "P"]) == 1
    capsys.readouterr()
    assert cli.main(["reset", "P", "--yes"]) == 0
    capsys.readouterr()

    storage = FileStorage(tmp_path)
    try:
        assert storage.get_component_state(("P",))["lifecycle"] == "queued"
        assert storage.get_component_state(("A",))["lifecycle"] == "done"
        assert storage.get_component_state(("B",))["lifecycle"] == "failed"
        assert storage.get_job_status("A", 1) == "done"
        assert storage.get_job_status("B", 1) == "failed"
        before_rows = _rows(storage)
        before_files = _node_files(tmp_path)
        recovery_calls = _observe_recovery(monkeypatch)

        assert cli.main(["resumefrom", "A"]) == 1

        output = capsys.readouterr()
        assert "P" in output.out + output.err
        assert "incomplete" in (output.out + output.err).lower()
        assert recovery_calls == []
        assert _rows(storage) == before_rows
        assert _node_files(tmp_path) == before_files
        assert storage.get_component_reservation(("A",)) is None
        assert storage.get_component_reservation(("B",)) is None
    finally:
        _close(storage)


def test_resume_requeue_and_component_transition_roll_back_together(
    tmp_path, monkeypatch, capsys,
):
    make_project(
        tmp_path,
        monkeypatch,
        edges="EDGES = [('A', 'A')]",
        runner="threaded",
        files={
            "A": '''
                from threading import Barrier
                from micro_workflow_manager import NodeRouter
                barrier = Barrier(3)
                router = NodeRouter("A", runner="threaded", max_threads=3)
                router.create_job(number=3)
                @router.task
                def run(ctx):
                    ctx.trace("attempt reached handler", job_id=ctx.job_id)
                    ctx.write_output(f"job-{ctx.job_id}.txt", str(ctx.job_id))
                    barrier.wait(timeout=10)
                    if ctx.job_id in (1, 2):
                        raise RuntimeError(f"expected failure {ctx.job_id}")
                    return ctx.job_id
            ''',
        },
    )
    capsys.readouterr()
    assert cli.main(["run", "A"]) == 1
    capsys.readouterr()

    storage = FileStorage(tmp_path)
    try:
        assert [storage.get_job_status("A", job_id) for job_id in (1, 2, 3)] == [
            "failed", "failed", "done",
        ]
        component = storage.get_component_state(("A",))
        assert component["lifecycle"] == "failed"
        reserve = FileStorage.reserve_execution_components

        def inject_after_admission(self, *args, **kwargs):
            result = reserve(self, *args, **kwargs)
            self.submit_db_mutation(lambda connection: connection.execute(
                "CREATE TRIGGER fail_second_resume BEFORE UPDATE OF status ON jobs "
                "WHEN OLD.node_name='A' AND OLD.job_id=2 AND OLD.status='failed' "
                "AND NEW.status='queued' BEGIN "
                "UPDATE component_states SET misaligned=1 WHERE component_key='[\"A\"]'; "
                "SELECT RAISE(ABORT, 'injected second resume update failure'); END"
            ))
            return result

        monkeypatch.setattr(FileStorage, 'reserve_execution_components', inject_after_admission)
        before_rows = _rows(storage)
        before_files = _node_files(tmp_path)
        before_sessions = {session["session_id"] for session in storage.list_execution_sessions()}

        assert cli.main(["resume", "A"]) == 1

        error = capsys.readouterr().err
        assert "injected second resume update failure" in error
        after_rows = _rows(storage)
        for table in (
            "jobs", "job_instances", "job_events", "job_execution_owners",
            "component_states", "nodes",
        ):
            assert after_rows[table] == before_rows[table]
        assert _node_files(tmp_path) == before_files
        assert storage.get_component_state(("A",)) == component
        assert storage.get_job_status("A", 3) == "done"
        assert storage.output_file("A", 3).exists()
        sessions = [
            session for session in storage.list_execution_sessions()
            if session["session_id"] not in before_sessions
        ]
        assert len(sessions) == 1
        assert sessions[0]["command"] == "resume"
        assert sessions[0]["selected_components"] == [("A",)]
        assert (sessions[0]["status"], sessions[0]["outcome"]) == ("terminal", "failed")
        assert "injected second resume update failure" in sessions[0]["failures"][0]["error"]
        assert storage.get_component_reservation(("A",)) is None
    finally:
        _close(storage)
