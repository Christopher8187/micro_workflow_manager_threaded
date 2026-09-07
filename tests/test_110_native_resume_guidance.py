"""Native resume repair guidance and refusal of missing failed-job ownership."""

from __future__ import annotations

from micro_workflow_manager import cli
from micro_workflow_manager.cli.project import load_workflow
from micro_workflow_manager.storage import FileStorage
from tests.test_036_hoeflein_scheduling import make_project
from tests.test_090_component_session_settlement import _close, _rows
from tests.test_093_native_cli_readiness import _node_files


def _make_misaligned_receivers(tmp_path, monkeypatch, receivers):
    """Complete P/B -> receivers, then publish a new P job to each receiver."""
    receiver_names = tuple(receivers)
    edges = [(producer, receiver) for producer in ("P", "B") for receiver in receiver_names]
    files = {
        "P": f"""
            from micro_workflow_manager import NodeRouter
            router = NodeRouter("P")
            router.create_job(number=1)
            @router.task
            def run(ctx):
                for receiver in {receiver_names!r}:
                    ctx.node(receiver).write_input(
                        f"p-{{ctx.job_id}}.txt", str(ctx.job_id)
                    )
                    ctx.node(receiver).add(value=ctx.job_id)
                return ctx.job_id
        """,
        "B": """
            from micro_workflow_manager import NodeRouter
            router = NodeRouter("B")
            router.create_job(number=1)
            @router.task
            def run(ctx):
                return ctx.job_id
        """,
    }
    for receiver in receiver_names:
        files[receiver] = f"""
            from micro_workflow_manager import NodeRouter
            router = NodeRouter({receiver!r})
            @router.task
            def run(ctx, value):
                ctx.write_output(f"received-{{value}}.txt", str(value))
                return value
        """
    make_project(
        tmp_path,
        monkeypatch,
        edges="EDGES = " + repr(edges),
        files=files,
    )

    workflow = load_workflow(tmp_path)
    try:
        workflow.run()
        workflow.start("P", job_id=2)
        assert workflow.run_job("P", 2) == 2
    finally:
        _close(workflow.storage)

    storage = FileStorage(tmp_path)
    for receiver in receiver_names:
        state = storage.get_component_state((receiver,))
        assert (state["lifecycle"], state["misaligned"]) == ("done", True)
    assert storage.get_component_state(("B",))["misaligned"] is False
    return storage


def _assert_refusal_preserved(storage, tmp_path, command, capsys, expected_fragments):
    before_rows = _rows(storage)
    before_files = _node_files(tmp_path)
    capsys.readouterr()

    assert cli.main(command) == 1

    output = capsys.readouterr()
    text = output.out + output.err
    for fragment in expected_fragments:
        assert fragment in text
    assert _rows(storage) == before_rows
    assert _node_files(tmp_path) == before_files
    for session in storage.list_execution_sessions():
        assert session["status"] == "terminal"


def test_resume_misalignment_guidance_names_fresh_run(tmp_path, monkeypatch, capsys):
    storage = _make_misaligned_receivers(tmp_path, monkeypatch, ("C",))
    try:
        _assert_refusal_preserved(
            storage,
            tmp_path,
            ["resume", "C"],
            capsys,
            ("mwf run C",),
        )
        assert storage.get_component_reservation(("C",)) is None
    finally:
        _close(storage)


def test_resumefrom_misalignment_guidance_names_descendant_reset_and_retry(
    tmp_path, monkeypatch, capsys,
):
    storage = _make_misaligned_receivers(tmp_path, monkeypatch, ("C",))
    try:
        _assert_refusal_preserved(
            storage,
            tmp_path,
            ["resumefrom", "B"],
            capsys,
            ("mwf resetfrom C", "mwf resumefrom B"),
        )
        assert storage.get_component_reservation(("B",)) is None
        assert storage.get_component_reservation(("C",)) is None
    finally:
        _close(storage)


def test_resumefrom_reports_separate_commands_for_misaligned_branches(
    tmp_path, monkeypatch, capsys,
):
    storage = _make_misaligned_receivers(tmp_path, monkeypatch, ("C", "D"))
    try:
        _assert_refusal_preserved(
            storage,
            tmp_path,
            ["resumefrom", "B"],
            capsys,
            ("mwf resetfrom C", "mwf resetfrom D", "mwf resumefrom B"),
        )
        assert storage.get_component_reservation(("B",)) is None
        assert storage.get_component_reservation(("C",)) is None
        assert storage.get_component_reservation(("D",)) is None
    finally:
        _close(storage)


def test_resume_refuses_failed_job_without_retained_owner_before_mutation(
    tmp_path, monkeypatch, capsys,
):
    make_project(
        tmp_path,
        monkeypatch,
        edges="EDGES = [('A', 'A')]",
        files={
            "A": """
                from micro_workflow_manager import NodeRouter
                router = NodeRouter("A")
                router.create_job(number=1)
                @router.task
                def run(ctx):
                    ctx.write_output("failed.txt", "retain me")
                    raise RuntimeError("expected original failure")
            """,
        },
    )
    capsys.readouterr()
    assert cli.main(["run", "A"]) == 1
    capsys.readouterr()

    storage = FileStorage(tmp_path)
    try:
        assert storage.get_job_status("A", 1) == "failed"
        assert storage.get_component_state(("A",))["lifecycle"] == "failed"
        storage.submit_db_mutation(lambda connection: connection.execute(
            "UPDATE job_instances SET last_execution_id=NULL "
            "WHERE node_name='A' AND job_id=1"
        ))
        before_rows = _rows(storage)
        before_files = _node_files(tmp_path)
        before_sessions = storage.list_execution_sessions()

        assert cli.main(["resume", "A"]) == 1

        error = capsys.readouterr().err
        assert "exact owner" in error.lower()
        assert "A/1" in error
        assert _rows(storage) == before_rows
        assert _node_files(tmp_path) == before_files
        assert storage.list_execution_sessions() == before_sessions
        assert storage.get_component_reservation(("A",)) is None
    finally:
        _close(storage)
