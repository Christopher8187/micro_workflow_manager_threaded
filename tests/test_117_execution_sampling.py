"""Public CLI regressions for native component sampling."""

from __future__ import annotations

import shlex

from micro_workflow_manager import cli
from micro_workflow_manager.cli.project import load_workflow
from micro_workflow_manager.storage import FileStorage
from tests.test_036_hoeflein_scheduling import make_project
from tests.test_090_component_session_settlement import _close, _rows


def _files(root):
    return {
        path.relative_to(root).as_posix(): path.read_bytes()
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


def _job_snapshot(storage, node, job_id):
    return {
        "job": storage.load_job(node, job_id),
        "status": storage.read_job_status_data(node, job_id),
        "owner": storage.read_job_current_owner(node, job_id),
        "events": storage.read_job_events(node, job_id),
        "files": _files(storage.job_base_dir(node, job_id)),
    }


def _component_result(storage, component):
    state = storage.get_component_state(component)
    return {
        key: state[key]
        for key in (
            "lifecycle",
            "stability",
            "instability_origin",
            "misaligned",
            "alignment_generation",
        )
    }


def _assert_sampled(storage, component):
    assert _component_result(storage, component) == {
        "lifecycle": "sampled",
        "stability": "stable",
        "instability_origin": None,
        "misaligned": False,
        "alignment_generation": 0,
    }
    assert storage.get_component_reservation(component) is None
    assert storage.get_live_main_session() is None


def _assert_one_terminal_session(storage, selected_jobs):
    session, = storage.list_execution_sessions()
    assert (session["status"], session["outcome"]) == ("terminal", "done")
    assert session["selected_jobs"] == sorted(selected_jobs)
    return session


def _uniform_component_project(tmp_path, monkeypatch, *, counts, edges):
    files = {}
    for node, count in counts.items():
        creation = (
            f'router.create_job(number={count}, params={{"role": "ordinary"}})'
            if count
            else ""
        )
        files[node] = f'''
            from micro_workflow_manager import NodeRouter
            router = NodeRouter({node!r}, runner="direct")
            {creation}
            @router.task
            def run(ctx, role):
                assert role == "ordinary"
                return f"{node}/{{ctx.job_id}}"
        '''
    make_project(
        tmp_path,
        monkeypatch,
        edges=f"EDGES = {edges!r}",
        files=files,
        runner="direct",
    )
    return FileStorage(tmp_path)


def test_sampling_percentage_uses_ceiling_for_only_the_addressed_raw_member(
    tmp_path, monkeypatch, capsys,
):
    storage = _uniform_component_project(
        tmp_path,
        monkeypatch,
        counts={"A": 3, "B": 2},
        edges=[("A", "B"), ("B", "A")],
    )
    before = {
        (node, job_id): _job_snapshot(storage, node, job_id)
        for node, count in (("A", 3), ("B", 2))
        for job_id in range(1, count + 1)
    }
    capsys.readouterr()
    try:
        # ceil(34% * 3) is two. The shorthand addresses A only, so B gets zero.
        assert cli.main(
            ["run", "A", "sample", "34%", "--seed", "ceiling", "--runner", "direct"]
        ) == 0

        selected = [
            job_id for job_id in range(1, 4)
            if storage.get_job_status("A", job_id) == "done"
        ]
        assert len(selected) == 2
        for job_id in set(range(1, 4)) - set(selected):
            assert _job_snapshot(storage, "A", job_id) == before["A", job_id]
        for job_id in range(1, 3):
            assert _job_snapshot(storage, "B", job_id) == before["B", job_id]

        session = _assert_one_terminal_session(
            storage, [("A", job_id) for job_id in selected]
        )
        assert {
            storage.read_job_current_owner("A", job_id)["session_id"]
            for job_id in selected
        } == {session["session_id"]}
        _assert_sampled(storage, ("A", "B"))
    finally:
        _close(storage)


def test_sampling_named_member_assignments_leave_omitted_members_at_zero(
    tmp_path, monkeypatch, capsys,
):
    storage = _uniform_component_project(
        tmp_path,
        monkeypatch,
        counts={"A": 3, "B": 2, "C": 3},
        edges=[("A", "B"), ("B", "C"), ("C", "A")],
    )
    before_b = {
        job_id: _job_snapshot(storage, "B", job_id)
        for job_id in range(1, 3)
    }
    capsys.readouterr()
    try:
        assert cli.main(
            [
                "run", "A", "sample", "A=34%", "C=50%",
                "--seed", "named-members", "--runner", "direct",
            ]
        ) == 0

        selected_a = [
            job_id for job_id in range(1, 4)
            if storage.get_job_status("A", job_id) == "done"
        ]
        selected_c = [
            job_id for job_id in range(1, 4)
            if storage.get_job_status("C", job_id) == "done"
        ]
        assert len(selected_a) == 2
        assert len(selected_c) == 2
        assert {
            job_id: _job_snapshot(storage, "B", job_id)
            for job_id in range(1, 3)
        } == before_b
        _assert_one_terminal_session(
            storage,
            [("A", job_id) for job_id in selected_a]
            + [("C", job_id) for job_id in selected_c],
        )
        _assert_sampled(storage, ("A", "B", "C"))
    finally:
        _close(storage)


def test_sampling_runs_same_component_causal_children_and_preserves_unrelated_work(
    tmp_path, monkeypatch, capsys,
):
    make_project(
        tmp_path,
        monkeypatch,
        edges="EDGES = [('A', 'B'), ('B', 'A')]",
        runner="direct",
        files={
            "A": '''
                from micro_workflow_manager import NodeRouter
                router = NodeRouter("A", runner="direct")
                router.create_job(params={"role": "sample-root"})
                @router.task
                def run(ctx, role):
                    assert role == "sample-root"
                    ctx.node("B").add(role="causal-child")
                    return "root"
            ''',
            "B": '''
                from micro_workflow_manager import NodeRouter
                router = NodeRouter("B", runner="direct")
                router.create_job(params={"role": "unrelated"})
                @router.task
                def run(ctx, role):
                    if role != "causal-child":
                        raise AssertionError("unrelated B job executed")
                    return "child"
            ''',
        },
    )
    storage = FileStorage(tmp_path)
    unrelated = _job_snapshot(storage, "B", 1)
    capsys.readouterr()
    try:
        assert cli.main(
            ["run", "A", "sample", "100%", "--seed", "causal", "--runner", "direct"]
        ) == 0

        assert storage.get_job_status("A", 1) == "done"
        assert storage.list_job_ids("B") == [1, 2]
        assert _job_snapshot(storage, "B", 1) == unrelated
        assert storage.get_job_status("B", 2) == "done"
        root = storage.read_job_current_owner("A", 1)
        child = storage.read_job_current_owner("B", 2)
        assert child["created_by_execution_id"] == root["execution_id"]
        assert child["session_id"] == root["session_id"]
        assert root["component"] == child["component"] == ("A", "B")
        session = _assert_one_terminal_session(storage, [("A", 1)])
        assert session["session_id"] == root["session_id"]
        _assert_sampled(storage, ("A", "B"))
    finally:
        _close(storage)


def test_sampling_zero_selection_creates_no_session_and_changes_no_data(
    tmp_path, monkeypatch, capsys,
):
    storage = _uniform_component_project(
        tmp_path,
        monkeypatch,
        counts={"A": 2, "B": 2},
        edges=[("A", "B"), ("B", "A")],
    )
    storage.db_mutation_barrier()
    before_rows = _rows(storage)
    before_files = _files(tmp_path / "node")
    before_component = storage.get_component_state(("A", "B"))
    capsys.readouterr()
    try:
        assert cli.main(
            ["run", "A", "sample", "A=0%", "--seed", "zero", "--runner", "direct"]
        ) == 0

        output = capsys.readouterr().out.lower()
        assert "no jobs selected" in output or "0 jobs" in output
        assert _rows(storage) == before_rows
        assert _files(tmp_path / "node") == before_files
        assert storage.get_component_state(("A", "B")) == before_component
        assert storage.list_execution_sessions() == []
        assert storage.get_component_reservation(("A", "B")) is None
        assert storage.get_live_main_session() is None
    finally:
        _close(storage)


def _replay_preparation_snapshot(storage):
    return {
        "component": storage.get_component_state(("A",)),
        "parent_component": storage.get_component_state(("P",)),
        "jobs": {
            job_id: _job_snapshot(storage, "A", job_id)
            for job_id in storage.list_job_ids("A")
        },
        "parent_jobs": {
            job_id: _job_snapshot(storage, "P", job_id)
            for job_id in storage.list_job_ids("P")
        },
        "input_owner": storage.read_node_input_owner("A", "P/evidence.txt"),
        "input_rows": {
            table: [dict(row) for row in storage.db_connection().execute(
                f'SELECT * FROM {table} ORDER BY rowid'
            )]
            for table in ('input_publications', 'managed_input_files', 'managed_input_producers')
        },
        "causes": storage.read_component_misalignment_causes(("A",)),
        "receipts": [dict(row) for row in storage.db_connection().execute(
            "SELECT * FROM preparation_receipts ORDER BY operation_id"
        )],
        "files": _files(storage.project_dir / "node"),
    }


def test_guarded_sampling_replay_refuses_changed_receiving_input_before_preparation(
    tmp_path, monkeypatch, capsys,
):
    make_project(
        tmp_path,
        monkeypatch,
        edges="EDGES = [('P', 'A')]",
        runner="direct",
        files={
            "P": '''
                from micro_workflow_manager import NodeRouter
                router = NodeRouter("P", runner="direct")
                router.create_job(params={"value": "first", "filename": "evidence.txt"})
                @router.task
                def run(ctx, value, filename):
                    ctx.node("A").write_input(filename, value)
                    return value
            ''',
            "A": '''
                from micro_workflow_manager import NodeRouter
                router = NodeRouter("A", runner="direct")
                router.create_job(number=4, params={"role": "sample"})
                @router.task
                def run(ctx, role):
                    assert role == "sample"
                    return ctx.job_id
            ''',
        },
    )
    assert cli.main(["run", "P", "--runner", "direct"]) == 0
    capsys.readouterr()
    assert cli.main(
        [
            "run", "A", "sample", "50%", "--seed", "guarded-input",
            "--plan", "--runner", "direct",
        ]
    ) == 0
    plan_output = capsys.readouterr().out
    replay_line, = [line for line in plan_output.splitlines() if "guarded replay:" in line]
    replay = shlex.split(replay_line.split("guarded replay:", 1)[1].strip())
    assert replay[0] == "mwf"

    workflow = load_workflow(tmp_path, "direct")
    storage = workflow.storage
    try:
        received = storage.node_input_dir("A") / "P" / "evidence.txt"
        assert received.read_text(encoding="utf-8") == "first"
        first_owner = storage.read_node_input_owner("A", "P/evidence.txt")
        assert first_owner["node_name"] == "P"

        workflow.add_job(None, "P", job_id=2, value="second", filename="second-evidence.txt")
        assert workflow.run_job("P", 2) == "second"
        second_received = storage.node_input_dir("A") / "P" / "second-evidence.txt"
        assert received.read_text(encoding="utf-8") == "first"
        assert second_received.read_text(encoding="utf-8") == "second"
        assert storage.read_node_input_owner("A", "P/evidence.txt") == first_owner
        second_owner = storage.read_node_input_owner("A", "P/second-evidence.txt")
        assert second_owner["execution_id"] != first_owner["execution_id"]

        storage.db_mutation_barrier()
        before = _replay_preparation_snapshot(storage)
        session_ids = {
            session["session_id"] for session in storage.list_execution_sessions()
        }
        capsys.readouterr()
        assert cli.main([*replay[1:], "--runner", "direct"]) == 1

        assert "changed" in capsys.readouterr().err.lower()
        assert _replay_preparation_snapshot(storage) == before
        assert received.read_text(encoding="utf-8") == "first"
        assert second_received.read_text(encoding="utf-8") == "second"
        assert storage.read_node_input_owner("A", "P/evidence.txt") == first_owner
        assert storage.read_node_input_owner("A", "P/second-evidence.txt") == second_owner
        added = [
            session for session in storage.list_execution_sessions()
            if session["session_id"] not in session_ids
        ]
        assert not added or all(
            (session["status"], session["outcome"]) == ("terminal", "failed")
            for session in added
        )
        assert storage.get_component_reservation(("A",)) is None
        assert storage.get_live_main_session() is None
    finally:
        _close(storage)


def test_partial_random_sample_after_full_success_is_sampled_until_full_population_runs(
    tmp_path, monkeypatch, capsys,
):
    storage = _uniform_component_project(
        tmp_path,
        monkeypatch,
        counts={"A": 4},
        edges=[("A", "A")],
    )
    capsys.readouterr()
    try:
        assert cli.main(["run", "A", "--runner", "direct"]) == 0
        capsys.readouterr()
        full_result = _component_result(storage, ("A",))
        assert full_result == {
            "lifecycle": "done",
            "stability": "stable",
            "instability_origin": None,
            "misaligned": False,
            "alignment_generation": 1,
        }
        before_partial = {
            job_id: _job_snapshot(storage, "A", job_id)
            for job_id in range(1, 5)
        }
        prior_sessions = {
            session["session_id"] for session in storage.list_execution_sessions()
        }

        assert cli.main(
            [
                "run", "A", "sample", "50%", "--seed", "partial-after-done",
                "--runner", "direct",
            ]
        ) == 0
        capsys.readouterr()
        partial_session, = [
            session for session in storage.list_execution_sessions()
            if session["session_id"] not in prior_sessions
        ]
        selected = [job_id for node, job_id in partial_session["selected_jobs"] if node == "A"]
        assert len(selected) == 2
        assert (partial_session["status"], partial_session["outcome"]) == ("terminal", "done")
        assert _component_result(storage, ("A",)) == {
            **full_result,
            "lifecycle": "sampled",
        }
        for job_id in set(range(1, 5)) - set(selected):
            assert _job_snapshot(storage, "A", job_id) == before_partial[job_id]
        for job_id in selected:
            assert (
                storage.read_job_current_owner("A", job_id)["execution_id"]
                != before_partial[job_id]["owner"]["execution_id"]
            )

        before_full_sample = {
            job_id: storage.read_job_current_owner("A", job_id)["execution_id"]
            for job_id in range(1, 5)
        }
        prior_sessions = {
            session["session_id"] for session in storage.list_execution_sessions()
        }
        assert cli.main(
            [
                "run", "A", "sample", "100%", "--seed", "full-after-sampled",
                "--runner", "direct",
            ]
        ) == 0
        capsys.readouterr()
        final_session, = [
            session for session in storage.list_execution_sessions()
            if session["session_id"] not in prior_sessions
        ]
        assert final_session["selected_jobs"] == [("A", job_id) for job_id in range(1, 5)]
        assert (final_session["status"], final_session["outcome"]) == ("terminal", "done")
        assert _component_result(storage, ("A",)) == full_result
        for job_id in range(1, 5):
            assert storage.get_job_status("A", job_id) == "done"
            assert (
                storage.read_job_current_owner("A", job_id)["execution_id"]
                != before_full_sample[job_id]
            )
        assert storage.get_component_reservation(("A",)) is None
        assert storage.get_live_main_session() is None
    finally:
        _close(storage)
