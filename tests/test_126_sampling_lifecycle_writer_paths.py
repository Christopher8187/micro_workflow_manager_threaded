"""Writer-path regressions for filtered sample lifecycle settlement."""

from __future__ import annotations

import pytest

from micro_workflow_manager import cli
from micro_workflow_manager.cli.project import load_workflow
from micro_workflow_manager.storage import FileStorage
from tests.test_036_hoeflein_scheduling import make_project
from tests.test_090_component_session_settlement import _close
from tests.test_114_selected_component_lifecycle import _seed_unstable_parent
from tests.test_117_execution_sampling import _component_result, _job_snapshot
from tests.test_123_sample_history_and_filtered_coverage import _filtered_project, _latest_sample


@pytest.mark.parametrize(
    "changed_status,expected_lifecycle",
    [
        pytest.param("cancelled", "sampled", id="changed-excluded-unfinished"),
        pytest.param("done", "done", id="changed-excluded-finished"),
    ],
)
def test_filtered_full_sample_rechecks_changed_excluded_work_in_settlement_writer(
    tmp_path, monkeypatch, capsys, changed_status, expected_lifecycle,
):
    from micro_workflow_manager.cli import run_selected

    storage = _filtered_project(tmp_path, monkeypatch)
    old = _job_snapshot(storage, "A", 1)
    original = run_selected.prepare_selected_addresses
    changed = []

    def change_excluded_after_admission(root, workflow, addresses, **kwargs):
        result = original(root, workflow, addresses, **kwargs)
        workflow.storage.set_job_status(
            "A", 1, changed_status, reason="change excluded job after sample admission",
        )
        workflow.storage.db_mutation_barrier()
        changed.append(_job_snapshot(workflow.storage, "A", 1))
        return result

    capsys.readouterr()
    try:
        with monkeypatch.context() as patch:
            patch.setattr(
                run_selected, "prepare_selected_addresses", change_excluded_after_admission,
            )
            assert cli.main([
                "run", "A", "sample", "100%", "--status", "queued",
                "--seed", "excluded-settlement", "--runner", "direct",
            ]) == 0

        changed_job, = changed
        assert changed_job["status"]["status"] == changed_status
        assert changed_job["job"] == old["job"]
        assert changed_job["owner"] == old["owner"]
        assert storage.get_job_status("A", 2) == "done"
        assert _component_result(storage, ("A",)) == {
            "lifecycle": expected_lifecycle,
            "stability": "stable",
            "instability_origin": None,
            "misaligned": False,
            "alignment_generation": 0,
        }
        session, selection = _latest_sample(storage)
        assert (session["status"], session["outcome"]) == ("terminal", "done")
        assert session["selected_jobs"] == [("A", 2)]
        assert selection["full_starting_coverage"] is True
        assert [(job["job_id"], job["status"])
                for job in selection["members"]["A"]["starting_jobs"]] == [
            (1, "failed"), (2, "queued"),
        ]
    finally:
        _close(storage)


@pytest.mark.parametrize("starting_status", ["failed", "cancelled"])
def test_random_sample_repairs_a_selected_unsuccessful_job(
    tmp_path, monkeypatch, capsys, starting_status,
):
    monkeypatch.setenv("MWF_TEST_SAMPLE_FAILURE", "1")
    make_project(
        tmp_path,
        monkeypatch,
        edges="EDGES = [('A', 'A')]",
        runner="direct",
        files={"A": '''
            import os
            from micro_workflow_manager import NodeRouter
            router = NodeRouter("A", runner="direct")
            router.create_job(params={"role": "repair"})
            @router.task
            def run(ctx, role):
                assert role == "repair"
                if os.environ.get("MWF_TEST_SAMPLE_FAILURE") == "1":
                    raise ValueError("initial sample target failure")
                return "repaired"
        '''},
    )
    assert cli.main(["run", "A", "job", "1", "--runner", "direct"]) == 1
    storage = FileStorage(tmp_path)
    old_owner = storage.read_job_current_owner("A", 1)
    assert storage.get_job_status("A", 1) == "failed"
    if starting_status == "cancelled":
        storage.set_job_status("A", 1, "cancelled", reason="cancel before sampled repair")
        storage.db_mutation_barrier()
    monkeypatch.delenv("MWF_TEST_SAMPLE_FAILURE")
    capsys.readouterr()
    try:
        assert cli.main([
            "run", "A", "sample", "100%", "--status", starting_status,
            "--seed", "repair-unsuccessful", "--runner", "direct",
        ]) == 0

        assert storage.get_job_status("A", 1) == "done"
        owner = storage.read_job_current_owner("A", 1)
        assert owner["execution_id"] != old_owner["execution_id"]
        assert owner["job_instance_id"] == old_owner["job_instance_id"]
        assert _component_result(storage, ("A",)) == {
            "lifecycle": "done",
            "stability": "stable",
            "instability_origin": None,
            "misaligned": False,
            "alignment_generation": 0,
        }
        session, selection = _latest_sample(storage)
        assert session["selected_jobs"] == [("A", 1)]
        assert selection["status_filter"] == [starting_status]
        assert selection["members"]["A"]["starting_jobs"][0]["status"] == starting_status
        assert selection["full_starting_coverage"] is True
    finally:
        _close(storage)


def test_partial_and_full_random_samples_preserve_exact_unstable_parent_origin(
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
                @router.task
                def run(ctx):
                    raise AssertionError("seeded parent task ran")
            ''',
            "A": '''
                from micro_workflow_manager import NodeRouter
                router = NodeRouter("A", runner="direct")
                router.create_job(number=2, params={"role": "sample"})
                @router.task
                def run(ctx, role):
                    assert role == "sample"
                    return ctx.job_id
            ''',
        },
    )
    workflow = load_workflow(tmp_path, "direct")
    storage = workflow.storage
    try:
        storage.register_component_topology(workflow.topology.snapshot())
        _seed_unstable_parent(storage, ("P",), "sample-interrupt-origin")
    finally:
        _close(storage)

    storage = FileStorage(tmp_path)
    capsys.readouterr()
    try:
        assert cli.main([
            "run", "A", "sample", "50%", "--seed", "unstable-partial",
            "--runner", "direct",
        ]) == 0
        partial = _component_result(storage, ("A",))
        assert partial == {
            "lifecycle": "sampled",
            "stability": "unstable",
            "instability_origin": "sample-interrupt-origin",
            "misaligned": False,
            "alignment_generation": 0,
        }

        assert cli.main([
            "run", "A", "sample", "100%", "--seed", "unstable-full",
            "--runner", "direct",
        ]) == 0
        assert _component_result(storage, ("A",)) == {
            **partial,
            "lifecycle": "done",
        }
        sessions = [session for session in storage.list_execution_sessions()
                    if session["command"] == "run sample"]
        assert len(sessions) == 2
        assert all((session["status"], session["outcome"]) == ("terminal", "done")
                   for session in sessions)
    finally:
        _close(storage)
