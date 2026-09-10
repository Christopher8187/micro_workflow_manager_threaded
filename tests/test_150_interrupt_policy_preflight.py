"""Interrupt choices are complete, deterministic, and resolved before mutation."""

from __future__ import annotations

import pytest

from micro_workflow_manager import cli
from micro_workflow_manager.cli.project import load_workflow
from tests.test_064_read_only_previews import _close_without_sidecars, _snapshot
from tests.test_133_readonly_reset_live_refusal import _closed_database_rows
from tests.test_149_interrupt_declarations import _make_interrupt_project


SIX_GRAPH_COMMANDS = [
    pytest.param(["run", "I"], id="run"),
    pytest.param(["runfrom", "P"], id="runfrom"),
    pytest.param(["runbetween", "P", "D"], id="runbetween"),
    pytest.param(["resume", "I"], id="resume"),
    pytest.param(["resumefrom", "P"], id="resumefrom"),
    pytest.param(["resumebetween", "P", "D"], id="resumebetween"),
]


def _preflight_project(tmp_path, monkeypatch, *, import_marker=True):
    _make_interrupt_project(
        tmp_path,
        monkeypatch,
        edges=[("P", "I"), ("I", "D")],
        interrupt_nodes={"I"},
        import_marker=import_marker,
    )


def _assert_closed_project_unchanged(tmp_path, before_rows, before_files):
    assert _closed_database_rows(tmp_path) == before_rows
    assert _snapshot(tmp_path) == before_files
    assert not (tmp_path / "user-router-imported.txt").exists()
    assert not (tmp_path / ".mwf" / "state.sqlite3-wal").exists()
    assert not (tmp_path / ".mwf" / "state.sqlite3-shm").exists()


@pytest.mark.parametrize("arguments", SIX_GRAPH_COMMANDS)
def test_all_six_commands_refuse_missing_reachable_individual_choice_before_mutation(
    tmp_path, monkeypatch, capsys, arguments,
):
    _preflight_project(tmp_path, monkeypatch)
    before_rows = _closed_database_rows(tmp_path)
    before_files = _snapshot(tmp_path)
    capsys.readouterr()

    assert cli.main([*arguments, "--interrupt-policy", "individual"]) == 1

    captured = capsys.readouterr()
    output = (captured.out + captured.err).lower()
    assert "interrupt" in output
    assert "individual" in output
    assert "i=run" in output and "i=stop" in output
    _assert_closed_project_unchanged(tmp_path, before_rows, before_files)


@pytest.mark.parametrize(
    "options,message",
    [
        (
            ["--interrupt-policy", "individual", "--interrupt-choice", "I=run",
             "--interrupt-choice", "Missing=run"],
            "unknown",
        ),
        (
            ["--interrupt-policy", "individual", "--interrupt-choice", "I=run",
             "--interrupt-choice", "P=run"],
            "not interrupt",
        ),
        (
            ["--interrupt-policy", "run-all", "--interrupt-choice", "I=run"],
            "contradict",
        ),
        (
            ["--interrupt-policy", "individual", "--interrupt-choice", "I=run",
             "--interrupt-choice", "J=stop"],
            "conflict",
        ),
    ],
    ids=["unknown-node", "ordinary-node", "policy-plus-choice", "component-alias-conflict"],
)
def test_invalid_or_contradictory_policy_refuses_before_import_or_mutation(
    tmp_path, monkeypatch, capsys, options, message,
):
    _make_interrupt_project(
        tmp_path,
        monkeypatch,
        edges=[("P", "I"), ("I", "J"), ("J", "I"), ("J", "D")],
        interrupt_nodes={"I"},
        import_marker=True,
    )
    before_rows = _closed_database_rows(tmp_path)
    before_files = _snapshot(tmp_path)
    capsys.readouterr()

    assert cli.main(["runfrom", "P", *options]) == 1

    captured = capsys.readouterr()
    assert message in (captured.out + captured.err).lower()
    _assert_closed_project_unchanged(tmp_path, before_rows, before_files)


@pytest.mark.parametrize("arguments", SIX_GRAPH_COMMANDS)
def test_all_six_readonly_plans_render_component_decisions_and_unused_choices(
    tmp_path, monkeypatch, capsys, arguments,
):
    _make_interrupt_project(
        tmp_path,
        monkeypatch,
        edges=[("P", "I"), ("I", "J"), ("J", "I"), ("J", "K"), ("K", "D")],
        interrupt_nodes={"I", "K"},
        import_marker=True,
    )
    before_rows = _closed_database_rows(tmp_path)
    before_files = _snapshot(tmp_path)
    capsys.readouterr()

    assert cli.main([
        *arguments,
        "--plan",
        "--interrupt-policy", "individual",
        "--interrupt-choice", "I=stop",
        "--interrupt-choice", "J=stop",
        "--interrupt-choice", "K=run",
    ]) == 0

    output = capsys.readouterr().out.lower()
    assert "interrupt component {i, j}" in output
    assert "canonical replay key: i" in output
    assert "stop" in output
    assert "k=run" in output
    assert "unreachable" in output or "unused" in output or "outside the selection" in output
    _assert_closed_project_unchanged(tmp_path, before_rows, before_files)


@pytest.mark.parametrize(
    "selection",
    [
        pytest.param(["job", "1"], id="selected-job"),
        pytest.param(["sample", "100%", "--seed", "interrupt-preflight"], id="sample"),
    ],
)
def test_selected_job_and_sample_require_policy_for_their_selected_component_before_mutation(
    tmp_path, monkeypatch, capsys, selection,
):
    _make_interrupt_project(
        tmp_path,
        monkeypatch,
        edges=[("I", "I")],
        interrupt_nodes={"I"},
        jobs={"I"},
    )
    workflow = load_workflow(tmp_path, "direct")
    workflow.storage.register_component_topology(workflow.topology.snapshot())
    _close_without_sidecars(workflow.storage, tmp_path)
    before_rows = _closed_database_rows(tmp_path)
    before_files = _snapshot(tmp_path)
    capsys.readouterr()

    assert cli.main([
        "run", "I", *selection, "--interrupt-policy", "individual",
    ]) == 1

    captured = capsys.readouterr()
    output = (captured.out + captured.err).lower()
    assert "interrupt" in output and "i=run" in output and "i=stop" in output
    assert _closed_database_rows(tmp_path) == before_rows
    assert _snapshot(tmp_path) == before_files


@pytest.mark.parametrize(
    "selection",
    [
        pytest.param(["job", "1"], id="selected-job"),
        pytest.param(["sample", "100%", "--seed", "interrupt-plan"], id="sample"),
    ],
)
def test_selected_job_and_sample_readonly_plans_retain_selection_and_render_stop(
    tmp_path, monkeypatch, capsys, selection,
):
    _make_interrupt_project(
        tmp_path,
        monkeypatch,
        edges=[("I", "I")],
        interrupt_nodes={"I"},
        jobs={"I"},
    )
    workflow = load_workflow(tmp_path, "direct")
    workflow.storage.register_component_topology(workflow.topology.snapshot())
    _close_without_sidecars(workflow.storage, tmp_path)
    before_rows = _closed_database_rows(tmp_path)
    before_files = _snapshot(tmp_path)
    capsys.readouterr()

    assert cli.main([
        "run", "I", *selection, "--plan", "--interrupt-policy", "stop-all",
    ]) == 0

    output = capsys.readouterr().out.lower()
    assert "interrupt component {i}" in output
    assert "canonical replay key: i" in output
    assert "stop" in output
    if selection[0] == "sample":
        assert "sample plan for: i" in output
        assert "job ids: 1" in output
    else:
        assert "i/1" in output
    assert _closed_database_rows(tmp_path) == before_rows
    assert _snapshot(tmp_path) == before_files
