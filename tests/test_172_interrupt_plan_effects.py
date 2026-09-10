"""Interrupt plans show the preparation that the executing command will apply."""
from __future__ import annotations

import pytest

from micro_workflow_manager import cli
from micro_workflow_manager.cli.project import load_workflow
from tests.test_064_read_only_previews import _close_without_sidecars, _snapshot
from tests.test_133_readonly_reset_live_refusal import _closed_database_rows
from tests.test_149_interrupt_declarations import _make_interrupt_project


@pytest.mark.parametrize("arguments", [
    ["run", "I"], ["runfrom", "S"], ["runbetween", "S", "D"],
    ["resume", "I"], ["resumefrom", "S"], ["resumebetween", "S", "D"],
])
def test_stop_plan_retains_selection_without_claiming_stopped_work_will_be_prepared(
    tmp_path, monkeypatch, capsys, arguments,
):
    _make_interrupt_project(
        tmp_path, monkeypatch, edges=[("S", "I"), ("I", "D")],
        interrupt_nodes={"I"}, jobs={"S", "I", "D"},
    )
    workflow = load_workflow(tmp_path, "direct")
    workflow.storage.register_component_topology(workflow.topology.snapshot())
    _close_without_sidecars(workflow.storage, tmp_path)
    rows, files = _closed_database_rows(tmp_path), _snapshot(tmp_path)
    capsys.readouterr()

    assert cli.main([*arguments, "--plan", "--interrupt-policy", "stop-all"]) == 0

    output = capsys.readouterr().out.lower()
    assert "interrupt component {i}" in output
    assert "action: stop" in output
    assert "selected nodes:" in output
    assert "i: generated output would be cleared" not in output
    assert "i/1: job would be requeued" not in output
    assert "i/1: trace would be cleared" not in output
    assert not any("i/1:" in line and "would resume" in line for line in output.splitlines())
    assert "preparation stopped before component {i}" in output
    assert _closed_database_rows(tmp_path) == rows
    assert _snapshot(tmp_path) == files


def test_applied_sample_stop_reports_its_actual_selection_without_calling_it_readonly(
    tmp_path, monkeypatch, capsys,
):
    _make_interrupt_project(
        tmp_path, monkeypatch, edges=[("I", "I")], interrupt_nodes={"I"}, jobs={"I"},
    )
    capsys.readouterr()
    assert cli.main([
        "run", "I", "sample", "100%", "--seed", "stopped-selection",
        "--interrupt-policy", "stop-all",
    ]) == 0
    output = capsys.readouterr().out.lower()
    assert "sample selection for: i" in output
    assert "job ids: 1" in output
    assert "stopped" in output
    assert "read-only" not in output
    assert "user code was not loaded" not in output


@pytest.mark.parametrize("policy", ["stop-all", "run-all"])
@pytest.mark.parametrize("keep_trace", [False, True])
@pytest.mark.parametrize("job_mode", ["job", "jobs"])
def test_selected_job_plan_reports_only_selected_preparation_and_component_peers(
    tmp_path, monkeypatch, capsys, policy, keep_trace, job_mode,
):
    _make_interrupt_project(
        tmp_path, monkeypatch, edges=[("I", "C"), ("C", "I"), ("I", "D")],
        interrupt_nodes={"I"}, jobs={"I", "C", "D"},
    )
    workflow = load_workflow(tmp_path, "direct")
    workflow.storage.register_component_topology(workflow.topology.snapshot())
    _close_without_sidecars(workflow.storage, tmp_path)
    rows, files = _closed_database_rows(tmp_path), _snapshot(tmp_path)
    capsys.readouterr()
    arguments = ["run", "I", job_mode, "1", "--plan", "--interrupt-policy", policy]
    if keep_trace:
        arguments.append("--keeptrace")

    assert cli.main(arguments) == 0

    output = capsys.readouterr().out.lower()
    assert "selected jobs:" in output and "i/1:" in output
    assert "same hoeflein component: c" in output
    assert "incomplete start-component inputs: (none)" in output
    assert "fully reset the start component" not in output
    if policy == "stop-all":
        assert "preparation stopped before component {c, i}" in output
        assert "selected jobs remain unchanged" in output
        assert "trace mode: preserve all affected trace journals" in output
        assert "clear affected trace journals" not in output
    else:
        assert "freshen selected jobs and remove work produced by their previous executions" in output
        expected = ("preserve all affected trace journals" if keep_trace
                    else "clear selected-job traces and removed causal-descendant traces")
        assert "trace mode: " + expected in output
    assert _closed_database_rows(tmp_path) == rows
    assert _snapshot(tmp_path) == files


@pytest.mark.parametrize('arguments', [
    ['run', 'I'], ['runfrom', 'I'], ['runbetween', 'I', 'E'],
    ['resume', 'I'], ['resumefrom', 'I'], ['resumebetween', 'I', 'E'],
])
def test_explicit_interrupt_plan_allows_only_its_start_readiness_override(
    tmp_path, monkeypatch, capsys, arguments,
):
    _make_interrupt_project(
        tmp_path, monkeypatch, edges=[('P', 'I'), ('I', 'D'), ('D', 'E')],
        interrupt_nodes={'I'}, jobs={'P', 'I', 'D', 'E'},
    )
    workflow = load_workflow(tmp_path, 'direct')
    workflow.storage.register_component_topology(workflow.topology.snapshot())
    _close_without_sidecars(workflow.storage, tmp_path)
    rows, files = _closed_database_rows(tmp_path), _snapshot(tmp_path)
    capsys.readouterr()

    assert cli.main([*arguments, '--interrupt', '--plan']) == 0

    output = capsys.readouterr().out.lower()
    assert 'explicit interrupt start component {i}' in output
    assert 'would refuse:' not in output
    assert 'no project state was changed' in output
    assert _closed_database_rows(tmp_path) == rows
    assert _snapshot(tmp_path) == files


def test_interrupt_resume_plan_still_refuses_an_incomplete_external_parent_of_a_descendant(
    tmp_path, monkeypatch, capsys,
):
    _make_interrupt_project(
        tmp_path, monkeypatch, edges=[('P', 'I'), ('I', 'D'), ('X', 'D')],
        interrupt_nodes={'I'}, jobs={'P', 'I', 'D', 'X'},
    )
    workflow = load_workflow(tmp_path, 'direct')
    workflow.storage.register_component_topology(workflow.topology.snapshot())
    _close_without_sidecars(workflow.storage, tmp_path)
    rows, files = _closed_database_rows(tmp_path), _snapshot(tmp_path)
    capsys.readouterr()

    assert cli.main(['resumefrom', 'I', '--interrupt', '--plan']) == 0

    output = capsys.readouterr().out.lower()
    assert "would refuse: cannot resume component ['d']" in output
    assert "('x',)" in output
    assert _closed_database_rows(tmp_path) == rows
    assert _snapshot(tmp_path) == files
