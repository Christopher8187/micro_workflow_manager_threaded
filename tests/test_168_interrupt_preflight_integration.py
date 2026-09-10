"""Interrupt policy is resolved from exact source and topology before mutation."""

from __future__ import annotations

import importlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from micro_workflow_manager import cli
from micro_workflow_manager.cli.project import load_workflow
from micro_workflow_manager.paths import config_file
from micro_workflow_manager.storage import FileStorage
from tests.test_064_read_only_previews import _close_without_sidecars, _snapshot
from tests.test_133_readonly_reset_live_refusal import _closed_database_rows
from tests.test_146_native_applied_recovery import _create_active_scope
from tests.test_149_interrupt_declarations import _make_interrupt_project, _router_source


def _close_bootstrap(root: Path) -> None:
    _close_without_sidecars(FileStorage(root), root)


def _write_router(root: Path, node: str, source: str) -> Path:
    path = root / "src" / "node_behavior" / f"{node}.py"
    path.write_text(source.strip() + "\n", encoding="utf-8")
    return path


def _declared_source(root: Path, form: str, *, jobs: bool = True) -> str:
    marker = root / "user-router-imported.txt"
    if form == "name-keyword":
        declaration = "router = NodeRouter(name='I', runner='direct', interrupt=True)"
        imported = "from micro_workflow_manager import NodeRouter"
    elif form == "imported-alias":
        declaration = "router = RoutedNode(name='I', runner='direct', interrupt=True)"
        imported = "from micro_workflow_manager import NodeRouter as RoutedNode"
    else:
        assert form == "from-file-literal"
        declaration = "router = NodeRouter.from_file('I.py', runner='direct', interrupt=True)"
        imported = "from micro_workflow_manager import NodeRouter"
    creation = "router.create_job(number=1)" if jobs else ""
    return f"""
from pathlib import Path
Path({str(marker)!r}).write_text('imported', encoding='utf-8')
{imported}
{declaration}
{creation}
@router.task
def run(ctx):
    ctx.write_output('ran.txt', 'I')
    return 'I'
"""


@pytest.mark.parametrize(
    "form",
    ["name-keyword", "imported-alias", "from-file-literal"],
)
def test_static_declaration_forms_match_actual_cli_loading_scope(
    tmp_path, monkeypatch, capsys, form,
):
    _make_interrupt_project(
        tmp_path, monkeypatch,
        edges=[("I", "I")], jobs={"I"},
    )
    _close_bootstrap(tmp_path)
    marker = tmp_path / "user-router-imported.txt"
    _write_router(tmp_path, "I", _declared_source(tmp_path, form))
    before_rows = _closed_database_rows(tmp_path)
    before_files = _snapshot(tmp_path)
    capsys.readouterr()

    assert cli.main([
        "run", "I", "--plan", "--interrupt-policy", "run-all",
    ]) == 0

    plan = capsys.readouterr().out.lower()
    assert "interrupt component {i}" in plan
    assert "canonical replay key: i" in plan
    assert "run" in plan
    assert _closed_database_rows(tmp_path) == before_rows
    assert _snapshot(tmp_path) == before_files
    assert not marker.exists()

    assert cli.main([
        "run", "I", "--runner", "direct", "--interrupt-policy", "run-all",
    ]) == 0
    assert marker.read_text(encoding="utf-8") == "imported"
    storage = FileStorage(tmp_path)
    try:
        schema = json.loads(storage.node_schema_file("I").read_text(encoding="utf-8"))
        assert schema["interrupt"] is True
        assert storage.get_job_status("I", 1) == "done"
    finally:
        _close_without_sidecars(storage, tmp_path)


def _make_stale_interrupt_project(tmp_path, monkeypatch):
    edges = [("A", "A"), ("I", "I")]
    _make_interrupt_project(
        tmp_path, monkeypatch,
        edges=edges, interrupt_nodes={"I"}, jobs={"I"},
    )
    _close_bootstrap(tmp_path)
    workflow = load_workflow(tmp_path, "direct")
    storage = workflow.storage
    shape = workflow.topology.snapshot().shape_json
    storage.register_component_topology(workflow.topology.snapshot())
    owner = _create_active_scope(storage, shape, "A", "stale-A")
    _close_without_sidecars(storage, tmp_path)
    for node in ("A", "I"):
        _write_router(
            tmp_path,
            node,
            _router_source(
                node,
                interrupt="True" if node == "I" else None,
                jobs=1 if node == "I" else 0,
                import_marker=True,
            ),
        )
    return edges, owner, tmp_path / "user-router-imported.txt"


def _running_session_row(root: Path, session_id: str):
    rows = _closed_database_rows(root)["execution_sessions"]
    matches = [row for row in rows if row[0] == session_id]
    assert len(matches) == 1
    return matches[0]


def test_interactive_run_all_is_chosen_before_stale_session_recovery_or_import(
    tmp_path, monkeypatch, capsys,
):
    _, _, marker = _make_stale_interrupt_project(tmp_path, monkeypatch)
    before_rows = _closed_database_rows(tmp_path)
    before_files = _snapshot(tmp_path)
    prompts = []

    def answer(prompt):
        prompts.append(prompt)
        assert _closed_database_rows(tmp_path) == before_rows
        assert _snapshot(tmp_path) == before_files
        assert not marker.exists()
        return "run-all"

    monkeypatch.setattr("builtins.input", answer)
    monkeypatch.setattr("sys.stdin", SimpleNamespace(isatty=lambda: True))
    capsys.readouterr()

    assert cli.main(["run", "I", "--runner", "direct"]) == 0

    assert len(prompts) == 1
    assert "run all" in prompts[0].lower()
    assert marker.exists()
    storage = FileStorage(tmp_path)
    try:
        stale = storage.get_execution_session("stale-A")
        assert stale is not None
        assert (stale["status"], stale["outcome"]) == ("terminal", "failed")
        assert storage.get_job_status("I", 1) == "done"
    finally:
        _close_without_sidecars(storage, tmp_path)


def test_interactive_individual_prompts_only_in_reachability_order_before_plan(
    tmp_path, monkeypatch, capsys,
):
    _make_interrupt_project(
        tmp_path, monkeypatch,
        edges=[("S", "I1"), ("I1", "I2")],
        interrupt_nodes={"I1", "I2"},
    )
    _close_bootstrap(tmp_path)
    for node in ("S", "I1", "I2"):
        _write_router(
            tmp_path,
            node,
            _router_source(
                node,
                interrupt="True" if node in {"I1", "I2"} else None,
                import_marker=True,
            ),
        )
    before_rows = _closed_database_rows(tmp_path)
    before_files = _snapshot(tmp_path)
    answers = iter(("individual", "stop"))
    prompts = []

    def answer(prompt):
        prompts.append(prompt)
        assert _closed_database_rows(tmp_path) == before_rows
        assert _snapshot(tmp_path) == before_files
        assert not (tmp_path / "user-router-imported.txt").exists()
        return next(answers)

    monkeypatch.setattr("builtins.input", answer)
    monkeypatch.setattr("sys.stdin", SimpleNamespace(isatty=lambda: True))
    capsys.readouterr()

    assert cli.main(["runfrom", "S", "--plan"]) == 0

    assert len(prompts) == 2
    assert "decide individually" in prompts[0].lower()
    assert "{i1}" in prompts[1].lower()
    assert all("i2" not in prompt.lower() for prompt in prompts)
    output = capsys.readouterr().out.lower()
    assert "interrupt component {i1}" in output
    assert "stop" in output
    assert _closed_database_rows(tmp_path) == before_rows
    assert _snapshot(tmp_path) == before_files


def test_policy_error_precedes_recovery_of_an_existing_stale_session(
    tmp_path, monkeypatch, capsys,
):
    _, _, marker = _make_stale_interrupt_project(tmp_path, monkeypatch)
    before_rows = _closed_database_rows(tmp_path)
    before_files = _snapshot(tmp_path)
    capsys.readouterr()

    assert cli.main([
        "run", "I", "--interrupt-policy", "individual",
    ]) == 1

    captured = capsys.readouterr()
    output = (captured.out + captured.err).lower()
    assert "individual" in output
    assert "i=run" in output and "i=stop" in output
    assert _closed_database_rows(tmp_path) == before_rows
    assert _snapshot(tmp_path) == before_files
    assert _running_session_row(tmp_path, "stale-A") == next(
        row for row in before_rows["execution_sessions"] if row[0] == "stale-A"
    )
    assert not marker.exists()


def test_explicit_interrupt_start_must_be_interrupt_classified_before_mutation(
    tmp_path, monkeypatch, capsys,
):
    _make_interrupt_project(
        tmp_path, monkeypatch,
        edges=[("A", "A")], jobs={"A"},
    )
    _close_bootstrap(tmp_path)
    _write_router(
        tmp_path, "A", _router_source("A", jobs=1, import_marker=True),
    )
    before_rows = _closed_database_rows(tmp_path)
    before_files = _snapshot(tmp_path)
    capsys.readouterr()

    assert cli.main([
        "run", "A", "--plan", "--interrupt", "--interrupt-policy", "run-all",
    ]) == 1

    captured = capsys.readouterr()
    output = (captured.out + captured.err).lower()
    assert "interrupt" in output and "not interrupt" in output
    assert _closed_database_rows(tmp_path) == before_rows
    assert _snapshot(tmp_path) == before_files
    assert not (tmp_path / "user-router-imported.txt").exists()


@pytest.mark.parametrize("drift", ["source", "topology"])
def test_observed_source_and_topology_are_revalidated_before_recovery_or_admission(
    tmp_path, monkeypatch, capsys, drift,
):
    edges, _, marker = _make_stale_interrupt_project(tmp_path, monkeypatch)
    before_rows = _closed_database_rows(tmp_path)
    before_files = _snapshot(tmp_path)
    interrupt_command = importlib.import_module(
        "micro_workflow_manager.cli.interrupt_command"
    )
    original = interrupt_command.read_interrupt_command_preflight
    injected_files = []

    def observe_then_drift(root, args):
        observed = original(root, args)
        if drift == "source":
            source = root / "src" / "node_behavior" / "I.py"
            source.write_text(
                source.read_text(encoding="utf-8") + "# changed after preflight\n",
                encoding="utf-8",
            )
        else:
            changed_edges = [*edges, ("I", "A")]
            config_path = config_file(root)
            config = json.loads(config_path.read_text(encoding="utf-8"))
            config["edges"] = [list(edge) for edge in changed_edges]
            config_path.write_text(
                json.dumps(config, ensure_ascii=False, indent=2) + "\n",
                encoding="utf-8",
            )
            (root / "src" / "graph.py").write_text(
                f"EDGES = {changed_edges!r}\n", encoding="utf-8",
            )
        injected_files.append(_snapshot(root))
        return observed

    monkeypatch.setattr(
        interrupt_command, "read_interrupt_command_preflight", observe_then_drift,
    )
    capsys.readouterr()

    assert cli.main([
        "run", "I", "--runner", "direct", "--interrupt-policy", "run-all",
    ]) == 1

    captured = capsys.readouterr()
    output = (captured.out + captured.err).lower()
    assert "interrupt" in output
    assert "changed" in output
    assert drift in output
    assert len(injected_files) == 1
    assert _closed_database_rows(tmp_path) == before_rows
    assert _snapshot(tmp_path) == injected_files[0]
    assert _running_session_row(tmp_path, "stale-A") == next(
        row for row in before_rows["execution_sessions"] if row[0] == "stale-A"
    )
    assert not marker.exists()
