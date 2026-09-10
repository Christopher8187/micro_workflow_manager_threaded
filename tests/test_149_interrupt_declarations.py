"""Public interrupt declarations are Boolean, raw-node local, and component classified."""

from __future__ import annotations

import json
import textwrap

import pytest

from micro_workflow_manager import MicroWorkflow, NodeRouter, cli
from micro_workflow_manager.cli.project import load_workflow
from micro_workflow_manager.storage import FileStorage
from tests.test_036_hoeflein_scheduling import make_project
from tests.test_064_read_only_previews import _close_without_sidecars, _snapshot
from tests.test_090_component_session_settlement import _close
from tests.test_133_readonly_reset_live_refusal import _closed_database_rows


def _router_source(
    node: str,
    *,
    interrupt: str | None = None,
    waiting: bool = False,
    wait_for: tuple[str, ...] | None = None,
    jobs: int = 0,
    import_marker: bool = False,
) -> str:
    declaration = "" if interrupt is None else f", interrupt={interrupt}"
    waiting_options = ""
    if waiting:
        waiting_options = f", waiting=True, wait_for={list(wait_for or ())!r}"
    creation = "" if jobs == 0 else f"router.create_job(number={jobs})"
    marker = ""
    if import_marker:
        marker = (
            "from pathlib import Path\n"
            "Path('user-router-imported.txt').write_text('imported', encoding='utf-8')\n"
        )
    return f"""
{marker}from micro_workflow_manager import NodeRouter
router = NodeRouter({node!r}, runner='direct'{declaration}{waiting_options})
{creation}
@router.task
def run(ctx):
    ctx.write_output('ran.txt', {node!r})
    return {node!r}
"""


def _make_interrupt_project(
    tmp_path,
    monkeypatch,
    *,
    edges,
    interrupt_nodes=(),
    jobs=(),
    waiting=None,
    import_marker=False,
):
    nodes = sorted({node for edge in edges for node in edge})
    waiting = waiting or {}
    # Register valid source before installing the declarations and import
    # sentinels observed by preflight.
    make_project(
        tmp_path,
        monkeypatch,
        edges=f"EDGES = {edges!r}",
        runner="direct",
        files={
            node: _router_source(
                node,
                waiting=node in waiting,
                wait_for=tuple(waiting.get(node, ())),
                jobs=1 if node in jobs else 0,
            )
            for node in nodes
        },
    )
    for node in nodes:
        source = _router_source(
            node,
            interrupt="True" if node in interrupt_nodes else None,
            waiting=node in waiting,
            wait_for=tuple(waiting.get(node, ())),
            jobs=1 if node in jobs else 0,
            import_marker=import_marker,
        )
        (tmp_path / "src" / "node_behavior" / f"{node}.py").write_text(
            textwrap.dedent(source).strip() + "\n", encoding="utf-8",
        )
    _close_without_sidecars(FileStorage(tmp_path), tmp_path)


def test_one_member_declaration_classifies_only_its_component_and_preserves_raw_waiting(
    tmp_path, monkeypatch, capsys,
):
    edges = [("P", "A"), ("A", "B"), ("B", "A"), ("B", "C")]
    _make_interrupt_project(
        tmp_path,
        monkeypatch,
        edges=edges,
        interrupt_nodes={"A"},
        jobs={"A"},
        waiting={"B": ("A",)},
    )
    before = _snapshot(tmp_path)
    capsys.readouterr()

    assert cli.main([
        "runfrom", "P", "--plan", "--interrupt-policy", "run-all",
    ]) == 0

    plan = capsys.readouterr().out.lower()
    assert "interrupt component {a, b}" in plan
    assert "canonical replay key: a" in plan
    assert "interrupt component {p}" not in plan
    assert "interrupt component {c}" not in plan
    assert _snapshot(tmp_path) == before

    assert cli.main([
        "runfrom", "P", "--interrupt-policy", "run-all", "--runner", "direct",
    ]) == 0
    workflow = load_workflow(tmp_path, "direct")
    try:
        a_schema = json.loads(workflow.storage.node_schema_file("A").read_text(encoding="utf-8"))
        b_schema = json.loads(workflow.storage.node_schema_file("B").read_text(encoding="utf-8"))
        assert workflow.nodes["A"].interrupt is True
        assert workflow.nodes["B"].interrupt is False
        assert a_schema["interrupt"] is True
        assert b_schema["interrupt"] is False
        assert workflow.nodes["B"].waiting is True
        assert workflow.nodes["B"].wait_for == ("A",)
        assert b_schema["waiting"] is True
        assert b_schema["wait_for"] == ["A"]
    finally:
        _close(workflow.storage)


@pytest.mark.parametrize("surface", ["router", "from-file", "workflow-task"])
def test_every_public_declaration_surface_propagates_exact_true(tmp_path, surface):
    workflow = MicroWorkflow(tmp_path, runner="direct", persist_graph=False)
    workflow.graph([("A", "A")])

    if surface == "workflow-task":
        @workflow.task("A", interrupt=True)
        def run(ctx):
            return "A"
    else:
        router = (
            NodeRouter("A", runner="direct", interrupt=True)
            if surface == "router"
            else NodeRouter.from_file(tmp_path / "A.py", runner="direct", interrupt=True)
        )

        @router.task
        def run(ctx):
            return "A"

        workflow.include_router(router)

    try:
        schema = json.loads(workflow.storage.node_schema_file("A").read_text(encoding="utf-8"))
        assert workflow.nodes["A"].interrupt is True
        assert schema["interrupt"] is True
    finally:
        _close(workflow.storage)


def test_later_false_declaration_cannot_clear_true(tmp_path):
    workflow = MicroWorkflow(tmp_path, runner="direct", persist_graph=False)
    workflow.graph([("A", "A")])
    first = workflow.ensure_node("A", interrupt=True)
    second = workflow.ensure_node("A", interrupt=False)
    try:
        assert first is second
        assert second.interrupt is True
    finally:
        _close(workflow.storage)


@pytest.mark.parametrize("expression", ["1", "'yes'", "declared_at_runtime"])
def test_nonboolean_or_dynamic_source_declaration_refuses_before_import_or_mutation(
    tmp_path, monkeypatch, capsys, expression,
):
    make_project(
        tmp_path,
        monkeypatch,
        edges="EDGES = [('A', 'A')]",
        runner="direct",
        files={"A": _router_source("A")},
    )
    invalid_source = f"""
        from pathlib import Path
        Path('user-router-imported.txt').write_text('imported', encoding='utf-8')
        from micro_workflow_manager import NodeRouter
        declared_at_runtime = True
        router = NodeRouter('A', runner='direct', interrupt={expression})
        @router.task
        def run(ctx):
            return 'A'
    """
    (tmp_path / "src" / "node_behavior" / "A.py").write_text(
        textwrap.dedent(invalid_source).strip() + "\n", encoding="utf-8",
    )
    _close_without_sidecars(FileStorage(tmp_path), tmp_path)
    before_rows = _closed_database_rows(tmp_path)
    before_files = _snapshot(tmp_path)
    capsys.readouterr()

    assert cli.main([
        "run", "A", "--plan", "--interrupt-policy", "run-all",
    ]) == 1

    captured = capsys.readouterr()
    output = (captured.out + captured.err).lower()
    assert "interrupt" in output
    assert "literal" in output and "boolean" in output
    assert _closed_database_rows(tmp_path) == before_rows
    assert _snapshot(tmp_path) == before_files
    assert not (tmp_path / "user-router-imported.txt").exists()


@pytest.mark.parametrize("surface", ["router", "from-file", "workflow-task", "ensure-node"])
@pytest.mark.parametrize("value", [None, 0, 1, "yes"])
def test_public_interrupt_declarations_reject_every_nonboolean_value(tmp_path, surface, value):
    workflow = MicroWorkflow(tmp_path, runner="direct", persist_graph=False)
    workflow.graph([("A", "A")])
    try:
        with pytest.raises(ValueError, match="interrupt.*Boolean"):
            if surface == "router":
                NodeRouter("A", interrupt=value)
            elif surface == "from-file":
                NodeRouter.from_file(tmp_path / "A.py", interrupt=value)
            elif surface == "workflow-task":
                workflow.task("A", interrupt=value)
            else:
                workflow.ensure_node("A", interrupt=value)
    finally:
        _close(workflow.storage)
