from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest

from micro_workflow_manager import cli
from micro_workflow_manager.storage import FileStorage


def _write_filter_project(root: Path) -> None:
    behavior = root / "src" / "node_behavior"
    behavior.mkdir(parents=True)
    (root / "src" / "graph.py").write_text(
        'EDGES = [("filter_numbers", "finished")]\n',
        encoding="utf-8",
    )
    (behavior / "filter_numbers.py").write_text(
        '''from micro_workflow_manager import NodeRouter
router = NodeRouter("filter_numbers")
router.create_job(number=10)

@router.task(retries=1)
def filter_numbers(ctx):
    value = ctx.job_id
    if value >= 8 or (value >= 5 and ctx.attempt == 1):
        raise ValueError(f"main rejected {value} on attempt {ctx.attempt}")
    return value

@router.fallback(name="broader_filter", retries=1)
def broader_filter(ctx, error):
    value = ctx.job_id
    if value >= 10 or (value >= 9 and ctx.attempt == 1):
        raise ValueError(f"fallback rejected {value} on attempt {ctx.attempt}")
    return value
''',
        encoding="utf-8",
    )
    (behavior / "finished.py").write_text(
        '''from micro_workflow_manager import NodeRouter
router = NodeRouter("finished")
@router.task
def finished(ctx):
    return None
''',
        encoding="utf-8",
    )


def test_filter_command_reconstructs_funnel_and_stage_boundaries(
    tmp_path: Path,
    monkeypatch,
    capsys,
):
    monkeypatch.chdir(tmp_path)
    _write_filter_project(tmp_path)
    assert cli.main(["init"]) == 0
    assert cli.main(["graph", "src/graph.py", "--runner", "direct"]) == 0
    capsys.readouterr()

    assert cli.main(["run", "filter_numbers"]) == 1
    capsys.readouterr()

    storage = FileStorage(tmp_path)
    storage.append_job_event(
        "filter_numbers",
        5,
        "retry_started",
        task="filter_numbers",
        attempt=2,
        attempts=2,
        previous_error="transition data must not win",
    )
    storage.write_output(
        "filter_numbers",
        10,
        {"status": "failed", "error": "output data must not win"},
    )

    assert cli.main(["filter", "filter_numbers"]) == 0
    output = capsys.readouterr().out

    assert "Filter funnel for node filter_numbers" in output
    assert "main: filter_numbers — attempt 1/2" in output
    assert "main: filter_numbers — attempt 2/2" in output
    assert "fallback: broader_filter — attempt 1/2" in output
    assert "fallback: broader_filter — attempt 2/2" in output
    assert "entered   passed  remaining" in output
    assert "attempt 1/2" in output and "      10        4        6" in output
    assert "attempt 2/2" in output and "       6        3        3" in output
    assert "       3        1        2" in output
    assert "       2        1        1" in output
    assert "Failed jobs" not in output
    assert "10: ValueError('fallback rejected 10 on attempt 2')" not in output

    assert cli.main(["filter", "filter_numbers", "stage", "1"]) == 0
    stage_one = capsys.readouterr().out
    assert "Filter stage 1/4" in stage_one
    assert "5: ValueError('main rejected 5 on attempt 1')" in stage_one
    assert "6: ValueError('main rejected 6 on attempt 1')" in stage_one
    assert "7: ValueError('main rejected 7 on attempt 1')" in stage_one
    assert "8:" not in stage_one
    assert "transition data must not win" not in stage_one

    assert cli.main(["filter", "filter_numbers", "stage", "4"]) == 0
    final_stage = capsys.readouterr().out
    assert "Filter stage 4/4" in final_stage
    assert final_stage.rstrip().endswith(
        "10: ValueError('fallback rejected 10 on attempt 2')"
    )
    assert "output data must not win" not in final_stage

    assert cli.main(["inspect", "filter_numbers", "filter"]) == 1
    assert "Use: mwf inspect" in capsys.readouterr().err


def test_init_gitignore_and_material_icons_cover_runtime_structure(
    tmp_path: Path,
    monkeypatch,
):
    monkeypatch.chdir(tmp_path)
    behavior = tmp_path / "src" / "node_behavior"
    behavior.mkdir(parents=True)
    (tmp_path / "src" / "graph.py").write_text(
        'EDGES = [("ingest", "publish")]\n',
        encoding="utf-8",
    )
    for node in ("ingest", "publish"):
        (behavior / f"{node}.py").write_text(
            "from micro_workflow_manager import NodeRouter\n"
            f"router = NodeRouter({node!r})\n"
            "@router.task\n"
            "def run(ctx): return None\n",
            encoding="utf-8",
        )

    assert cli.main(["init"]) == 0
    assert cli.main(["graph", "src/graph.py"]) == 0

    gitignore = (tmp_path / ".gitignore").read_text(encoding="utf-8")
    assert ".mwf/" in gitignore
    assert "node/*/jobs/**" in gitignore
    assert "node/*/idempotency/**" in gitignore
    assert "clipboard/*/idempotency/**" in gitignore
    assert "clipboard/*/.mwf-node-state.sqlite3" in gitignore
    assert (tmp_path / ".mwf" / "state.sqlite3").is_file()

    settings = json.loads(
        (tmp_path / ".vscode" / "settings.json").read_text(encoding="utf-8")
    )
    files = settings["material-icon-theme.files.associations"]
    folders = settings["material-icon-theme.folders.associations"]
    assert files["graph.py"] == "routing"
    assert folders["clipboard"] == "archive"
    assert "node" not in folders
    assert folders["queued"] == "queue"
    assert folders["idempotency"] == "keys"
    assert folders["ingest"] == "flow"
    assert folders["publish"] == "flow"


def test_readme_routes_documentation_and_requires_output_provenance():
    root = Path(__file__).resolve().parents[1]
    readme = (root / "README.md").read_text(encoding="utf-8")
    agents = (root / "AGENTS.md").read_text(encoding="utf-8")
    task = (root / "docs" / "architecture" / "task.md").read_text(
        encoding="utf-8"
    )
    assert "# micro-workflow-manager 0.6.1" in readme
    for target in (
        "CONTEXT.md",
        "docs/architecture/graph.md",
        "docs/architecture/node.md",
        "docs/architecture/task.md",
        "docs/operations.md",
        "docs/installation.md",
        "docs/testing.md",
        "tests/README.md",
        "benchmarks/README.md",
        "docs/release-history.md",
        "docs/plans/0.6.4.md",
    ):
        assert (root / target).is_file()
        assert f"]({target})" in readme
    for skill in (
        "mwf-design-new-architecture",
        "mwf-modify-architecture",
        "mwf-analyze-architecture",
        "mwf-test",
        "mwf-document-workflow",
    ):
        assert (root / ".agents" / "skills" / skill / "SKILL.md").is_file()
        assert f"`{skill}`" in agents
    assert "## Durable result and output provenance" in task
    assert "Every node has one framework output prefix" in task
    assert "Project provenance" not in task
    for legacy in ("AGENT.md", "DESIGN.md", "HOW_TO_TEST.md"):
        assert not (root / legacy).exists()


EXAMPLE_STARTS = {
    "document_refinery": "discover_sources",
    "geometry_solver_lab": "parse_construction",
    "agent_prompt_chain": "draft_brief",
    "agent_router": "classify_request",
    "agent_parallelization": "fan_out",
    "agent_orchestrator_workers": "plan_work",
    "agent_evaluator_optimizer": "generate_candidate",
    "database_change_manager": "plan_schema_change",
    "pygame_state_machine": "load_game_session",
}


@pytest.mark.parametrize("example_name,start_node", EXAMPLE_STARTS.items())
def test_design_example_runs_and_writes_provenance(
    example_name: str,
    start_node: str,
    tmp_path: Path,
    monkeypatch,
    capsys,
):
    source = Path(__file__).resolve().parents[1] / "examples" / example_name
    project = tmp_path / example_name
    shutil.copytree(source, project, ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
    monkeypatch.chdir(project)

    assert cli.main(["init"]) == 0
    assert cli.main(["graph", "src/graph.py", "--runner", "direct"]) == 0
    assert cli.main(["runfrom", start_node]) == 0
    capsys.readouterr()

    provenance = list((project / "node").glob("*/output/provenance/*.json"))
    assert provenance, f"{example_name} wrote no output provenance"
    for path in provenance:
        record = json.loads(path.read_text(encoding="utf-8"))
        assert record["node"]
        assert record["job_id"] >= 1
        assert "inputs" in record
        assert "decisions" in record
        assert "result" in record
