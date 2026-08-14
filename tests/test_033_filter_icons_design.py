from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest

from micro_workflow_manager import cli


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

    assert cli.main(["filter", "filter_numbers", "stage", "4"]) == 0
    final_stage = capsys.readouterr().out
    assert "Filter stage 4/4" in final_stage
    assert final_stage.rstrip().endswith(
        "10: ValueError('fallback rejected 10 on attempt 2')"
    )

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


def test_readme_links_design_and_requires_output_provenance():
    root = Path(__file__).resolve().parents[1]
    readme = (root / "README.md").read_text(encoding="utf-8")
    design = (root / "DESIGN.md").read_text(encoding="utf-8")
    assert "# micro-workflow-manager 0.5.11" in readme
    assert "[DESIGN.md](DESIGN.md)" in readme
    assert "provenance" in readme.lower()
    assert "## Advice first" in design
    assert "Prompt chaining" in design
    assert "Database change manager" in design
    assert "Pygame state machine" in design
    agent = (root / "AGENT.md").read_text(encoding="utf-8")
    assert "mwf run NODE --monitor" in agent
    assert "Reduce concurrency first" in agent
    assert "test-code freezing from framework freezing" in agent
    assert "Repeat-use matrix" in agent
    assert "STUBBORN_ISSUE.md" in agent


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
