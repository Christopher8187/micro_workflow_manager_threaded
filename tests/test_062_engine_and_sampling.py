from __future__ import annotations

import hashlib
import importlib
import json
from pathlib import Path

from micro_workflow_manager import cli
from micro_workflow_manager.cli.engine import build_engine_snapshot, render_engine_html
from micro_workflow_manager.cli.project import load_workflow
from micro_workflow_manager.cli.sampling import plan_sample
from micro_workflow_manager.storage import FileStorage
from tests.state_helpers import seed_job


def _write_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")


def _tree_digest(root: Path) -> str:
    digest = hashlib.sha256()
    for path in sorted(item for item in root.rglob("*") if item.is_file()):
        digest.update(path.relative_to(root).as_posix().encode("utf-8"))
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


def _job_output(state: FileStorage, node: str, job_id: int) -> dict:
    return state.read_json(state.output_file(node, job_id))


def _make_engine_project(root: Path) -> None:
    _write_json(
        root / ".mwf" / "project.json",
        {
            "version": 4,
            "schema_version": 4,
            "graph_path": "src/graph.py",
            "runner": "threaded",
            "edges": [["A", "B"], ["B", "C"]],
        },
    )
    behavior = root / "src" / "node_behavior"
    behavior.mkdir(parents=True)
    (root / "src" / "graph.py").write_text(
        "EDGES = [('A', 'B'), ('B', 'C')]\n",
        encoding="utf-8",
    )
    (behavior / "A.py").write_text(
        """
from micro_workflow_manager import NodeRouter
router = NodeRouter("A")
@router.task
def run(ctx):
    ctx.node("B").add(autostart=True)
""".strip()
        + "\n",
        encoding="utf-8",
    )


def test_engine_snapshot_collapses_hoeflein_component_and_render_is_graph_only(tmp_path):
    _make_engine_project(tmp_path)
    before = _tree_digest(tmp_path)

    snapshot = build_engine_snapshot(tmp_path)
    html = render_engine_html(snapshot)

    assert _tree_digest(tmp_path) == before
    assert [node["members"] for node in snapshot["nodes"]] == [["A", "B"], ["C"]]
    assert snapshot["edges"] == [{"source": "component-1", "target": "component-2"}]
    assert "HOEFLEIN COMPONENT" in html
    assert "Architecture" not in html
    assert "Runtime" not in html
    assert "Contracts" not in html
    assert "toolbar" not in html
    assert "https://" not in html


def test_engine_dispatches_before_runtime_layout(tmp_path, monkeypatch):
    _make_engine_project(tmp_path)
    monkeypatch.chdir(tmp_path)
    dispatch = cli.main
    main_module = importlib.import_module("micro_workflow_manager.cli.main")
    # Importing a child module named main temporarily assigns that module to the
    # package attribute. Preserve the public callable used by the whole suite.
    cli.main = dispatch
    observed = []
    monkeypatch.setattr(main_module, "engine_command", lambda root: observed.append(root) or 0)
    monkeypatch.setattr(
        main_module,
        "ensure_runtime_layout",
        lambda root: (_ for _ in ()).throw(AssertionError("engine touched runtime layout")),
    )

    assert dispatch(["engine"]) == 0
    assert observed == [tmp_path]


def _make_sample_project(tmp_path: Path, monkeypatch) -> FileStorage:
    monkeypatch.chdir(tmp_path)
    behavior = tmp_path / "src" / "node_behavior"
    behavior.mkdir(parents=True)
    (tmp_path / "src" / "graph.py").write_text(
        "EDGES = [('gate', 'work')]\n",
        encoding="utf-8",
    )
    (behavior / "gate.py").write_text(
        """
from micro_workflow_manager import NodeRouter
router = NodeRouter("gate")
router.create_job(params={"value": 1})
@router.task
def run(ctx, value):
    return value
""".strip()
        + "\n",
        encoding="utf-8",
    )
    (behavior / "work.py").write_text(
        """
from micro_workflow_manager import NodeRouter
router = NodeRouter("work")
@router.task
def run(ctx, value):
    return {"value": value, "sampled": True}
""".strip()
        + "\n",
        encoding="utf-8",
    )
    assert cli.main(["init"]) == 0
    assert cli.main(["graph", "src/graph.py", "--runner", "direct"]) == 0
    state = FileStorage(tmp_path)
    for job_id in range(1, 21):
        seed_job(tmp_path, "work", job_id, "done", params={"value": job_id})
        state.write_output("work", job_id, {"marker": job_id})
    state.db_mutation_barrier()
    return state


def test_sample_plan_is_deterministic_and_replay_guard_detects_drift(tmp_path, monkeypatch, capsys):
    state = _make_sample_project(tmp_path, monkeypatch)
    capsys.readouterr()
    workflow = load_workflow(tmp_path, "direct")
    first = plan_sample(workflow, "work", 5, seed="acceptance")
    second = plan_sample(workflow, "work", 5, seed="acceptance")
    other = plan_sample(workflow, "work", 5, seed="other")

    assert first.selected_job_ids == second.selected_job_ids
    assert first.population_digest == second.population_digest
    assert first.selected_job_ids != other.selected_job_ids

    state.set_job_status("work", 1, "failed", error="changed")
    state.db_mutation_barrier()
    assert cli.main(
        [
            "run", "work", "sample", "5", "--seed", "acceptance",
            "--expect-population", first.population_digest,
            "--runner", "direct",
        ]
    ) == 1
    error = capsys.readouterr().err
    assert "Sample population changed" in error


def test_sample_run_bypasses_readiness_and_preserves_unselected_jobs(tmp_path, monkeypatch, capsys):
    state = _make_sample_project(tmp_path, monkeypatch)
    capsys.readouterr()
    workflow = load_workflow(tmp_path, "direct")
    plan = plan_sample(workflow, "work", 6, seed="partial-node")
    selected = set(plan.selected_job_ids)
    unselected = set(range(1, 21)) - selected

    # gate is still queued, so an ordinary `mwf run work` would be refused.
    assert state.get_node_status("gate") == "queued"
    assert cli.main(
        ["run", "work", "sample", "6", "--seed", "partial-node", "--runner", "direct"]
    ) == 0
    output = capsys.readouterr().out
    assert "isolated work sample" in output

    for job_id in selected:
        assert state.get_job_status("work", job_id) == "done"
        assert _job_output(state, "work", job_id)["result_repr"] == repr(
            {"value": job_id, "sampled": True}
        )
    for job_id in unselected:
        assert state.get_job_status("work", job_id) == "done"
        assert _job_output(state, "work", job_id) == {"marker": job_id}

    run_state = state.get_run_state()
    assert run_state["command"] == "run sample"
    assert run_state["selected_jobs"] == sorted(selected)
    assert run_state["selection"]["algorithm"] == "mwf.sample.v1"
    assert run_state["selection"]["population_digest"] == plan.population_digest


def test_sample_status_filter_and_plan_are_read_only(tmp_path, monkeypatch, capsys):
    state = _make_sample_project(tmp_path, monkeypatch)
    for job_id in (2, 4, 6, 8):
        state.set_job_status("work", job_id, "failed", error="fixture")
    state.db_mutation_barrier()
    capsys.readouterr()
    before = {
        job_id: (state.get_job_status("work", job_id), _job_output(state, "work", job_id))
        for job_id in range(1, 21)
    }

    assert cli.main(
        [
            "run", "work", "sample", "3", "--seed", "failed-only",
            "--status", "failed", "--plan", "--runner", "direct",
        ]
    ) == 0
    out = capsys.readouterr().out
    selected_line = next(line for line in out.splitlines() if line.strip().startswith("job IDs:"))
    selected = {int(value) for value in selected_line.split(":", 1)[1].split()}
    assert selected <= {2, 4, 6, 8}
    assert len(selected) == 3
    after = {
        job_id: (state.get_job_status("work", job_id), _job_output(state, "work", job_id))
        for job_id in range(1, 21)
    }
    assert after == before
