import json
from pathlib import Path

from micro_workflow_manager import cli


def make_cli_project(tmp_path: Path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    src = tmp_path / "src"
    behavior = src / "node_behavior"
    behavior.mkdir(parents=True)
    (src / "graph.py").write_text(
        "EDGES = [('alpha', 'beta'), ('alpha', 'gamma')]\n",
        encoding="utf-8",
    )

    assert cli.main(["init"]) == 0
    assert cli.main(["graph", "src/graph.py"]) == 0


def write_json(path: Path, data: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data), encoding="utf-8")


def seed_dirty_node(tmp_path: Path, node: str):
    node_dir = tmp_path / "node" / node
    (node_dir / "input" / "keep.txt").write_text("input", encoding="utf-8")
    (node_dir / "output" / "remove.txt").write_text("output", encoding="utf-8")
    job_dir = node_dir / "jobs" / "1"
    write_json(job_dir / "job.json", {"job_id": 1, "node_name": node, "created_at": "test", "parent": None})
    write_json(job_dir / "input.json", {"value": node})
    write_json(job_dir / "status.json", {"job_id": 1, "node_name": node, "status": "done"})
    write_json(job_dir / "output.json", {"done": True})
    (job_dir / "files").mkdir(parents=True, exist_ok=True)
    (job_dir / "files" / "debug.txt").write_text("remove", encoding="utf-8")


def test_top_level_help_points_to_command_help_and_describe(capsys):
    assert cli.main([]) == 0
    out = capsys.readouterr().out

    assert "mwf <command> --help" in out
    assert "mwf clean --help" in out
    assert "mwf --describe runfrom" in out
    assert "mwf clean *" in out
    assert "mwf wipe *" in out


def test_describe_explains_command_context_and_current_project(tmp_path, monkeypatch, capsys):
    make_cli_project(tmp_path, monkeypatch)
    capsys.readouterr()

    assert cli.main(["--describe", "clean"]) == 0
    out = capsys.readouterr().out

    assert "mwf clean" in out
    assert "Code context:" in out
    assert "File-system context" in out
    assert "Current directory context:" in out
    assert f"project root: {tmp_path}" in out
    assert "graph path: src/graph.py" in out
    assert "nodes on disk: alpha, beta, gamma" in out
    assert "More syntax help: mwf clean --help" in out


def test_clean_star_cleans_all_nodes_but_preserves_inputs(tmp_path, monkeypatch, capsys):
    make_cli_project(tmp_path, monkeypatch)
    seed_dirty_node(tmp_path, "alpha")
    seed_dirty_node(tmp_path, "beta")
    capsys.readouterr()

    assert cli.main(["clean", "*"]) == 0
    out = capsys.readouterr().out

    assert "Cleaned all nodes: alpha, beta, gamma" in out
    for node in ["alpha", "beta"]:
        node_dir = tmp_path / "node" / node
        assert (node_dir / "input" / "keep.txt").read_text(encoding="utf-8") == "input"
        assert not (node_dir / "output" / "remove.txt").exists()
        assert (node_dir / "jobs").is_dir()
        assert not (node_dir / "jobs" / "1").exists()
        node_status = json.loads((node_dir / "node_state.json").read_text(encoding="utf-8"))
        assert node_status["status"] == "queued"


def test_wipe_star_wipes_all_nodes_and_removes_inputs(tmp_path, monkeypatch, capsys):
    make_cli_project(tmp_path, monkeypatch)
    seed_dirty_node(tmp_path, "alpha")
    seed_dirty_node(tmp_path, "gamma")
    capsys.readouterr()

    assert cli.main(["wipe", "*"]) == 0
    out = capsys.readouterr().out

    assert "Wiped all nodes: alpha, beta, gamma" in out
    for node in ["alpha", "gamma"]:
        node_dir = tmp_path / "node" / node
        assert (node_dir / "input").is_dir()
        assert not (node_dir / "input" / "keep.txt").exists()
        assert not (node_dir / "output" / "remove.txt").exists()
        assert (node_dir / "jobs").is_dir()
        assert not (node_dir / "jobs" / "1").exists()
        node_status = json.loads((node_dir / "node_state.json").read_text(encoding="utf-8"))
        assert node_status["status"] == "queued"


def test_clean_star_also_works_when_shell_expands_star(tmp_path, monkeypatch, capsys):
    make_cli_project(tmp_path, monkeypatch)
    seed_dirty_node(tmp_path, "alpha")
    capsys.readouterr()

    expanded_star = sorted(path.name for path in tmp_path.iterdir() if not path.name.startswith("."))
    assert cli.main(["clean", *expanded_star]) == 0
    out = capsys.readouterr().out

    assert "Cleaned all nodes: alpha, beta, gamma" in out
    assert not (tmp_path / "node" / "alpha" / "output" / "remove.txt").exists()


def make_chain_project(tmp_path: Path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    src = tmp_path / "src"
    behavior = src / "node_behavior"
    behavior.mkdir(parents=True)
    (src / "graph.py").write_text(
        "EDGES = [('A', 'B'), ('B', 'C')]\n",
        encoding="utf-8",
    )
    (behavior / "A.py").write_text(
        """
from micro_workflow_manager import NodeRouter
router = NodeRouter("A")
router.create_job(params={"value": "from A"})
@router.task
def run(ctx, value):
    ctx.node("B").add(value=value)
    return value
""".strip(),
        encoding="utf-8",
    )
    (behavior / "B.py").write_text(
        """
from micro_workflow_manager import NodeRouter
router = NodeRouter("B")
@router.task
def run(ctx, value):
    ctx.node("C").add(value=value + " then B")
    return value
""".strip(),
        encoding="utf-8",
    )
    (behavior / "C.py").write_text(
        """
from micro_workflow_manager import NodeRouter
router = NodeRouter("C")
@router.task
def run(ctx, value):
    return value
""".strip(),
        encoding="utf-8",
    )

    assert cli.main(["init"]) == 0
    assert cli.main(["graph", "src/graph.py", "--runner", "direct"]) == 0


def test_run_b_after_run_a_keeps_a_finished_status(tmp_path, monkeypatch, capsys):
    make_chain_project(tmp_path, monkeypatch)
    capsys.readouterr()

    assert cli.main(["run", "A", "--runner", "direct"]) == 0
    capsys.readouterr()
    assert json.loads((tmp_path / "node" / "A" / "node_state.json").read_text())["status"] == "done"

    assert cli.main(["run", "B", "--runner", "direct"]) == 0
    out = capsys.readouterr().out

    assert "Ran:" in out
    assert "  B" in out
    assert json.loads((tmp_path / "node" / "A" / "node_state.json").read_text())["status"] == "done"
    assert json.loads((tmp_path / "node" / "B" / "node_state.json").read_text())["status"] == "done"


def test_cleaning_a_removes_finished_status_and_blocks_b(tmp_path, monkeypatch, capsys):
    make_chain_project(tmp_path, monkeypatch)
    capsys.readouterr()

    assert cli.main(["run", "A", "--runner", "direct"]) == 0
    capsys.readouterr()

    assert cli.main(["clean", "A"]) == 0
    capsys.readouterr()
    assert json.loads((tmp_path / "node" / "A" / "node_state.json").read_text())["status"] == "queued"
    assert not (tmp_path / "node" / "A" / "jobs" / "1").exists()

    assert cli.main(["run", "B", "--runner", "direct"]) == 1
    out = capsys.readouterr().out

    assert "B is not ready yet." in out
    assert "Previous nodes not finished:" in out
    assert "A: queued" in out
