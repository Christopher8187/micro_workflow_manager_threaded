import json
import shutil
import subprocess
from pathlib import Path

import pytest

from micro_workflow_manager import cli
from micro_workflow_manager.storage import FileStorage
from tests.state_helpers import seed_job


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


def read_status_or_queued(path: Path) -> str:
    root = path.parents[4]
    node = path.parents[2].name
    job_id = int(path.parent.name)
    return FileStorage(root).get_job_status(node, job_id) or "queued"


def seed_dirty_node(tmp_path: Path, node: str):
    node_dir = tmp_path / "node" / node
    (node_dir / "input" / "keep.txt").write_text("input", encoding="utf-8")
    (node_dir / "output" / "remove.txt").write_text("output", encoding="utf-8")
    state = seed_job(tmp_path, node, 1, "done", params={"value": node})
    state.write_output(node, 1, {"done": True})
    files = state.files_dir(node, 1)
    (files / "debug.txt").write_text("remove", encoding="utf-8")


def test_top_level_help_points_to_command_help_and_describe(capsys):
    assert cli.main([]) == 0
    out = capsys.readouterr().out

    assert "mwf <command> --help" in out
    assert "mwf clean --help" in out
    assert "mwf --describe runfrom" in out
    assert "mwf clean *" in out
    assert "mwf reset *" in out
    assert "mwf wipe *" in out
    assert "mwf restart <node-name> job 42" in out


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
        assert FileStorage(tmp_path).get_node_status(node) == "queued"


def test_reset_star_preserves_job_definitions_and_requeues_jobs(tmp_path, monkeypatch, capsys):
    make_cli_project(tmp_path, monkeypatch)
    seed_dirty_node(tmp_path, "alpha")
    seed_dirty_node(tmp_path, "beta")
    capsys.readouterr()

    assert cli.main(["reset", "*"]) == 0
    out = capsys.readouterr().out

    assert "Reset all nodes: alpha, beta, gamma" in out
    for node in ["alpha", "beta"]:
        node_dir = tmp_path / "node" / node
        job_dir = node_dir / "jobs" / "1"
        assert (node_dir / "input" / "keep.txt").read_text(encoding="utf-8") == "input"
        assert not (node_dir / "output" / "remove.txt").exists()
        assert FileStorage(tmp_path).job_exists(node, 1)
        assert json.loads((job_dir / "input.json").read_text(encoding="utf-8")) == {"value": node}
        assert read_status_or_queued(job_dir / "status.json") == "queued"
        assert not (job_dir / "output.json").exists()
        assert not (job_dir / "files" / "debug.txt").exists()
        assert FileStorage(tmp_path).get_node_status(node) == "queued"


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
        assert FileStorage(tmp_path).get_node_status(node) == "queued"


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
    assert FileStorage(tmp_path).get_node_status("A") == "done"

    assert cli.main(["run", "B", "--runner", "direct"]) == 0
    out = capsys.readouterr().out

    assert "Ran:" in out
    assert "  B" in out
    assert FileStorage(tmp_path).get_node_status("A") == "done"
    assert FileStorage(tmp_path).get_node_status("B") == "done"


def test_cleaning_a_removes_finished_status_and_blocks_b(tmp_path, monkeypatch, capsys):
    make_chain_project(tmp_path, monkeypatch)
    capsys.readouterr()

    assert cli.main(["run", "A", "--runner", "direct"]) == 0
    capsys.readouterr()

    assert cli.main(["clean", "A"]) == 0
    capsys.readouterr()
    assert FileStorage(tmp_path).get_node_status("A") == "queued"
    assert not (tmp_path / "node" / "A" / "jobs" / "1").exists()

    assert cli.main(["run", "B", "--runner", "direct"]) == 1
    out = capsys.readouterr().out

    assert "Cannot run B: incomplete predecessor components: A" in out
    assert "Hoeflein components are scheduled on the quotient DAG" in out
    assert "A: queued" in out


def make_runfrom_default_descendant_project(tmp_path: Path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    src = tmp_path / "src"
    behavior = src / "node_behavior"
    behavior.mkdir(parents=True)
    (src / "graph.py").write_text(
        "EDGES = [('split', 'tagify'), ('tagify', 'disintegrate')]\n",
        encoding="utf-8",
    )
    (behavior / "split.py").write_text(
        """
from micro_workflow_manager import NodeRouter
router = NodeRouter("split")
router.create_job(number=1)
@router.task
def run(ctx):
    ctx.node("tagify").add(value="page 1")
    return "split"
""".strip(),
        encoding="utf-8",
    )
    (behavior / "tagify.py").write_text(
        """
from micro_workflow_manager import NodeRouter
router = NodeRouter("tagify")
@router.task
def run(ctx, value):
    ctx.node("disintegrate").write_input("page.txt", value, overwrite=True)
    return value
""".strip(),
        encoding="utf-8",
    )
    (behavior / "disintegrate.py").write_text(
        """
from micro_workflow_manager import NodeRouter
router = NodeRouter("disintegrate")
router.create_job(number=1)
@router.task
def run(ctx):
    text = ctx.input_path("page.txt").read_text(encoding="utf-8")
    ctx.write("combined.txt", text)
    return text
""".strip(),
        encoding="utf-8",
    )

    assert cli.main(["init"]) == 0
    assert cli.main(["graph", "src/graph.py", "--runner", "direct"]) == 0


def test_runfrom_preserves_router_created_jobs_on_descendant_nodes(tmp_path, monkeypatch, capsys):
    make_runfrom_default_descendant_project(tmp_path, monkeypatch)
    capsys.readouterr()

    assert FileStorage(tmp_path).job_exists("disintegrate", 1)

    assert cli.main(["runfrom", "split", "--runner", "direct"]) == 0
    out = capsys.readouterr().out

    assert "Ran:" in out
    assert "  split" in out
    assert "  tagify" in out
    assert "  disintegrate" in out
    assert FileStorage(tmp_path).job_exists("disintegrate", 1)
    assert (tmp_path / "node" / "disintegrate" / "jobs" / "1" / "files" / "combined.txt").read_text(encoding="utf-8") == "page 1"
    assert FileStorage(tmp_path).get_node_status("disintegrate") == "done"

    assert cli.main(["runfrom", "split", "--runner", "direct"]) == 0
    capsys.readouterr()
    assert len(list((tmp_path / "node" / "tagify" / "jobs").iterdir())) == 1
    assert (tmp_path / "node" / "disintegrate" / "jobs" / "1" / "files" / "combined.txt").read_text(encoding="utf-8") == "page 1"


def make_job_selection_project(tmp_path: Path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    src = tmp_path / "src"
    behavior = src / "node_behavior"
    behavior.mkdir(parents=True)
    (src / "graph.py").write_text(
        "EDGES = [('work', 'after')]\n",
        encoding="utf-8",
    )
    (behavior / "work.py").write_text(
        """
from micro_workflow_manager import NodeRouter
router = NodeRouter("work")
router.create_job(number=10)
@router.task
def run(ctx):
    ctx.write("job.txt", f"job {ctx.job_id}")
    return ctx.job_id
""".strip(),
        encoding="utf-8",
    )
    (behavior / "after.py").write_text(
        """
from micro_workflow_manager import NodeRouter
router = NodeRouter("after")
@router.task
def run(ctx):
    return "after"
""".strip(),
        encoding="utf-8",
    )

    assert cli.main(["init"]) == 0
    assert cli.main(["graph", "src/graph.py", "--runner", "direct"]) == 0


def test_run_job_selection_runs_individual_jobs_and_ranges_only(tmp_path, monkeypatch, capsys):
    make_job_selection_project(tmp_path, monkeypatch)
    capsys.readouterr()

    assert cli.main(["run", "work", "job", "1", "3", "8-10", "--runner", "direct"]) == 0
    out = capsys.readouterr().out

    assert "Ran jobs for work:" in out
    for job_id in [1, 3, 8, 9, 10]:
        assert f"  {job_id}" in out
        assert (tmp_path / "node" / "work" / "jobs" / str(job_id) / "files" / "job.txt").read_text(encoding="utf-8") == f"job {job_id}"

    for job_id in [2, 4, 5, 6, 7]:
        assert not (tmp_path / "node" / "work" / "jobs" / str(job_id) / "files" / "job.txt").exists()
        status_path = tmp_path / "node" / "work" / "jobs" / str(job_id) / "status.json"
        assert read_status_or_queued(status_path) == "queued"

    assert FileStorage(tmp_path).get_node_status("work") == "queued"


def test_run_job_selection_resets_only_selected_job_artifacts(tmp_path, monkeypatch, capsys):
    make_job_selection_project(tmp_path, monkeypatch)
    capsys.readouterr()

    assert cli.main(["run", "work", "job", "2", "--runner", "direct"]) == 0
    capsys.readouterr()

    selected_file = tmp_path / "node" / "work" / "jobs" / "2" / "files" / "job.txt"
    stale_file = tmp_path / "node" / "work" / "jobs" / "2" / "files" / "stale.txt"
    other_status = tmp_path / "node" / "work" / "jobs" / "3" / "status.json"
    stale_file.write_text("old", encoding="utf-8")

    assert cli.main(["run", "work", "job", "2", "--runner", "direct"]) == 0
    capsys.readouterr()

    assert selected_file.read_text(encoding="utf-8") == "job 2"
    assert not stale_file.exists()
    assert read_status_or_queued(other_status) == "queued"


def test_run_job_selection_rejects_bad_selectors(tmp_path, monkeypatch, capsys):
    make_job_selection_project(tmp_path, monkeypatch)
    capsys.readouterr()

    assert cli.main(["run", "work", "job", "3-1", "--runner", "direct"]) == 1
    err = capsys.readouterr().err
    assert "Invalid job range: 3-1" in err

    assert cli.main(["run", "work", "job", "999", "--runner", "direct"]) == 1
    err = capsys.readouterr().err
    assert "Job does not exist: work/999" in err


def test_init_creates_vscode_settings_and_gitignore(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)

    assert cli.main(["init"]) == 0
    capsys.readouterr()

    settings = json.loads((tmp_path / ".vscode" / "settings.json").read_text(encoding="utf-8"))
    assert settings["workbench.iconTheme"] == "material-icon-theme"
    assert settings["material-icon-theme.files.associations"][".mwfignore"] == "routing"
    for key in ["files.exclude", "search.exclude"]:
        assert settings[key]["**/*.egg-info"] is True
        assert settings[key]["**/__pycache__"] is True
        assert settings[key]["**/.pytest_cache"] is True
        assert settings[key]["**/.mwf"] is True

    gitignore = (tmp_path / ".gitignore").read_text(encoding="utf-8")
    for entry in [
        ".mwf/",
        "node/*/input/*/",
        "node/*/jobs/**",
        "node/*/output/*/",
        "node/*/queued/**",
        "node/*/node_state.json",
        "node/*/job_index.json",
        "node/*/default_jobs.json",
        "node/*/schema.json",
        "clipboard/*/input/*/",
        "clipboard/*/jobs/**",
        "clipboard/*/output/*/",
        "clipboard/*/queued/**",
        "clipboard/*/node_state.json",
        "clipboard/*/job_index.json",
        "clipboard/*/job_index.dirty",
        "clipboard/*/default_jobs.json",
        "clipboard/*/schema.json",
        "*.egg-info/",
        "__pycache__/",
        ".pytest_cache/",
    ]:
        assert entry in gitignore

    assert "node/*/input/**" not in gitignore
    assert "node/*/output/**" not in gitignore


def test_init_gitignore_keeps_direct_input_output_files_but_ignores_nested(tmp_path, monkeypatch, capsys):
    git = shutil.which("git")
    if git is None:
        pytest.skip("git executable is required to verify .gitignore matching semantics")

    monkeypatch.chdir(tmp_path)

    assert cli.main(["init"]) == 0
    capsys.readouterr()

    paths = [
        "node/work/input/root.txt",
        "node/work/input/nested/page.txt",
        "node/work/output/result.txt",
        "node/work/output/images/page.png",
        "node/work/jobs/1/job.json",
        "node/work/queued/1.queued",
        "clipboard/work/input/root.txt",
        "clipboard/work/input/nested/page.txt",
        "clipboard/work/output/result.txt",
        "clipboard/work/output/images/page.png",
        "clipboard/work/jobs/1/job.json",
        "clipboard/work/queued/1.queued",
        "clipboard/work/node_state.json",
        "clipboard/work/job_index.json",
        "clipboard/work/job_index.dirty",
        "clipboard/work/default_jobs.json",
        "clipboard/work/schema.json",
    ]
    for rel in paths:
        path = tmp_path / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("x", encoding="utf-8")

    subprocess.run([git, "init"], cwd=tmp_path, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    def is_ignored(rel: str) -> bool:
        return subprocess.run(
            [git, "check-ignore", "-q", rel],
            cwd=tmp_path,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        ).returncode == 0

    assert not is_ignored("node/work/input/root.txt")
    assert is_ignored("node/work/input/nested/page.txt")
    assert not is_ignored("node/work/output/result.txt")
    assert is_ignored("node/work/output/images/page.png")
    assert is_ignored("node/work/jobs/1/job.json")
    assert is_ignored("node/work/queued/1.queued")

    assert not is_ignored("clipboard/work/input/root.txt")
    assert is_ignored("clipboard/work/input/nested/page.txt")
    assert not is_ignored("clipboard/work/output/result.txt")
    assert is_ignored("clipboard/work/output/images/page.png")
    assert is_ignored("clipboard/work/jobs/1/job.json")
    assert is_ignored("clipboard/work/queued/1.queued")
    assert is_ignored("clipboard/work/node_state.json")
    assert is_ignored("clipboard/work/job_index.json")
    assert is_ignored("clipboard/work/job_index.dirty")
    assert is_ignored("clipboard/work/default_jobs.json")
    assert is_ignored("clipboard/work/schema.json")


def test_reinit_updates_sidecars_without_duplicating_gitignore_section(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)

    assert cli.main(["init"]) == 0
    assert cli.main(["init"]) == 0
    capsys.readouterr()

    gitignore = (tmp_path / ".gitignore").read_text(encoding="utf-8")
    assert gitignore.count("# >>> micro-workflow-manager generated state >>>") == 1
    assert gitignore.count("# <<< micro-workflow-manager generated state <<<") == 1


def make_cleanup_component_project(tmp_path: Path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    src = tmp_path / "src"
    behavior = src / "node_behavior"
    behavior.mkdir(parents=True)
    (src / "graph.py").write_text(
        "EDGES = [('A', 'B'), ('B', 'C'), ('C', 'B'), ('C', 'D')]\n",
        encoding="utf-8",
    )
    for node in ["A", "B", "C", "D"]:
        (behavior / f"{node}.py").write_text(
            f'''from micro_workflow_manager import NodeRouter
router = NodeRouter("{node}")
@router.task
def run(ctx): return "{node}"
''',
            encoding="utf-8",
        )
    assert cli.main(["init"]) == 0
    assert cli.main(["graph", "src/graph.py", "--runner", "direct"]) == 0


@pytest.mark.parametrize("command", ["clean", "reset", "wipe"])
def test_cleanup_commands_expand_one_node_to_whole_hoeflein_component(
    tmp_path,
    monkeypatch,
    capsys,
    command,
):
    make_cleanup_component_project(tmp_path, monkeypatch)
    for node in ["A", "B", "C", "D"]:
        seed_dirty_node(tmp_path, node)
    capsys.readouterr()

    assert cli.main([command, "B"]) == 0
    out = capsys.readouterr().out
    assert "Hoeflein component(s) {B, C}" in out

    storage = FileStorage(tmp_path)
    for node in ["B", "C"]:
        node_dir = tmp_path / "node" / node
        assert not (node_dir / "output" / "remove.txt").exists()
        if command == "reset":
            assert storage.job_exists(node, 1)
            assert storage.get_job_status(node, 1) == "queued"
            assert (node_dir / "input" / "keep.txt").exists()
        else:
            assert not storage.job_exists(node, 1)
            if command == "wipe":
                assert not (node_dir / "input" / "keep.txt").exists()
            else:
                assert (node_dir / "input" / "keep.txt").exists()

    for node in ["A", "D"]:
        node_dir = tmp_path / "node" / node
        assert (node_dir / "output" / "remove.txt").exists()
        assert storage.job_exists(node, 1)
        assert storage.get_job_status(node, 1) == "done"


def test_cleanup_component_dry_run_lists_expanded_component(tmp_path, monkeypatch, capsys):
    make_cleanup_component_project(tmp_path, monkeypatch)
    capsys.readouterr()
    assert cli.main(["reset", "C", "--dry-run"]) == 0
    out = capsys.readouterr().out
    assert "selected Hoeflein components: {B, C}" in out
    assert "  B: would preserve jobs/input" in out
    assert "  C: would preserve jobs/input" in out
    assert "  A:" not in out
    assert "  D:" not in out
