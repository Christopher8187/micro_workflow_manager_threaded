from __future__ import annotations

from pathlib import Path

from micro_workflow_manager import cli
from micro_workflow_manager.models import Job
from micro_workflow_manager.storage import FileStorage


def _project(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    behavior = tmp_path / "src" / "node_behavior"
    behavior.mkdir(parents=True)
    (tmp_path / "src" / "graph.py").write_text(
        "EDGES = [('A', 'B'), ('B', 'C'), ('C', 'B'), ('C', 'D')]\n",
        encoding="utf-8",
    )
    sources = {
        "A": '''
from pathlib import Path
from micro_workflow_manager import NodeRouter
router = NodeRouter("A")
router.create_job(number=1)
@router.task
def run(ctx):
    root = Path(ctx.system.storage.project_dir)
    marker = root / "executed-A.txt"
    marker.write_text("yes", encoding="utf-8")
    ctx.node("B").add(value="from A")
''',
        "B": '''
from micro_workflow_manager import NodeRouter
router = NodeRouter("B")
@router.task
def run(ctx, value):
    ctx.node("C").add(value=value)
''',
        "C": '''
from micro_workflow_manager import NodeRouter
router = NodeRouter("C")
@router.task
def run(ctx, value):
    ctx.node("D").add(value=value)
''',
        "D": '''
from micro_workflow_manager import NodeRouter
router = NodeRouter("D")
@router.task
def run(ctx, value):
    return value
''',
    }
    for node, source in sources.items():
        (behavior / f"{node}.py").write_text(source.strip() + "\n", encoding="utf-8")
    assert cli.main(["init"]) == 0
    assert cli.main(["graph", "src/graph.py", "--runner", "direct"]) == 0


def test_reset_requires_typed_confirmation_and_does_not_run(tmp_path, monkeypatch, capsys):
    _project(tmp_path, monkeypatch)
    capsys.readouterr()

    monkeypatch.setattr("builtins.input", lambda prompt: "no")
    assert cli.main(["reset", "A"]) == 1
    aborted = capsys.readouterr().out
    assert "Aborted mwf reset; requested reset was not applied" in aborted
    assert "bootstrap and router mounting may already have updated framework state" in aborted

    assert cli.main(["reset", "A", "--dry-run"]) == 0
    preview = capsys.readouterr().out
    assert "requested reset was not applied" in preview
    assert "bootstrap and router mounting may already have updated framework state" in preview
    assert "no files, jobs, inputs, outputs, or statuses were changed" not in preview

    monkeypatch.setattr("builtins.input", lambda prompt: "reset")
    assert cli.main(["reset", "A"]) == 0
    assert not (tmp_path / "executed-A.txt").exists()
    storage = FileStorage(tmp_path)
    assert storage.job_exists("A", 1)
    assert storage.get_job_status("A", 1) == "queued"


def test_reset_selected_jobs_matches_run_job_preparation(tmp_path, monkeypatch, capsys):
    _project(tmp_path, monkeypatch)
    storage = FileStorage(tmp_path)
    storage.create_job(Job(job_id=2, node_name="A", params={"number": 2}))
    storage.set_job_status("A", 1, "done")
    storage.set_job_status("A", 2, "done")
    capsys.readouterr()

    assert cli.main(["reset", "A", "job", "2", "--yes"]) == 0
    assert storage.get_job_status("A", 1) == "done"
    assert storage.get_job_status("A", 2) == "queued"
    assert not (tmp_path / "executed-A.txt").exists()


def test_resetfrom_freshens_descendants_without_running(tmp_path, monkeypatch, capsys):
    _project(tmp_path, monkeypatch)
    assert cli.main(["runfrom", "A", "--runner", "direct"]) == 0
    storage = FileStorage(tmp_path)
    assert storage.list_job_ids("B")
    assert storage.list_job_ids("D")
    (tmp_path / "executed-A.txt").unlink()
    capsys.readouterr()

    assert cli.main(["resetfrom", "A", "--yes"]) == 0
    assert not (tmp_path / "executed-A.txt").exists()
    assert storage.get_job_status("A", 1) == "queued"
    assert storage.list_job_ids("B") == []
    assert storage.list_job_ids("C") == []
    assert storage.list_job_ids("D") == []


def test_cleanfrom_and_wipefrom_use_descendant_scope(tmp_path, monkeypatch, capsys):
    _project(tmp_path, monkeypatch)
    for node in ["A", "B", "C", "D"]:
        node_dir = tmp_path / "node" / node
        (node_dir / "input" / "keep.txt").write_text("input", encoding="utf-8")
        (node_dir / "output" / "result.txt").write_text("output", encoding="utf-8")
    capsys.readouterr()

    assert cli.main(["cleanfrom", "B", "--yes"]) == 0
    for node in ["B", "C", "D"]:
        assert (tmp_path / "node" / node / "input" / "keep.txt").exists()
        assert not (tmp_path / "node" / node / "output" / "result.txt").exists()
    assert (tmp_path / "node" / "A" / "output" / "result.txt").exists()

    assert cli.main(["wipefrom", "B", "--yes"]) == 0
    for node in ["B", "C", "D"]:
        assert not (tmp_path / "node" / node / "input" / "keep.txt").exists()
    assert (tmp_path / "node" / "A" / "input" / "keep.txt").exists()
