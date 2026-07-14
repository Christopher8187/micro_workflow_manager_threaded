from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

from micro_workflow_manager import cli
from micro_workflow_manager.cli.deploy import deploy_setup
from micro_workflow_manager.storage import FileStorage


def _project(tmp_path: Path, monkeypatch, *, failing: bool = False):
    monkeypatch.chdir(tmp_path)
    behavior = tmp_path / "src" / "node_behavior"
    behavior.mkdir(parents=True)
    (tmp_path / "src" / "graph.py").write_text("EDGES = [('A', 'B')]\n", encoding="utf-8")
    body = "raise RuntimeError('boom')" if failing else "return ctx.job_id"
    (behavior / "A.py").write_text(
        f'''from micro_workflow_manager import NodeRouter
router = NodeRouter("A", max_threads=2)
router.create_job(number=1)
@router.task
def run(ctx):
    {body}
''',
        encoding="utf-8",
    )
    (behavior / "B.py").write_text(
        '''from micro_workflow_manager import NodeRouter
router = NodeRouter("B")
@router.task
def run(ctx):
    return None
''',
        encoding="utf-8",
    )
    assert cli.main(["init"]) == 0
    assert cli.main(["graph", "src/graph.py", "--runner", "threaded"]) == 0


def test_thread_override_is_consumed_by_one_run_and_removed(tmp_path, monkeypatch, capsys):
    _project(tmp_path, monkeypatch)
    capsys.readouterr()

    assert cli.main(["threads", "A", "5"]) == 0
    pending = json.loads((tmp_path / ".mwf" / "threads.json").read_text(encoding="utf-8"))
    assert pending["run_id"] is None
    assert pending["overrides"] == {"A": 5}

    assert cli.main(["run", "A", "--runner", "threaded"]) == 0
    capsys.readouterr()
    assert not (tmp_path / ".mwf" / "threads.json").exists()

    assert cli.main(["threads", "A"]) == 0
    output = capsys.readouterr().out
    assert "runtime override: (none)" in output
    assert "effective max_threads: 2" in output


def test_stale_run_bound_thread_override_is_ignored_and_cleared_by_next_run(tmp_path, monkeypatch, capsys):
    _project(tmp_path, monkeypatch)
    capsys.readouterr()
    storage = FileStorage(tmp_path)
    storage.write_thread_override_state({"A": 7}, run_id="old-run")
    assert storage.read_thread_overrides() == {}

    assert cli.main(["run", "A", "--runner", "threaded"]) == 0
    capsys.readouterr()
    assert not (tmp_path / ".mwf" / "threads.json").exists()


def test_restart_requeues_failed_job_without_active_run(tmp_path, monkeypatch, capsys):
    _project(tmp_path, monkeypatch, failing=True)
    capsys.readouterr()
    assert cli.main(["run", "A", "--runner", "direct"]) == 1
    capsys.readouterr()

    storage = FileStorage(tmp_path)
    assert storage.get_job_status("A", 1) == "failed"
    assert cli.main(["restart", "A", "job", "1"]) == 0
    output = capsys.readouterr().out
    assert "failed-job retry" in output
    assert "mwf resume A" in output
    assert storage.get_job_status("A", 1) == "queued"


def test_restart_refuses_done_job(tmp_path, monkeypatch, capsys):
    _project(tmp_path, monkeypatch)
    capsys.readouterr()
    assert cli.main(["run", "A", "--runner", "direct"]) == 0
    capsys.readouterr()
    assert cli.main(["restart", "A", "job", "1"]) == 1
    assert "never resets done work" in capsys.readouterr().err


def test_deploy_setup_prompts_for_nonstandard_port(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    assert cli.main(["init"]) == 0
    capsys.readouterr()
    replies = iter(["server.example", "chris", "22022", "key", ""])
    monkeypatch.setattr("builtins.input", lambda _prompt: next(replies))
    args = SimpleNamespace(
        host=None,
        user=None,
        port=None,
        auth=None,
        tool="openssh",
        key=None,
        pscp=None,
        plink=None,
        python_command=None,
    )
    assert deploy_setup(tmp_path, args) == 0
    config = json.loads((tmp_path / ".mwf" / "deploy" / "server.json").read_text(encoding="utf-8"))
    assert config["port"] == 22022
