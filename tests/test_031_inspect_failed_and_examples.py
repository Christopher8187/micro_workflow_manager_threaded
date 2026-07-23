import json
from pathlib import Path

from micro_workflow_manager import cli
from micro_workflow_manager.cli.descriptions import COMMAND_DESCRIPTIONS
from tests.state_helpers import seed_job


def _write_json(path: Path, value: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding="utf-8")


def _make_project(tmp_path: Path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    behavior = tmp_path / "src" / "node_behavior"
    behavior.mkdir(parents=True)
    (tmp_path / "src" / "graph.py").write_text("EDGES=[('process_number','result')]\n", encoding="utf-8")
    for node in ("process_number", "result"):
        (behavior / f"{node}.py").write_text(
            "from micro_workflow_manager import NodeRouter\n"
            f"router=NodeRouter({node!r})\n"
            "@router.task\n"
            "def run(ctx): return None\n",
            encoding="utf-8",
        )
    assert cli.main(["init"]) == 0
    assert cli.main(["graph", "src/graph.py"]) == 0


def _seed_job(tmp_path: Path, job_id: int, status: str, *, error: str | None = None):
    state = seed_job(
        tmp_path,
        "process_number",
        job_id,
        status,
        params={"value": job_id},
        created_at="2026-07-14T12:00:00",
        status_extra={
            "finished_at": f"2026-07-14T12:00:{job_id:02d}",
            "duration_seconds": float(job_id),
        },
    )
    if error is not None:
        state.write_output("process_number", job_id, {"status": "failed", "error": error})


def test_inspect_failed_lists_ids_errors_and_restart_command(tmp_path, monkeypatch, capsys):
    _make_project(tmp_path, monkeypatch)
    _seed_job(tmp_path, 1, "done")
    _seed_job(tmp_path, 2, "failed", error="ValueError('bad value')")
    _seed_job(tmp_path, 4, "failed", error="TimeoutError('request exceeded deadline')")
    capsys.readouterr()

    assert cli.main(["inspect", "process_number", "failed"]) == 0
    out = capsys.readouterr().out

    assert "Failed jobs for node process_number" in out
    assert "count: 2" in out
    assert "IDs: 2 4" in out
    assert "ValueError('bad value')" in out
    assert "TimeoutError('request exceeded deadline')" in out
    assert "mwf inspect process_number job 2" in out
    assert "mwf resume process_number" in out
    assert "mwf resumefrom <start-node>" in out
    assert "  1:" not in out


def test_inspect_failed_handles_empty_node(tmp_path, monkeypatch, capsys):
    _make_project(tmp_path, monkeypatch)
    capsys.readouterr()

    assert cli.main(["inspect", "process_number", "failed"]) == 0
    out = capsys.readouterr().out
    assert "count: 0" in out
    assert "IDs: (none)" in out


def test_inspect_help_advertises_failed_mode(capsys):
    import pytest

    with pytest.raises(SystemExit) as exit_info:
        cli.main(["inspect", "--help"])
    assert exit_info.value.code == 0
    out = capsys.readouterr().out
    assert "job|failed|debug" in out
    assert "filter" not in out.split("positional arguments:", 1)[-1].split("options:", 1)[0]
    assert "list failed job IDs" in out


def test_descriptions_do_not_use_wait_as_a_node_name():
    combined = "\n".join(COMMAND_DESCRIPTIONS.values())
    forbidden = (
        "mwf inspect wait",
        "mwf run wait",
        "mwf wipe wait",
        'NodeRouter("wait")',
    )
    for text in forbidden:
        assert text not in combined
