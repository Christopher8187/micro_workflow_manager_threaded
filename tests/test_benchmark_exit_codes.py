import hashlib
import json
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace

import pytest

from benchmarks import benchmark_explode_pump_function, benchmark_hoeflein_sync, benchmark_hoeflein_wait
from micro_workflow_manager import MicroWorkflow


def test_repeated_api_growth_rejects_a_clear_progressive_slowdown():
    from benchmarks.benchmark_repeated_api_rounds import evaluate_growth

    result = evaluate_growth([[1, 2, 8, 10]] * 3)

    assert result["early_median"] == 1.5
    assert result["late_median"] == 9
    assert result["allowance"] == 5.5
    assert result["passed"] is False


@pytest.mark.parametrize("late, expected", [(1, True), (4, True), (4.01, False)])
def test_repeated_api_growth_preserves_the_fixed_allowance(late, expected):
    from benchmarks.benchmark_repeated_api_rounds import evaluate_growth

    result = evaluate_growth([[1, 1, late, late]] * 3)

    assert result["allowance"] == 4
    assert result["passed"] is expected


def _controlled_api_rounds(rounds):
    def run(source, directory, plan):
        (directory / "samples.json").write_text(
            json.dumps({"samples": [{"controlled_rounds": row} for row in rounds]}),
            encoding="utf-8",
        )
        return rounds
    return run


def test_repeated_api_benchmark_returns_nonzero_for_excessive_growth(tmp_path, monkeypatch):
    from benchmarks.benchmark_repeated_api_rounds import main
    from benchmarks import benchmark_repeated_api_rounds as benchmark

    monkeypatch.setattr(benchmark, "run_repetitions", _controlled_api_rounds([[1, 2, 8, 10]] * 3))
    output = tmp_path / "measurement"

    assert main(["--output", str(output), "--source-commit", "test-source", "--source-state", "controlled timing rows"]) == 1
    result = json.loads((output / "result.json").read_text(encoding="utf-8"))
    assert result["growth"]["passed"] is False
    assert result["exit_code"] == 1


def test_repeated_api_benchmark_accepts_the_exact_allowance(tmp_path, monkeypatch):
    from benchmarks import benchmark_repeated_api_rounds as benchmark

    monkeypatch.setattr(benchmark, "run_repetitions", _controlled_api_rounds([[1, 1, 4, 4]] * 3))
    output = tmp_path / "measurement"

    assert benchmark.main(["--output", str(output), "--source-commit", "test-source", "--source-state", "controlled timing rows"]) == 0
    result = json.loads((output / "result.json").read_text(encoding="utf-8"))
    assert result["growth"]["passed"] is True
    assert result["growth"]["late_median"] == result["growth"]["allowance"] == 4


def test_repeated_api_benchmark_preserves_child_failure(tmp_path, monkeypatch):
    from benchmarks import benchmark_repeated_api_rounds as benchmark

    def child_failure(*args):
        raise RuntimeError("controlled child failure")

    monkeypatch.setattr(benchmark, "run_repetitions", child_failure)
    output = tmp_path / "measurement"

    assert benchmark.main(["--output", str(output), "--source-commit", "test-source", "--source-state", "controlled failure"]) == 1
    result = json.loads((output / "result.json").read_text(encoding="utf-8"))
    assert result["growth"] is None
    assert "controlled child failure" in result["error"]


def test_repeated_api_worker_rejects_unfinished_jobs(tmp_path, monkeypatch):
    from pathlib import Path
    import micro_workflow_manager
    from benchmarks import benchmark_repeated_api_rounds as benchmark

    # Simulate early scheduler return, retaining real creation and SQLite state.
    monkeypatch.setattr(MicroWorkflow, "run_node", lambda *args, **kwargs: None)
    source = Path(micro_workflow_manager.__file__).resolve().parent.parent
    output = tmp_path / "unfinished"

    assert benchmark.measure_project(source, output) == 1
    result = json.loads((output / "result.json").read_text(encoding="utf-8"))
    assert result["correctness"] == "failed"
    assert result["cleanup"] == "passed"
    assert result["rounds"] == []
    storage = micro_workflow_manager.storage.FileStorage(output / "project")
    try:
        assert storage.job_status_counts("merge")["queued"] == 96
    finally:
        storage.close_thread_connection()


@pytest.mark.parametrize("damage", [None, "source_path", "source_hash", "executed_jobs", "historical_outputs"])
def test_repeated_api_parent_binds_results_to_the_measured_source(tmp_path, monkeypatch, damage):
    from benchmarks import benchmark_repeated_api_rounds as benchmark

    def completed_child(command, *, cwd, env, **kwargs):
        directory = Path(command[command.index("--child") + 1])
        directory.mkdir()
        plan = json.loads((directory.parent / "plan.json").read_text(encoding="utf-8"))
        rows = []
        for position in range(5):
            total = (position + 1) * 96
            cumulative = 96 * (position + 1) * (position + 2) // 2
            rows.append(dict(
                round=position, warmup=position == 0, jobs=total, new_jobs=96, run_seconds=1, drain_seconds=0,
                counts=dict(cancelled=0, done=total, failed=0, queued=0, running=0, skipped=0),
                writer=dict(pending_mutations=0, queued=0, durability_backlog=0), pruned=0,
                asynchronous_errors=[], runtime_future_observations=4 * total, outputs_checked=total,
                runtimes_checked=total, cumulative_outputs_checked=cumulative, integrity="ok",
                event_counts={name: total for name in ("queued", "started", "task_started", "output_written", "done")},
                cumulative_events={name: cumulative for name in ("queued", "started", "task_started", "output_written", "done")},
            ))
        data = dict(exit_code=0, correctness="passed", cleanup="passed", cleanup_seconds=0,
                    source=plan["source"], source_sha256=plan["source_sha256"],
                    worker_sha256=plan["worker_sha256"], command=command[1:], rounds=rows,
                    environment={name: env.get(name) for name in ("PYTHONPATH", "PYTHONDONTWRITEBYTECODE", "TEMP", "TMP")},
                    **plan["environment"])
        if damage == "source_path":
            data["source"] = "a different source copy"
        elif damage == "source_hash":
            data["source_sha256"] = {}
        elif damage == "executed_jobs":
            data["rounds"][-1]["jobs"] = 96
        elif damage == "historical_outputs":
            data["rounds"][-1]["outputs_checked"] = 96
        (directory / "result.json").write_text(json.dumps(data), encoding="utf-8")
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(benchmark, "subprocess", SimpleNamespace(run=completed_child, STDOUT=subprocess.STDOUT))
    output = tmp_path / "measurement"
    assert benchmark.main(["--output", str(output), "--source-commit", "test-source",
                           "--source-state", "controlled child observations"]) == (0 if damage is None else 1)
    result = json.loads((output / "result.json").read_text(encoding="utf-8"))
    assert result["samples_sha256"] == hashlib.sha256((output / "samples.json").read_bytes()).hexdigest()
    if damage is None:
        assert result["growth"]["passed"] is True
    else:
        assert result["growth"] is None


def test_repeated_api_benchmark_requires_source_state(tmp_path, monkeypatch):
    from benchmarks import benchmark_repeated_api_rounds as benchmark

    monkeypatch.setattr(benchmark, "run_repetitions", _controlled_api_rounds([[1, 1, 1, 1]] * 3))
    output = tmp_path / "measurement"
    with pytest.raises(SystemExit) as error:
        benchmark.main(["--output", str(output), "--source-commit", "test-source"])
    assert error.value.code == 2
    assert not output.exists()


def test_repeated_api_benchmark_refuses_optimized_python():
    from benchmarks import benchmark_repeated_api_rounds as benchmark

    result = subprocess.run([sys.executable, "-O", benchmark.__file__, "--help"],
                            capture_output=True, text=True, check=False)
    assert result.returncode != 0
    assert "requires unoptimized Python" in result.stderr


def test_repeated_api_benchmark_reads_source_version_without_tomllib():
    import micro_workflow_manager
    from benchmarks import benchmark_repeated_api_rounds as benchmark

    script = """
import builtins
import json
from pathlib import Path
import runpy
import sys
original_import = builtins.__import__
def without_tomllib(name, *args, **kwargs):
    if name == 'tomllib':
        raise ModuleNotFoundError('tomllib is unavailable on Python 3.10')
    return original_import(name, *args, **kwargs)
builtins.__import__ = without_tomllib
module = runpy.run_path(sys.argv[1], run_name='benchmark_compatibility_check')
print(json.dumps(module['environment_metadata'](Path(sys.argv[1]).resolve().parents[1])))
"""
    result = subprocess.run([sys.executable, "-c", script, benchmark.__file__],
                            capture_output=True, text=True, check=False)
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)["mwf_version"] == micro_workflow_manager.__version__


def test_hoeflein_wait_returns_nonzero_with_unfinished_jobs(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(benchmark_hoeflein_wait.tempfile, "mkdtemp", lambda **kwargs: str(tmp_path))
    monkeypatch.setattr("sys.argv", ["benchmark_hoeflein_wait.py", "--seeds", "1", "--rounds", "2", "--threads", "1"])
    # Reproduce the observed scheduler early return at the benchmark boundary.
    # The workflow, SQLite state, queued seed, and completion check remain real.
    monkeypatch.setattr(MicroWorkflow, "run_component", lambda *args, **kwargs: None)

    assert benchmark_hoeflein_wait.main() == 1
    result = json.loads(capsys.readouterr().out)
    assert "incomplete" in result["error"].lower()
    assert result["job_counts"]["A"]["queued"] == 1


def test_hoeflein_wait_returns_zero_after_complete_execution(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(benchmark_hoeflein_wait.tempfile, "mkdtemp", lambda **kwargs: str(tmp_path))
    monkeypatch.setattr("sys.argv", ["benchmark_hoeflein_wait.py", "--seeds", "1", "--rounds", "2", "--threads", "1", "--delay", "0"])

    assert benchmark_hoeflein_wait.main() == 0
    result = json.loads(capsys.readouterr().out)
    assert result["done"] == {"A": 2, "B": 1}
    assert result["error"] is None


def test_hoeflein_wait_returns_nonzero_when_terminal_jobs_miss_expected_rounds(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(benchmark_hoeflein_wait.tempfile, "mkdtemp", lambda **kwargs: str(tmp_path))
    monkeypatch.setattr("sys.argv", ["benchmark_hoeflein_wait.py", "--seeds", "1", "--rounds", "2", "--threads", "1"])

    def finish_only_seed(workflow, *args, **kwargs):
        workflow.storage.set_job_status("A", 1, "done")

    monkeypatch.setattr(MicroWorkflow, "run_component", finish_only_seed)
    assert benchmark_hoeflein_wait.main() == 1
    result = json.loads(capsys.readouterr().out)
    assert "incomplete" in result["error"].lower()
    assert result["done"] == {"A": 1, "B": 0}
    assert all(count == 0 for counts in result["job_counts"].values()
               for status, count in counts.items() if status != "done")


def test_hoeflein_sync_returns_nonzero_when_the_workflow_raises(monkeypatch):
    monkeypatch.setattr(
        benchmark_hoeflein_sync,
        "args",
        lambda: SimpleNamespace(
            handlers=1,
            seeds=0,
            rounds=1,
            handler_delay=0.0,
            payload_delay_per_job=0.0,
            explode_threads=1,
            handler_threads=1,
        ),
    )

    def fail_run_component(self, component, *, ignore_readiness):
        raise RuntimeError("intentional benchmark failure")

    monkeypatch.setattr(MicroWorkflow, "run_component", fail_run_component)

    assert benchmark_hoeflein_sync.main() == 1


def test_explode_pump_returns_nonzero_when_any_sample_has_failures(monkeypatch):
    monkeypatch.setattr(
        benchmark_explode_pump_function,
        "run_once",
        lambda args: {
            "function": "test",
            "pump_total": 1,
            "declared_limit_total": 1,
            "jobs_per_second": 1.0,
            "elapsed_seconds": 1.0,
            "failed": 2,
            "mutation_backlog_peak": 0,
            "normalized_node_rate_cv": 0.0,
        },
    )
    monkeypatch.setattr(
        "sys.argv",
        ["benchmark_explode_pump_function.py", "--repeats", "1"],
    )

    assert benchmark_explode_pump_function.main() == 1
