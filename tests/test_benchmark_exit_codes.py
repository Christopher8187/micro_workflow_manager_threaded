import json
from types import SimpleNamespace

from benchmarks import benchmark_explode_pump_function, benchmark_hoeflein_sync, benchmark_hoeflein_wait
from micro_workflow_manager import MicroWorkflow


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
