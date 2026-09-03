from types import SimpleNamespace

from benchmarks import benchmark_explode_pump_function, benchmark_hoeflein_sync
from micro_workflow_manager import MicroWorkflow


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
