import json
import random
import time
from pathlib import Path

import pytest

from micro_workflow_manager import MicroWorkflow

pytestmark = pytest.mark.stress


def _tiny_random_wait(tag: int, job_id: int, origin: int = 0):
    # Deterministic pseudo-random waits scramble completion order without making
    # the test flaky.
    time.sleep(random.Random((tag * 1_000_003) + (job_id * 97) + origin).uniform(0.0, 0.0015))


def _status(tmp_path: Path, node_name: str) -> str:
    return json.loads((tmp_path / "node" / node_name / "node_state.json").read_text())["status"]


def _job_count(tmp_path: Path, node_name: str) -> int:
    return len([path for path in (tmp_path / "node" / node_name / "jobs").iterdir() if path.is_dir()])


def _assert_done_counts(workflow: MicroWorkflow, tmp_path: Path, expected: dict[str, int]):
    for node_name, count in expected.items():
        assert _status(tmp_path, node_name) == "done"
        assert _job_count(tmp_path, node_name) == count
        assert workflow.storage.read_job_index(node_name)["counts"]["done"] == count


def test_branch_cycle_a_to_b_c_to_a_1000_total_jobs_autostart_true_random_waits(tmp_path):
    """A -> B,C and B,C -> A with autostart=True under random completion order.

    334 seed A jobs produce 167 B jobs, 167 C jobs, and 334 return A jobs:
    668 + 167 + 167 = 1002 total jobs. This is intentionally just over 1000.
    """
    workflow = MicroWorkflow(project_dir=tmp_path, runner="threaded")
    workflow.graph([
        ("A", "B"),
        ("A", "C"),
        ("B", "A"),
        ("C", "A"),
    ])

    @workflow.task("A", max_threads=32)
    def run_a(ctx, phase="seed", origin=None):
        origin_value = ctx.job_id if origin is None else int(origin)
        _tiny_random_wait(1, ctx.job_id, origin_value)
        if phase == "seed":
            target = "B" if ctx.job_id % 2 else "C"
            ctx.node(target).add(autostart=True, origin=ctx.job_id)
        return None

    @workflow.task("B", max_threads=32)
    def run_b(ctx, origin):
        _tiny_random_wait(2, ctx.job_id, int(origin))
        ctx.node("A").add(autostart=True, phase="return", origin=origin)
        return None

    @workflow.task("C", max_threads=32)
    def run_c(ctx, origin):
        _tiny_random_wait(3, ctx.job_id, int(origin))
        ctx.node("A").add(autostart=True, phase="return", origin=origin)
        return None

    workflow.create_jobs("A", number=334, params={"phase": "seed"})
    workflow.run()
    _assert_done_counts(workflow, tmp_path, {"A": 668, "B": 167, "C": 167})


def test_chain_cycle_a_to_b_to_c_to_a_1000_total_jobs_autostart_true_random_waits(tmp_path):
    """A -> B -> C -> A with autostart=True under random completion order.

    250 seed A jobs produce 250 B jobs, 250 C jobs, and 250 return A jobs:
    500 + 250 + 250 = 1000 total jobs.
    """
    workflow = MicroWorkflow(project_dir=tmp_path, runner="threaded")
    workflow.graph([
        ("A", "B"),
        ("B", "C"),
        ("C", "A"),
    ])

    @workflow.task("A", max_threads=32)
    def run_a(ctx, phase="seed", origin=None):
        origin_value = ctx.job_id if origin is None else int(origin)
        _tiny_random_wait(11, ctx.job_id, origin_value)
        if phase == "seed":
            ctx.node("B").add(autostart=True, origin=ctx.job_id)
        return None

    @workflow.task("B", max_threads=32)
    def run_b(ctx, origin):
        _tiny_random_wait(12, ctx.job_id, int(origin))
        ctx.node("C").add(autostart=True, origin=origin)
        return None

    @workflow.task("C", max_threads=32)
    def run_c(ctx, origin):
        _tiny_random_wait(13, ctx.job_id, int(origin))
        ctx.node("A").add(autostart=True, phase="return", origin=origin)
        return None

    workflow.create_jobs("A", number=250, params={"phase": "seed"})
    workflow.run()
    _assert_done_counts(workflow, tmp_path, {"A": 500, "B": 250, "C": 250})
