from __future__ import annotations

"""Alternating two-node Hoeflein benchmark using explicit wait_for gates."""

import argparse
import json
import tempfile
import time
from pathlib import Path

from micro_workflow_manager import MicroWorkflow, NodeRouter


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--seeds", type=int, default=200)
    p.add_argument("--rounds", type=int, default=3)
    p.add_argument("--threads", type=int, default=100)
    p.add_argument("--delay", type=float, default=0.001)
    a = p.parse_args()

    w = MicroWorkflow(Path(tempfile.mkdtemp(prefix="mwf-wait-bench-")), runner="threaded")
    w.graph([("A", "B"), ("B", "A")])

    A = NodeRouter("A", runner="threaded", max_threads=a.threads, wait_for=["B"])
    B = NodeRouter("B", runner="threaded", max_threads=a.threads, wait_for=["A"])

    @A.task
    def run_a(ctx, seed, round):
        time.sleep(a.delay)
        if round < a.rounds:
            ctx.node("B").add(seed=seed, round=round)
        return round

    @B.task
    def run_b(ctx, seed, round):
        time.sleep(a.delay)
        if round < a.rounds:
            ctx.node("A").add(seed=seed, round=round + 1)
        return round

    w.include_routers(A, B)
    w.add_jobs(None, "A", [{"seed": i, "round": 1} for i in range(a.seeds)])
    started = time.perf_counter()
    error = None
    try:
        w.run_component({"A", "B"}, ignore_readiness=True)
    except BaseException as exc:
        error = repr(exc)
    elapsed = time.perf_counter() - started
    job_counts = {node: w.storage.job_status_counts(node) for node in ("A", "B")}
    done = {node: counts.get("done", 0) for node, counts in job_counts.items()}
    expected_done = {
        "A": max(a.seeds, 0) * max(a.rounds, 1),
        "B": max(a.seeds, 0) * max(a.rounds - 1, 0),
    }
    unfinished = any(
        count for counts in job_counts.values()
        for status, count in counts.items() if status != "done"
    )
    if error is None and (done != expected_done or unfinished):
        error = "Incomplete benchmark execution: expected completion counts or empty pending state were not reached"
    total = done["A"] + done["B"]
    print(json.dumps({
        "elapsed_seconds": elapsed,
        "jobs_per_second": total / elapsed if elapsed else 0.0,
        "done": done,
        "expected_done": expected_done,
        "job_counts": job_counts,
        "error": error,
    }, sort_keys=True))
    return 0 if error is None else 1


if __name__ == "__main__":
    raise SystemExit(main())
