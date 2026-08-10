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
    done = {"A": 0, "B": 0}

    A = NodeRouter("A", runner="threaded", max_threads=a.threads, wait_for=["B"])
    B = NodeRouter("B", runner="threaded", max_threads=a.threads, wait_for=["A"])

    @A.task
    def run_a(ctx, seed, round):
        time.sleep(a.delay)
        done["A"] += 1
        if round < a.rounds:
            ctx.node("B").add(seed=seed, round=round)
        return round

    @B.task
    def run_b(ctx, seed, round):
        time.sleep(a.delay)
        done["B"] += 1
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
    total = done["A"] + done["B"]
    print(json.dumps({
        "elapsed_seconds": elapsed,
        "jobs_per_second": total / elapsed if elapsed else 0.0,
        "done": done,
        "error": error,
    }, sort_keys=True))
    return 0 if error is None else 1


if __name__ == "__main__":
    raise SystemExit(main())
