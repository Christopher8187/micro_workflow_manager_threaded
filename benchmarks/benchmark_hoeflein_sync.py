from __future__ import annotations

"""Stress Hoeflein live-member synchronization under VPS-like backpressure.

Example (single CPU, deliberately slow explode payload reads):

    taskset -c 0 python benchmarks/benchmark_hoeflein_sync.py \
        --handlers 10 --seeds 400 --rounds 3 --handler-delay 0.04 \
        --payload-delay-per-job 0.006

The key regression metric is ``post_start_max_q_gt0_r0_seconds``: after explode
has started running at least once, how long can it have durable queued work but
zero running work. A resident Hoeflein pump keeps this interval bounded by the
actual next payload-load latency rather than scheduler teardown/restart delay.
"""

import argparse
import json
import tempfile
import threading
import time
from pathlib import Path

from micro_workflow_manager import MicroWorkflow, NodeRouter


def args():
    p = argparse.ArgumentParser()
    p.add_argument("--handlers", type=int, default=10)
    p.add_argument("--seeds", type=int, default=400)
    p.add_argument("--rounds", type=int, default=3)
    p.add_argument("--handler-delay", type=float, default=0.04)
    p.add_argument("--payload-delay-per-job", type=float, default=0.006)
    p.add_argument("--explode-threads", type=int, default=50)
    p.add_argument("--handler-threads", type=int, default=100)
    return p.parse_args()


def main() -> int:
    a = args()
    root = Path(tempfile.mkdtemp(prefix="mwf-hoeflein-sync-"))
    workflow = MicroWorkflow(root, runner="threaded")
    handlers = [f"handler{i}" for i in range(a.handlers)]
    edges = []
    for handler in handlers:
        edges.extend((("explode", handler), (handler, "explode")))
    workflow.graph(edges)

    explode = NodeRouter("explode", runner="threaded", max_threads=a.explode_threads)

    @explode.task
    def route(ctx, seed, depth):
        ctx.node(handlers[seed % len(handlers)]).add(
            autostart=True, seed=seed, depth=depth
        )
        return depth

    workflow.include_router(explode)
    for seed in range(a.seeds):
        workflow.add_job(None, "explode", seed=seed, depth=0)

    for name in handlers:
        router = NodeRouter(name, runner="api", max_threads=a.handler_threads)

        def handler(ctx, seed, depth, _name=name):
            ctx.sleep(a.handler_delay)
            if depth + 1 < a.rounds:
                ctx.node("explode").add(
                    autostart=True, seed=seed, depth=depth + 1
                )
            return (_name, depth)

        router.task(handler)
        workflow.include_router(router)

    original_load = workflow.storage.load_jobs_batch

    def delayed_load(node_name, job_ids):
        if node_name == "explode" and a.payload_delay_per_job:
            time.sleep(a.payload_delay_per_job * len(job_ids))
        return original_load(node_name, job_ids)

    workflow.storage.load_jobs_batch = delayed_load  # type: ignore[method-assign]

    state = {
        "stop": False,
        "seen_running": False,
        "q0_started": None,
        "max_q0": 0.0,
        "post_q0_started": None,
        "post_max_q0": 0.0,
        "peak_q": 0,
        "peak_r": 0,
    }

    def monitor():
        while not state["stop"]:
            counts = workflow.storage.job_status_counts("explode")
            queued = counts.get("queued", 0)
            running = counts.get("running", 0)
            state["peak_q"] = max(state["peak_q"], queued)
            state["peak_r"] = max(state["peak_r"], running)
            if running:
                state["seen_running"] = True

            now = time.perf_counter()
            if queued and not running:
                if state["q0_started"] is None:
                    state["q0_started"] = now
                state["max_q0"] = max(
                    state["max_q0"], now - state["q0_started"]
                )
            else:
                state["q0_started"] = None

            if state["seen_running"] and queued and not running:
                if state["post_q0_started"] is None:
                    state["post_q0_started"] = now
                state["post_max_q0"] = max(
                    state["post_max_q0"], now - state["post_q0_started"]
                )
            else:
                state["post_q0_started"] = None
            time.sleep(0.005)

    watcher = threading.Thread(target=monitor, daemon=True)
    watcher.start()
    started = time.perf_counter()
    error = None
    try:
        workflow.run_component(set(["explode", *handlers]), ignore_readiness=True)
    except BaseException as exc:  # benchmark reports rather than hides failures
        error = repr(exc)
    elapsed = time.perf_counter() - started
    state["stop"] = True
    watcher.join(timeout=2)

    total_done = sum(
        workflow.storage.job_status_counts(node).get("done", 0)
        for node in ["explode", *handlers]
    )
    print(json.dumps({
        "elapsed_seconds": elapsed,
        "done_jobs": total_done,
        "jobs_per_second": total_done / elapsed if elapsed else 0.0,
        "max_q_gt0_r0_seconds": state["max_q0"],
        "post_start_max_q_gt0_r0_seconds": state["post_max_q0"],
        "peak_explode_queued": state["peak_q"],
        "peak_explode_running": state["peak_r"],
        "error": error,
    }, sort_keys=True))
    return 1 if error is not None else 0


if __name__ == "__main__":
    raise SystemExit(main())
