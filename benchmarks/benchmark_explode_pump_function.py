#!/usr/bin/env python3
"""Benchmark API pump sizing functions on the simultaneous explode shape.

The ten handler limits and initial job populations come from the fixed-declared
explode baseline. Every candidate preserves those per-node limits exactly; only
the number of cooperative controller pumps used to partition each limit changes.
"""
from __future__ import annotations

import argparse
import json
import math
import statistics
import tempfile
import threading
import time
from pathlib import Path
from urllib.parse import urlencode

from micro_workflow_manager import MicroWorkflow, NodeRouter
from micro_workflow_manager.networking import (
    close_shared_http_transport,
    configure_shared_http_transport,
    shared_http_transport,
)
from micro_workflow_manager.runners.api import ApiRunner
from micro_workflow_manager.workflow.component_scheduler import allocate_api_pumps


NODES = (
    ("explodeclaim", 200, 334),
    ("explodecontext", 400, 266),
    ("explodedefinition", 800, 589),
    ("explodeexample", 500, 269),
    ("explodeexercise", 1400, 1218),
    ("explodeexplanation", 400, 287),
    ("explodejas", 400, 134),
    ("explodenotation", 200, 72),
    ("exploderemark", 400, 351),
    ("explodetheorem", 600, 667),
)


def pump_count(limit: int, *, divisor: int, cap: int) -> int:
    return min(cap, max(1, math.ceil(limit / divisor)))


def pump_vector(args) -> dict[str, int]:
    limits = {name: limit for name, limit, _jobs in NODES}
    if args.global_budget:
        return allocate_api_pumps(limits, pump_budget=args.global_budget)
    return {
        name: pump_count(limit, divisor=args.divisor, cap=args.cap)
        for name, limit in limits.items()
    }


class Recorder:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.rows = {
            name: {"first": None, "last": None, "started": 0, "done": 0}
            for name, _limit, _jobs in NODES
        }

    def start(self, name: str) -> None:
        now = time.perf_counter()
        with self.lock:
            row = self.rows[name]
            if row["first"] is None:
                row["first"] = now
            row["started"] += 1

    def finish(self, name: str) -> None:
        with self.lock:
            row = self.rows[name]
            row["last"] = time.perf_counter()
            row["done"] += 1


def run_once(args) -> dict:
    original = ApiRunner._run_source
    vector = pump_vector(args)

    def sized_run_source(self, node_name, items, run_one):
        previous = self.startup_lanes_provider
        self.startup_lanes_provider = lambda _items: vector[node_name]
        try:
            return original(self, node_name, items, run_one)
        finally:
            self.startup_lanes_provider = previous

    ApiRunner._run_source = sized_run_source
    close_shared_http_transport()
    configure_shared_http_transport(
        http2=True,
        streams_per_connection=80,
        architecture="manager",
        verify=False,
    )
    query = urlencode({"bytes": args.response_bytes, "bps": 0, "delay_ms": args.delay_ms, "chunk": 4096})
    target = f"{args.endpoint.rstrip('/')}/transfer?{query}"
    recorder = Recorder()
    mutation_peak = [0, 0]
    stop = threading.Event()

    try:
        with tempfile.TemporaryDirectory(prefix="mwf-explode-pumps-", ignore_cleanup_errors=True) as directory:
            workflow = MicroWorkflow(Path(directory), runner="api")
            workflow.active_job_restart_enabled = True
            workflow.graph([("fanout", name) for name, _limit, _jobs in NODES])
            source = NodeRouter("fanout", runner="threaded", max_threads=1)
            source.create_job(params={"seed": True})

            @source.task
            def fanout(ctx, seed):
                for name, _limit, jobs in NODES:
                    ctx.node(name).add_many([{"request_index": index} for index in range(jobs)])
                return seed

            workflow.include_router(source)
            retained = [source]
            for name, limit, _jobs in NODES:
                router = NodeRouter(name, runner="api", max_threads=limit, timeout=180)

                def handler(ctx, request_index, *, _name=name):
                    recorder.start(_name)
                    response = shared_http_transport.request("GET", target, timeout=(10, 120))
                    response.raise_for_status()
                    if len(response.content) != args.response_bytes:
                        raise RuntimeError("response length mismatch")
                    recorder.finish(_name)
                    return request_index

                router.task(timeout=180)(handler)
                workflow.include_router(router)
                retained.append(router)

            def sample() -> None:
                while not stop.wait(0.01):
                    diag = workflow.storage.mutation_writer_diagnostics()
                    mutation_peak[0] = max(mutation_peak[0], int(diag.get("durability_backlog", 0)))
                    mutation_peak[1] = max(mutation_peak[1], int(diag.get("urgent", 0)))
                workflow.storage.close_thread_connection()

            sampler = threading.Thread(target=sample, daemon=True)
            sampler.start()
            started = time.perf_counter()
            try:
                workflow.run()
            finally:
                elapsed = time.perf_counter() - started
                stop.set()
                sampler.join(timeout=2)
            workflow.storage.flush_db_mutations()
            failed = sum(workflow.storage.job_status_counts(name).get("failed", 0) for name, _limit, _jobs in NODES)
            final_diag = workflow.storage.mutation_writer_diagnostics()
            time.sleep(0.6)
            workflow.storage.close_database_connections()
    finally:
        ApiRunner._run_source = original
        snapshot = shared_http_transport.snapshot()
        close_shared_http_transport()

    per_node = {}
    for name, limit, jobs in NODES:
        row = recorder.rows[name]
        duration = max(1e-9, row["last"] - row["first"])
        per_node[name] = {
            "limit": limit,
            "jobs": jobs,
            "pumps": vector[name],
            "done": row["done"],
            "jobs_per_second": row["done"] / duration,
            "first_request_offset_seconds": row["first"] - started,
        }
    total_jobs = sum(jobs for _name, _limit, jobs in NODES)
    normalized = [row["jobs_per_second"] / row["limit"] for row in per_node.values()]
    return {
        "divisor": args.divisor,
        "cap": args.cap,
        "function": (
            f"global marginal allocation, budget={args.global_budget}"
            if args.global_budget
            else f"min({args.cap}, ceil(n/{args.divisor}))"
        ),
        "declared_limit_total": sum(limit for _name, limit, _jobs in NODES),
        "pump_total": sum(row["pumps"] for row in per_node.values()),
        "jobs": total_jobs,
        "elapsed_seconds": elapsed,
        "jobs_per_second": total_jobs / elapsed,
        "failed": failed,
        "mutation_backlog_peak": mutation_peak[0],
        "mutation_urgent_peak": mutation_peak[1],
        "mutation_backlog_final": int(final_diag.get("durability_backlog", 0)),
        "normalized_node_rate_cv": statistics.pstdev(normalized) / statistics.mean(normalized),
        "http_clients": snapshot.get("client_count", 0),
        "per_node": per_node,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--endpoint", default="https://127.0.0.1:8766")
    parser.add_argument("--divisor", type=int, default=384)
    parser.add_argument("--cap", type=int, default=12)
    parser.add_argument("--global-budget", type=int, default=0)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--response-bytes", type=int, default=1024)
    parser.add_argument("--delay-ms", type=float, default=5.0)
    parser.add_argument("--jsonl", type=Path)
    args = parser.parse_args()
    if args.divisor < 1 or args.cap < 1 or args.repeats < 1 or args.global_budget < 0:
        parser.error("divisor, cap, and repeats must be positive")
    rows = []
    for sample_index in range(1, args.repeats + 1):
        row = run_once(args)
        row["sample_index"] = sample_index
        rows.append(row)
        print(json.dumps(row, sort_keys=True), flush=True)
        if args.jsonl:
            with args.jsonl.open("a", encoding="utf-8") as stream:
                stream.write(json.dumps(row, sort_keys=True) + "\n")
    summary = {
        "kind": "summary",
        "function": rows[0]["function"],
        "pump_total": rows[0]["pump_total"],
        "declared_limit_total": rows[0]["declared_limit_total"],
        "median_jobs_per_second": statistics.median(row["jobs_per_second"] for row in rows),
        "median_elapsed_seconds": statistics.median(row["elapsed_seconds"] for row in rows),
        "max_failed": max(row["failed"] for row in rows),
        "median_mutation_backlog_peak": statistics.median(row["mutation_backlog_peak"] for row in rows),
        "median_normalized_node_rate_cv": statistics.median(row["normalized_node_rate_cv"] for row in rows),
    }
    print(json.dumps(summary, sort_keys=True), flush=True)
    if args.jsonl:
        with args.jsonl.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(summary, sort_keys=True) + "\n")
    return 1 if summary["max_failed"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
