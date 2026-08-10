#!/usr/bin/env python3
"""3D MWF fan-out matrix using the real localhost HTTP transport.

Axes:
  concurrency         aggregate API max_threads across all fan-out nodes
  transfer throughput per HTTP response (bytes/s; 0 = unlimited)
  fan-out node count  number of independent downstream API DAG nodes

The default cell uses jobs=2*concurrency. For high-concurrency/tens-of-nodes
cells pass --jobs explicitly when you want exactly tens/hundreds of jobs/node.
Modes:
  workflow  full durable MWF fan-out + API lifecycle (production-shape)
  runner    ApiRunner + MWF shared HTTP transport, no SQLite/filesystem
  transport direct httpx AsyncClient control, no MWF runner/storage
"""
from __future__ import annotations

import argparse
import asyncio
import json
import inspect
import math
import os
import statistics
import tempfile
import threading
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from urllib.parse import urlencode

import httpx

from micro_workflow_manager import MicroWorkflow, NodeRouter
from micro_workflow_manager.networking import (
    close_shared_http_transport,
    configure_shared_http_transport,
    shared_http_transport,
)
from micro_workflow_manager.runners.api import ApiRunner


@dataclass(slots=True)
class Result:
    mode: str
    protocol: str
    concurrency: int
    fanout_nodes: int
    jobs: int
    jobs_per_node: float
    response_bytes: int
    bytes_per_second: int
    delay_ms: float
    streams_per_connection: int
    http1_connections_per_shard: int
    elapsed_seconds: float
    jobs_per_second: float
    mib_per_second: float
    transfer_floor_seconds: float
    ideal_jobs_per_second: float | None
    efficiency_vs_transfer_floor: float | None
    fd_peak: int
    http_clients: int
    failed: int
    mutation_backlog_peak: int = 0
    mutation_urgent_peak: int = 0
    mutation_submitted: int = 0
    mutation_counts: dict | None = None
    node_status_writes: dict | None = None


def split(total: int, buckets: int):
    q, r = divmod(total, buckets)
    return [q + (i < r) for i in range(buckets)]


def fd_count():
    try:
        return len(os.listdir("/proc/self/fd"))
    except OSError:
        return -1


def transfer_url(args):
    q = urlencode({
        "bytes": args.response_bytes,
        "bps": args.bytes_per_second,
        "delay_ms": args.delay_ms,
        "chunk": args.chunk_size,
    })
    return f"{args.endpoint.rstrip('/')}/transfer?{q}"


def ideal_metrics(args, jobs):
    floor = args.delay_ms / 1000.0
    if args.bytes_per_second:
        floor += args.response_bytes / args.bytes_per_second
    if floor <= 0:
        return 0.0, None
    waves = math.ceil(jobs / args.concurrency)
    ideal = jobs / (waves * floor)
    return floor, ideal


def fd_sampler(stop, box):
    while not stop.wait(0.01):
        box[0] = max(box[0], fd_count())


def configure_transport(args):
    close_shared_http_transport()
    kwargs = {
        "http2": args.http2,
        "streams_per_connection": args.streams_per_connection,
        "verify": False if args.http2 else True,
    }
    # Keep this benchmark runnable against older MWF source trees for
    # before/after comparisons. 0.5.4 adds this tuning knob.
    if "http1_connections_per_shard" in inspect.signature(configure_shared_http_transport).parameters:
        kwargs["http1_connections_per_shard"] = args.http1_connections_per_shard
    configure_shared_http_transport(**kwargs)


def run_runner(args, jobs):
    configure_transport(args)
    url = transfer_url(args)
    peak = [fd_count()]
    stop = threading.Event()
    sampler = threading.Thread(target=fd_sampler, args=(stop, peak), daemon=True)
    sampler.start()
    start = time.perf_counter()
    try:
        values = ApiRunner(max_threads=args.concurrency, poll_interval=0.005).run_jobs(
            "bench",
            list(range(jobs)),
            lambda i: len(shared_http_transport.request("GET", url, timeout=(10, 120)).content),
        )
    finally:
        elapsed = time.perf_counter() - start
        stop.set(); sampler.join(timeout=1)
    snap = shared_http_transport.snapshot()
    close_shared_http_transport()
    failed = sum(1 for value in values if value != args.response_bytes)
    return elapsed, peak[0], int(snap.get("client_count", 0)), failed


async def _transport_control(args, jobs):
    url = transfer_url(args)
    limits = httpx.Limits(
        max_connections=args.concurrency,
        max_keepalive_connections=args.concurrency,
        keepalive_expiry=60,
    )
    async with httpx.AsyncClient(http2=args.http2, verify=False if args.http2 else True, limits=limits) as client:
        semaphore = asyncio.Semaphore(args.concurrency)
        async def one(_):
            async with semaphore:
                response = await client.get(url, timeout=httpx.Timeout(120, connect=10))
                response.raise_for_status()
                return len(response.content)
        started = time.perf_counter()
        values = await asyncio.gather(*(one(i) for i in range(jobs)))
        return time.perf_counter() - started, values


def run_transport(args, jobs):
    peak = [fd_count()]
    stop = threading.Event()
    sampler = threading.Thread(target=fd_sampler, args=(stop, peak), daemon=True)
    sampler.start()
    try:
        elapsed, values = asyncio.run(_transport_control(args, jobs))
    finally:
        stop.set(); sampler.join(timeout=1)
    return elapsed, peak[0], 1, sum(1 for value in values if value != args.response_bytes)


def run_workflow(args, jobs):
    if args.concurrency < args.fanout_nodes:
        raise ValueError("concurrency must be >= fanout_nodes")
    per_jobs = split(jobs, args.fanout_nodes)
    per_limits = split(args.concurrency, args.fanout_nodes)
    configure_transport(args)
    url = transfer_url(args)
    peak = [fd_count()]
    stop = threading.Event()
    with tempfile.TemporaryDirectory(prefix="mwf-http-fanout-") as directory:
        workflow = MicroWorkflow(Path(directory), runner="api")
        # CLI run/runfrom enables this. The benchmark deliberately uses the
        # production lease/fence/lifecycle path rather than the cheaper
        # programmatic-only unfenced path.
        workflow.active_job_restart_enabled = not args.diagnostic_no_restart
        if args.diagnostic_no_output:
            workflow.storage.write_output = lambda *_a, **_k: None
        if args.diagnostic_no_task_events:
            _append = workflow.storage.append_job_event
            def _filtered_event(node_name, job_id, event, **data):
                if event == "task_started":
                    return None
                return _append(node_name, job_id, event, **data)
            workflow.storage.append_job_event = _filtered_event
        if args.diagnostic_no_fence:
            from contextlib import contextmanager
            @contextmanager
            def _no_fence(*_a, **_k):
                yield
            workflow.storage.guard_job_execution = _no_fence
        if args.diagnostic_no_runtime_start:
            _persist = workflow.scheduler_supervisor._persist_runtime
            def _filtered_runtime(watch, *, state, error=None, wait=True, priority=10):
                if state == "running":
                    return None
                return _persist(watch, state=state, error=error, wait=wait, priority=priority)
            workflow.scheduler_supervisor._persist_runtime = _filtered_runtime
        mutation_counts = {}
        node_status_writes = {}
        _set_node_status = workflow.storage.set_node_status
        _set_node_statuses = workflow.storage.set_node_statuses
        def _count_node_status(node_name, status):
            key = f"{node_name}:{status}"
            node_status_writes[key] = node_status_writes.get(key, 0) + 1
            return _set_node_status(node_name, status)
        def _count_node_statuses(statuses):
            for node_name, status in statuses.items():
                key = f"{node_name}:{status}"
                node_status_writes[key] = node_status_writes.get(key, 0) + 1
            return _set_node_statuses(statuses)
        workflow.storage.set_node_status = _count_node_status
        workflow.storage.set_node_statuses = _count_node_statuses
        _submit = workflow.storage.submit_db_mutation
        _grouped = workflow.storage.submit_grouped_db_mutation
        def _count_submit(operation, *, wait=True, priority=10, **kwargs):
            key = f"plain:{getattr(operation, '__name__', 'operation')}"
            mutation_counts[key] = mutation_counts.get(key, 0) + 1
            return _submit(operation, wait=wait, priority=priority, **kwargs)
        def _count_group(group_key, item, operation, *, wait=True, priority=10, collect_seconds=0.001, **kwargs):
            head = group_key[0] if isinstance(group_key, tuple) and group_key else str(group_key)
            key = f"group:{head}"
            mutation_counts[key] = mutation_counts.get(key, 0) + 1
            return _grouped(group_key, item, operation, wait=wait, priority=priority, collect_seconds=collect_seconds, **kwargs)
        workflow.storage.submit_db_mutation = _count_submit
        workflow.storage.submit_grouped_db_mutation = _count_group
        workflow.graph([("fanout", f"H{i:03d}") for i in range(args.fanout_nodes)])
        source = NodeRouter("fanout", runner="threaded", max_threads=1)
        source.create_job(params={"seed": True})

        @source.task
        def fanout(ctx, seed):
            for i, count in enumerate(per_jobs):
                if count:
                    ctx.node(f"H{i:03d}").add_many([{"request_index": j} for j in range(count)])
            return jobs
        workflow.include_router(source)
        # The benchmark itself retains generated router objects so old releases
        # can be measured fairly even though 0.5.3 had an id-reuse bug for
        # short-lived programmatic routers. 0.5.4 fixes that framework bug and
        # has a separate regression test for it.
        retained_routers = [source]

        for i, limit in enumerate(per_limits):
            router = NodeRouter(f"H{i:03d}", runner="api", max_threads=max(1, limit), timeout=180)
            def make_handler():
                def handler(ctx, request_index):
                    response = shared_http_transport.request("GET", url, timeout=(10, 120))
                    response.raise_for_status()
                    if len(response.content) != args.response_bytes:
                        raise RuntimeError("response length mismatch")
                    return request_index
                return handler
            router.task(timeout=180)(make_handler())
            workflow.include_router(router)
            retained_routers.append(router)

        mutation_peak = [0, 0]
        def sample_workflow():
            while not stop.wait(0.01):
                peak[0] = max(peak[0], fd_count())
                diag = workflow.storage.mutation_writer_diagnostics()
                mutation_peak[0] = max(mutation_peak[0], int(diag.get("durability_backlog", 0)))
                mutation_peak[1] = max(mutation_peak[1], int(diag.get("urgent", 0)))
            workflow.storage.close_thread_connection()
        sampler = threading.Thread(target=sample_workflow, daemon=True)
        sampler.start()
        started = time.perf_counter()
        try:
            workflow.run()
        finally:
            elapsed = time.perf_counter() - started
            stop.set(); sampler.join(timeout=1)
        failed = sum(workflow.storage.job_status_counts(f"H{i:03d}").get("failed", 0) for i in range(args.fanout_nodes))
        snap = shared_http_transport.snapshot()
        final_diag = workflow.storage.mutation_writer_diagnostics()
        args._mutation_stats = (
            mutation_peak[0], mutation_peak[1],
            int(final_diag.get("submitted_serial", 0)),
            mutation_counts, node_status_writes,
        )
    close_shared_http_transport()
    return elapsed, peak[0], int(snap.get("client_count", 0)), failed


def run_cell(args):
    jobs = args.jobs or max(args.concurrency * 2, args.fanout_nodes * 20)
    if args.mode == "workflow":
        elapsed, peak, clients, failed = run_workflow(args, jobs)
    elif args.mode == "runner":
        elapsed, peak, clients, failed = run_runner(args, jobs)
    else:
        elapsed, peak, clients, failed = run_transport(args, jobs)
    floor, ideal = ideal_metrics(args, jobs)
    jps = jobs / elapsed
    mutation = getattr(args, "_mutation_stats", (0, 0, 0, {}, {}))
    if hasattr(args, "_mutation_stats"):
        delattr(args, "_mutation_stats")
    return Result(
        mode=args.mode,
        protocol="h2" if args.http2 else "h1",
        concurrency=args.concurrency,
        fanout_nodes=args.fanout_nodes,
        jobs=jobs,
        jobs_per_node=jobs / args.fanout_nodes,
        response_bytes=args.response_bytes,
        bytes_per_second=args.bytes_per_second,
        delay_ms=args.delay_ms,
        streams_per_connection=args.streams_per_connection,
        http1_connections_per_shard=args.http1_connections_per_shard,
        elapsed_seconds=elapsed,
        jobs_per_second=jps,
        mib_per_second=(jobs * args.response_bytes / (1024 * 1024)) / elapsed,
        transfer_floor_seconds=floor,
        ideal_jobs_per_second=ideal,
        efficiency_vs_transfer_floor=(jps / ideal if ideal else None),
        fd_peak=peak,
        http_clients=clients,
        failed=failed,
        mutation_backlog_peak=mutation[0],
        mutation_urgent_peak=mutation[1],
        mutation_submitted=mutation[2],
        mutation_counts=mutation[3],
        node_status_writes=mutation[4],
    )


def csv_ints(text):
    return [int(value) for value in text.split(",") if value.strip()]


def _write_jsonl(path: str, row: dict) -> None:
    text = json.dumps(row, sort_keys=True)
    print(text, flush=True)
    if path:
        with open(path, "a", encoding="utf-8") as handle:
            handle.write(text + "\n")


def run_repeated_cell(args):
    samples = []
    for sample_index in range(1, args.repeats + 1):
        row = asdict(run_cell(args))
        row["kind"] = "sample"
        row["sample_index"] = sample_index
        samples.append(row)
        _write_jsonl(args.jsonl, row)
    if args.repeats == 1:
        return samples[0]

    summary = {
        "kind": "summary",
        "mode": args.mode,
        "protocol": "h2" if args.http2 else "h1",
        "concurrency": args.concurrency,
        "fanout_nodes": args.fanout_nodes,
        "jobs": samples[0]["jobs"],
        "jobs_per_node": samples[0]["jobs_per_node"],
        "response_bytes": args.response_bytes,
        "bytes_per_second": args.bytes_per_second,
        "delay_ms": args.delay_ms,
        "streams_per_connection": args.streams_per_connection,
        "http1_connections_per_shard": args.http1_connections_per_shard,
        "repeats": args.repeats,
        "jobs_per_second_samples": [row["jobs_per_second"] for row in samples],
        "median_jobs_per_second": statistics.median(row["jobs_per_second"] for row in samples),
        "min_jobs_per_second": min(row["jobs_per_second"] for row in samples),
        "max_jobs_per_second": max(row["jobs_per_second"] for row in samples),
        "median_fd_peak": statistics.median(row["fd_peak"] for row in samples),
        "median_mutation_backlog_peak": statistics.median(row["mutation_backlog_peak"] for row in samples),
        "median_mutation_submitted": statistics.median(row["mutation_submitted"] for row in samples),
    }
    _write_jsonl(args.jsonl, summary)
    return summary


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--endpoint", default="http://127.0.0.1:8765")
    p.add_argument("--http2", action="store_true")
    p.add_argument("--mode", choices=["workflow", "runner", "transport"], default="workflow")
    p.add_argument("--concurrency", type=int, default=32)
    p.add_argument("--fanout-nodes", type=int, default=4)
    p.add_argument("--jobs", type=int, default=0)
    p.add_argument("--response-bytes", type=int, default=65536)
    p.add_argument("--bytes-per-second", type=int, default=0)
    p.add_argument("--delay-ms", type=float, default=5.0)
    p.add_argument("--chunk-size", type=int, default=4096)
    p.add_argument("--streams-per-connection", type=int, default=80)
    p.add_argument(
        "--http1-connections-per-shard", type=int, default=16,
        help="MWF HTTP/1.1 client-pool shard size; ignored by direct transport control",
    )
    p.add_argument("--repeats", type=int, default=1)
    p.add_argument("--matrix", action="store_true")
    p.add_argument("--concurrencies", default="32,128,512,1024")
    p.add_argument("--fanout-node-counts", default="1,4,10,20")
    p.add_argument("--transfer-rates", default="0,10485760,1048576,262144")
    p.add_argument("--jsonl", default="")
    p.add_argument("--diagnostic-no-restart", action="store_true")
    p.add_argument("--diagnostic-no-output", action="store_true")
    p.add_argument("--diagnostic-no-task-events", action="store_true")
    p.add_argument("--diagnostic-no-fence", action="store_true")
    p.add_argument("--diagnostic-no-runtime-start", action="store_true")
    args = p.parse_args()
    if args.repeats < 1:
        p.error("--repeats must be >= 1")
    if args.http1_connections_per_shard < 1:
        p.error("--http1-connections-per-shard must be >= 1")

    if not args.matrix:
        run_repeated_cell(args)
        return
    requested_matrix_jobs = args.jobs
    for concurrency in csv_ints(args.concurrencies):
        for nodes in csv_ints(args.fanout_node_counts):
            if nodes > concurrency:
                continue
            for rate in csv_ints(args.transfer_rates):
                args.concurrency = concurrency
                args.fanout_nodes = nodes
                args.bytes_per_second = rate
                args.jobs = requested_matrix_jobs or max(concurrency * 2, nodes * 20)
                run_repeated_cell(args)


if __name__ == "__main__":
    main()
