"""Reproduce high-concurrency Hoeflein monitor lag with a mock HTTP server.

The graph shape is copied from pdftostructureddata's ``explode`` component:
``explode`` is bidirectionally connected to ten API handler nodes. The benchmark
uses deterministic pseudo-random response delays, records when the mock provider
has returned, when MWF has written output.json, and when SQLite/``mwf monitor``
can see terminal state.

Examples:
    python benchmarks/reproduce_explode_ghost.py --jobs-per-handler 256
    python benchmarks/reproduce_explode_ghost.py --jobs-per-handler 512 1024
    python benchmarks/reproduce_explode_ghost.py --handlers 10 --queued-handlers 1
"""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import tempfile
import threading
import time
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Callable

import httpx

from micro_workflow_manager import MicroWorkflow, NodeRouter
from micro_workflow_manager.networking import (
    close_shared_http_transport,
    configure_shared_http_transport,
    shared_http_transport,
)


HANDLERS = (
    "explodeclaim",
    "explodecontext",
    "explodedefinition",
    "explodeexample",
    "explodeexercise",
    "explodeexplanation",
    "explodejas",
    "explodenotation",
    "exploderemark",
    "explodetheorem",
)


@dataclass(slots=True)
class Result:
    handlers: int
    queued_handlers: int
    jobs_per_handler: int
    max_threads_per_handler: int
    jobs: int
    elapsed_seconds: float
    first_provider_response_seconds: float | None
    first_output_seconds: float | None
    first_monitor_terminal_seconds: float | None
    all_jobs_admitted_seconds: float | None
    max_provider_not_terminal: int
    max_output_not_terminal: int
    max_provider_without_monitor_row: int
    terminal_publish_lag_p50_seconds: float | None
    terminal_publish_lag_p95_seconds: float | None
    terminal_publish_lag_max_seconds: float | None
    final_queued: int
    final_running: int
    final_done: int
    final_failed: int


def _rounded(value: float | None) -> float | None:
    return None if value is None else round(value, 6)


def run_case(
    jobs_per_handler: int,
    *,
    max_threads: int | None = None,
    handlers: int = 10,
    queued_handlers: int | None = None,
    seed: int = 7,
    sample_seconds: float = 0.005,
    observer: Callable[[object, list[str]], None] | None = None,
) -> Result:
    if not 1 <= handlers <= len(HANDLERS):
        raise ValueError(f"handlers must be between 1 and {len(HANDLERS)}")
    if jobs_per_handler < 1:
        raise ValueError("jobs_per_handler must be positive")
    if max_threads is None:
        max_threads = jobs_per_handler
    if max_threads < 1:
        raise ValueError("max_threads must be positive")
    if queued_handlers is None:
        queued_handlers = handlers
    if not 1 <= queued_handlers <= handlers:
        raise ValueError("queued_handlers must be between 1 and handlers")

    selected = HANDLERS[:handlers]
    queued = set(selected[:queued_handlers])
    total = queued_handlers * jobs_per_handler
    close_shared_http_transport()

    counter_lock = threading.Lock()
    provider_completed = 0
    outputs_written = 0
    first_provider_at: float | None = None
    first_output_at: float | None = None
    output_wall_times: dict[tuple[str, int], float] = {}

    async def mock_provider(request: httpx.Request) -> httpx.Response:
        nonlocal provider_completed, first_provider_at
        payload = json.loads(request.content)
        # Stable jitter from 1.0ms through 10.5ms. Different nodes and job IDs
        # finish out of order without making benchmark runs non-reproducible.
        jitter_slot = (
            int(payload["value"]) * 1103515245
            + int(payload["node_index"]) * 12345
            + seed
        ) % 20
        await asyncio.sleep(0.001 + jitter_slot * 0.0005)
        completed_at = time.perf_counter()
        with counter_lock:
            provider_completed += 1
            if first_provider_at is None:
                first_provider_at = completed_at
        return httpx.Response(200, json={"ok": True}, request=request)

    configure_shared_http_transport(
        http2=True,
        streams_per_connection=max(1, total),
        transport=httpx.MockTransport(mock_provider),
    )

    try:
        with tempfile.TemporaryDirectory(
            prefix="mwf-explode-ghost-",
            ignore_cleanup_errors=True,
        ) as directory:
            workflow = MicroWorkflow(Path(directory), runner="api")
            workflow.active_job_restart_enabled = True
            edges = []
            for handler_name in selected:
                edges.extend((("explode", handler_name), (handler_name, "explode")))
            workflow.graph(edges)

            explode = NodeRouter(
                "explode",
                runner="threaded",
                max_threads=1,
                wait_for=list(selected),
            )

            @explode.task
            def run_explode(ctx):
                return None

            workflow.include_router(explode)
            # Retain routers for the entire run. ``include_router`` currently
            # de-duplicates by object id, so immediate garbage collection could
            # let CPython reuse an id during repeated benchmark cases.
            mounted_routers = [explode]

            for node_index, handler_name in enumerate(selected):
                router = NodeRouter(
                    handler_name,
                    runner="api",
                    max_threads=max_threads,
                    wait_for=["explode"],
                )

                def make_handler(index: int):
                    def run_handler(ctx, value):
                        return shared_http_transport.post_json(
                            "https://mock.local/explode",
                            timeout=30,
                            json={"value": value, "node_index": index},
                        )

                    return run_handler

                router.task(timeout=60)(make_handler(node_index))
                workflow.include_router(router)
                mounted_routers.append(router)
                if handler_name in queued:
                    workflow.add_jobs(
                        None,
                        handler_name,
                        [{"value": value} for value in range(jobs_per_handler)],
                    )

            original_write_output = workflow.storage.write_output

            def counted_write_output(node_name, job_id, data):
                nonlocal outputs_written, first_output_at
                result = original_write_output(node_name, job_id, data)
                written_at = time.perf_counter()
                with counter_lock:
                    outputs_written += 1
                    output_wall_times[(node_name, int(job_id))] = time.time()
                    if first_output_at is None:
                        first_output_at = written_at
                return result

            workflow.storage.write_output = counted_write_output

            samples: list[tuple[float, int, int, int, int, int, int]] = []
            stop = threading.Event()
            started_at = time.perf_counter()

            def sample_monitor() -> None:
                while not stop.is_set():
                    with counter_lock:
                        provider = provider_completed
                        outputs = outputs_written
                    rows = workflow.storage.db_connection().execute(
                        "SELECT status, COUNT(*) AS count FROM jobs GROUP BY status"
                    ).fetchall()
                    counts = {str(row["status"]): int(row["count"]) for row in rows}
                    samples.append(
                        (
                            time.perf_counter() - started_at,
                            provider,
                            outputs,
                            counts.get("queued", 0),
                            counts.get("running", 0),
                            counts.get("done", 0),
                            counts.get("failed", 0),
                        )
                    )
                    if observer is not None:
                        observer(workflow, list(selected))
                    stop.wait(sample_seconds)
                workflow.storage.close_thread_connection()

            monitor = threading.Thread(target=sample_monitor, name="benchmark-monitor")
            monitor.start()
            errors: list[BaseException] = []

            def run_workflow() -> None:
                try:
                    workflow.run_node("explode", ignore_readiness=True)
                except BaseException as error:
                    errors.append(error)

            worker = threading.Thread(target=run_workflow, name="benchmark-workflow")
            worker.start()
            worker.join(timeout=300)
            stop.set()
            monitor.join(timeout=5)
            finished_at = time.perf_counter()

            if worker.is_alive():
                raise TimeoutError("benchmark exceeded 300 seconds")
            if errors:
                raise errors[0]

            final_rows = workflow.storage.db_connection().execute(
                "SELECT status, COUNT(*) AS count FROM jobs GROUP BY status"
            ).fetchall()
            final_counts = {
                str(row["status"]): int(row["count"]) for row in final_rows
            }
            with counter_lock:
                provider_final = provider_completed
                output_final = outputs_written
                provider_at = first_provider_at
                output_at = first_output_at

            # Include an exact final sample even when the sampling thread stopped
            # immediately after the workflow returned.
            samples.append(
                (
                    finished_at - started_at,
                    provider_final,
                    output_final,
                    final_counts.get("queued", 0),
                    final_counts.get("running", 0),
                    final_counts.get("done", 0),
                    final_counts.get("failed", 0),
                )
            )

            first_terminal = next((time_value for time_value, *_, done, failed in samples if done + failed), None)
            admitted_all = next((time_value for time_value, _, _, queued_count, *_ in samples if queued_count == 0), None)
            max_provider_not_terminal = max(
                provider - done - failed
                for _, provider, _, _, _, done, failed in samples
            )
            max_output_not_terminal = max(
                outputs - done - failed
                for _, _, outputs, _, _, done, failed in samples
            )
            max_provider_without_monitor_row = max(
                0,
                max(
                    provider - running - done - failed
                    for _, provider, _, _, running, done, failed in samples
                ),
            )

            terminal_rows = workflow.storage.db_connection().execute(
                "SELECT node_name, job_id, time FROM job_events "
                "WHERE event IN ('done','failed','cancelled','skipped')"
            ).fetchall()
            publish_lags = []
            for row in terminal_rows:
                key = (str(row["node_name"]), int(row["job_id"]))
                output_wall = output_wall_times.get(key)
                if output_wall is None:
                    continue
                event_wall = datetime.fromisoformat(str(row["time"])).timestamp()
                publish_lags.append(max(0.0, event_wall - output_wall))
            publish_lags.sort()

            def percentile(values: list[float], fraction: float) -> float | None:
                if not values:
                    return None
                index = min(len(values) - 1, max(0, math.ceil(len(values) * fraction) - 1))
                return values[index]

            return Result(
                handlers=handlers,
                queued_handlers=queued_handlers,
                jobs_per_handler=jobs_per_handler,
                max_threads_per_handler=max_threads,
                jobs=total,
                elapsed_seconds=_rounded(finished_at - started_at),
                first_provider_response_seconds=_rounded(
                    None if provider_at is None else provider_at - started_at
                ),
                first_output_seconds=_rounded(
                    None if output_at is None else output_at - started_at
                ),
                first_monitor_terminal_seconds=_rounded(first_terminal),
                all_jobs_admitted_seconds=_rounded(admitted_all),
                max_provider_not_terminal=max_provider_not_terminal,
                max_output_not_terminal=max_output_not_terminal,
                max_provider_without_monitor_row=max_provider_without_monitor_row,
                terminal_publish_lag_p50_seconds=_rounded(percentile(publish_lags, 0.50)),
                terminal_publish_lag_p95_seconds=_rounded(percentile(publish_lags, 0.95)),
                terminal_publish_lag_max_seconds=_rounded(max(publish_lags) if publish_lags else None),
                final_queued=final_counts.get("queued", 0),
                final_running=final_counts.get("running", 0),
                final_done=final_counts.get("done", 0),
                final_failed=final_counts.get("failed", 0),
            )
    finally:
        close_shared_http_transport()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--jobs-per-handler",
        type=int,
        nargs="+",
        default=[256],
        help="One or more queue sizes to run (default: 256).",
    )
    parser.add_argument("--handlers", type=int, default=10)
    parser.add_argument(
        "--max-threads",
        type=int,
        default=None,
        help="Per-handler API limit; defaults to jobs-per-handler.",
    )
    parser.add_argument(
        "--queued-handlers",
        type=int,
        default=None,
        help="Queue jobs on only this many handlers while retaining the full graph.",
    )
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--sample-seconds", type=float, default=0.005)
    args = parser.parse_args()

    for jobs_per_handler in args.jobs_per_handler:
        result = run_case(
            jobs_per_handler,
            max_threads=args.max_threads,
            handlers=args.handlers,
            queued_handlers=args.queued_handlers,
            seed=args.seed,
            sample_seconds=args.sample_seconds,
        )
        print(json.dumps(asdict(result), sort_keys=True))


if __name__ == "__main__":
    main()
