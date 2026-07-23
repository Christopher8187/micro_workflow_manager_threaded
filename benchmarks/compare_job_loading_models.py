"""Benchmark MWF API admission models on uneven high-concurrency Hoeflein graphs.

The profiles include the exact explode(workings) shape from the supplied monitor
trace and a much larger skewed case with several nodes above 3,000 jobs and
several below 500. Every candidate is run in a fresh process. A candidate is
eligible only when all jobs are durably terminal, no provider completion lacks a
monitor row, and exact output->terminal p95 remains below the configured ghost
gate.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import math
import os
import statistics
import subprocess
import sys
import tempfile
import threading
import time
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path

import httpx

from micro_workflow_manager import MicroWorkflow, NodeRouter
from micro_workflow_manager.networking import (
    close_shared_http_transport,
    configure_shared_http_transport,
    shared_http_transport,
)

from reproduce_explode_ghost import HANDLERS


OBSERVED_JOBS = [272, 205, 537, 258, 1192, 298, 80, 53, 363, 564]
OBSERVED_THREADS = [3000, 5000, 12000, 5000, 9000, 6000, 5000, 12000, 5000, 5000]
SKEWED_JOBS = [420, 3200, 180, 3600, 300, 3300, 80, 340, 490, 350]
SKEWED_THREADS = [3000, 5000, 12000, 5000, 9000, 6000, 5000, 12000, 5000, 5000]
CAPACITY_BOUND_JOBS = [440, 3200, 160, 3100, 280, 3300, 90, 320, 460, 350]
CAPACITY_BOUND_THREADS = [350, 3000, 500, 3000, 450, 3000, 300, 300, 480, 300]

PROFILES = {
    "observed": (OBSERVED_JOBS, OBSERVED_THREADS),
    "skewed": (SKEWED_JOBS, SKEWED_THREADS),
    "capacity-bound": (CAPACITY_BOUND_JOBS, CAPACITY_BOUND_THREADS),
}

COMMON = {
    "MWF_API_PREFETCH": "0",
    "MWF_API_TERMINAL_MICROBATCH": "1",
    "MWF_API_EVENT_DRAIN_SECONDS": "0.010",
    "MWF_PARALLEL_JOB_PAYLOAD_READS": "1",
    "MWF_SQLITE_CLAIM_TRANSACTION_ROWS": "192",
}

CANDIDATES = {
    "balanced-047": {
        **COMMON,
        "MWF_API_STARTUP_STRATEGY": "balanced",
        "MWF_API_EVENT_DRAIN_SECONDS": "0.010",
        "MWF_API_MAX_ADMISSION_BURST": "512",
        "MWF_API_ADMISSION_TARGET_ROUNDS": "4",
    },
    "legacy-64": {
        **COMMON,
        "MWF_API_STARTUP_STRATEGY": "event",
        "MWF_API_MAX_ADMISSION_BURST": "96",
        "MWF_API_ADMISSION_TARGET_ROUNDS": "999999",
    },
    "window-256": {
        **COMMON,
        "MWF_API_STARTUP_STRATEGY": "event",
        "MWF_API_MAX_ADMISSION_BURST": "256",
        "MWF_API_ADMISSION_TARGET_ROUNDS": "999999",
    },
    "window-8": {
        **COMMON,
        "MWF_API_STARTUP_STRATEGY": "event",
        "MWF_API_MAX_ADMISSION_BURST": "1024",
        "MWF_API_ADMISSION_TARGET_ROUNDS": "8",
    },
    "window-4": {
        **COMMON,
        "MWF_API_STARTUP_STRATEGY": "event",
        "MWF_API_MAX_ADMISSION_BURST": "1024",
        "MWF_API_ADMISSION_TARGET_ROUNDS": "4",
    },
    "window-2": {
        **COMMON,
        "MWF_API_STARTUP_STRATEGY": "event",
        "MWF_API_MAX_ADMISSION_BURST": "2048",
        "MWF_API_ADMISSION_TARGET_ROUNDS": "2",
    },
    "elastic": {
        **COMMON,
        "MWF_API_STARTUP_STRATEGY": "elastic",
        "MWF_API_MAX_ADMISSION_BURST": "1024",
        "MWF_API_ADMISSION_TARGET_ROUNDS": "4",
        "MWF_API_JOBS_PER_STARTUP_LANE": "1500",
    },
    "lanes-2": {
        **COMMON,
        "MWF_API_STARTUP_STRATEGY": "lanes:2",
        "MWF_API_MAX_ADMISSION_BURST": "1024",
        "MWF_API_ADMISSION_TARGET_ROUNDS": "4",
    },
    "lanes-3": {
        **COMMON,
        "MWF_API_STARTUP_STRATEGY": "lanes:3",
        "MWF_API_MAX_ADMISSION_BURST": "1024",
        "MWF_API_ADMISSION_TARGET_ROUNDS": "4",
    },
    "lanes-4": {
        **COMMON,
        "MWF_API_STARTUP_STRATEGY": "lanes:4",
        "MWF_API_MAX_ADMISSION_BURST": "1024",
        "MWF_API_ADMISSION_TARGET_ROUNDS": "4",
    },
}


@dataclass(slots=True)
class ProfileResult:
    profile: str
    jobs: int
    handlers: int
    elapsed_seconds: float
    all_jobs_admitted_seconds: float | None
    first_provider_response_seconds: float | None
    first_output_seconds: float | None
    first_terminal_seconds: float | None
    max_provider_without_monitor_row: int
    max_output_not_terminal: int
    terminal_publish_lag_p50_seconds: float | None
    terminal_publish_lag_p95_seconds: float | None
    terminal_publish_lag_max_seconds: float | None
    final_queued: int
    final_running: int
    final_done: int
    final_failed: int


def _percentile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    values = sorted(values)
    index = min(len(values) - 1, max(0, math.ceil(len(values) * fraction) - 1))
    return values[index]


def run_profile(profile: str, seed: int, sample_seconds: float = 0.01) -> ProfileResult:
    job_counts, thread_counts = PROFILES[profile]
    selected = HANDLERS[: len(job_counts)]
    total = sum(job_counts)
    close_shared_http_transport()

    counter_lock = threading.Lock()
    provider_completed = 0
    outputs_written = 0
    first_provider_at = None
    first_output_at = None
    output_wall_times: dict[tuple[str, int], float] = {}

    async def mock_provider(request: httpx.Request) -> httpx.Response:
        nonlocal provider_completed, first_provider_at
        payload = json.loads(request.content)
        value = int(payload["value"])
        node_index = int(payload["node_index"])
        # 0.5ms to 35ms, with a small long tail every 97th request. This makes
        # completions race admission without making the benchmark nondeterministic.
        slot = (value * 1103515245 + node_index * 12345 + seed) % 48
        delay = 0.0005 + slot * 0.0007
        if (value + node_index * 17 + seed) % 97 == 0:
            delay += 0.040
        await asyncio.sleep(delay)
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
            prefix=f"mwf-load-{profile}-",
            ignore_cleanup_errors=True,
        ) as directory:
            workflow = MicroWorkflow(Path(directory), runner="api")
            workflow.active_job_restart_enabled = True
            edges = []
            for handler_name in selected:
                edges.extend((("explode", handler_name), (handler_name, "explode")))
            workflow.graph(edges)

            explode = NodeRouter(
                "explode", runner="threaded", max_threads=1, wait_for=list(selected)
            )

            @explode.task
            def run_explode(ctx):
                return None

            workflow.include_router(explode)
            mounted = [explode]

            for node_index, (handler_name, jobs, threads) in enumerate(
                zip(selected, job_counts, thread_counts)
            ):
                router = NodeRouter(
                    handler_name,
                    runner="api",
                    max_threads=threads,
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
                mounted.append(router)
                workflow.add_jobs(
                    None,
                    handler_name,
                    [{"value": value} for value in range(jobs)],
                )

            original_write_output = workflow.storage.write_output

            def counted_write_output(node_name, job_id, data):
                nonlocal outputs_written, first_output_at
                result = original_write_output(node_name, job_id, data)
                now_perf = time.perf_counter()
                with counter_lock:
                    outputs_written += 1
                    output_wall_times[(node_name, int(job_id))] = time.time()
                    if first_output_at is None:
                        first_output_at = now_perf
                return result

            workflow.storage.write_output = counted_write_output

            samples = []
            stop = threading.Event()
            started_at = time.perf_counter()

            def sample_monitor():
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
                    stop.wait(sample_seconds)
                workflow.storage.close_thread_connection()

            monitor = threading.Thread(target=sample_monitor, name="load-model-monitor")
            monitor.start()
            errors: list[BaseException] = []

            def worker():
                try:
                    workflow.run_node("explode", ignore_readiness=True)
                except BaseException as error:
                    errors.append(error)

            run_thread = threading.Thread(target=worker, name="load-model-workflow")
            run_thread.start()
            timeout_seconds = max(120, min(600, total / 40))
            run_thread.join(timeout=timeout_seconds)
            stop.set()
            monitor.join(timeout=5)
            finished_at = time.perf_counter()
            if run_thread.is_alive():
                raise TimeoutError(f"{profile} exceeded {timeout_seconds:.0f}s")
            if errors:
                raise errors[0]

            workflow.storage.flush_db_mutations()
            rows = workflow.storage.db_connection().execute(
                "SELECT status, COUNT(*) AS count FROM jobs GROUP BY status"
            ).fetchall()
            final = {str(row["status"]): int(row["count"]) for row in rows}
            with counter_lock:
                provider_final = provider_completed
                outputs_final = outputs_written
                provider_at = first_provider_at
                output_at = first_output_at
            samples.append(
                (
                    finished_at - started_at,
                    provider_final,
                    outputs_final,
                    final.get("queued", 0),
                    final.get("running", 0),
                    final.get("done", 0),
                    final.get("failed", 0),
                )
            )

            admitted = next((t for t, _, _, q, *_ in samples if q == 0), None)
            first_terminal = next(
                (t for t, *_, done, failed in samples if done + failed), None
            )
            max_missing = max(
                0,
                max(
                    provider - running - done - failed
                    for _, provider, _, _, running, done, failed in samples
                ),
            )
            max_output_not_terminal = max(
                outputs - done - failed
                for _, _, outputs, _, _, done, failed in samples
            )

            terminal_rows = workflow.storage.db_connection().execute(
                "SELECT node_name, job_id, time FROM job_events "
                "WHERE event IN ('done','failed','cancelled','skipped')"
            ).fetchall()
            lags = []
            for row in terminal_rows:
                output_time = output_wall_times.get(
                    (str(row["node_name"]), int(row["job_id"]))
                )
                if output_time is None:
                    continue
                terminal_time = datetime.fromisoformat(str(row["time"])).timestamp()
                lags.append(max(0.0, terminal_time - output_time))

            return ProfileResult(
                profile=profile,
                jobs=total,
                handlers=len(selected),
                elapsed_seconds=round(finished_at - started_at, 6),
                all_jobs_admitted_seconds=None if admitted is None else round(admitted, 6),
                first_provider_response_seconds=(
                    None if provider_at is None else round(provider_at - started_at, 6)
                ),
                first_output_seconds=(
                    None if output_at is None else round(output_at - started_at, 6)
                ),
                first_terminal_seconds=(
                    None if first_terminal is None else round(first_terminal, 6)
                ),
                max_provider_without_monitor_row=max_missing,
                max_output_not_terminal=max_output_not_terminal,
                terminal_publish_lag_p50_seconds=(
                    None if not lags else round(_percentile(lags, 0.50), 6)
                ),
                terminal_publish_lag_p95_seconds=(
                    None if not lags else round(_percentile(lags, 0.95), 6)
                ),
                terminal_publish_lag_max_seconds=(
                    None if not lags else round(max(lags), 6)
                ),
                final_queued=final.get("queued", 0),
                final_running=final.get("running", 0),
                final_done=final.get("done", 0),
                final_failed=final.get("failed", 0),
            )
    finally:
        close_shared_http_transport()


def worker(candidate: str, profile: str, seed: int) -> dict:
    environment = CANDIDATES[candidate]
    previous = {key: os.environ.get(key) for key in environment}
    os.environ.update(environment)
    try:
        result = asdict(run_profile(profile, seed))
    finally:
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
    result["candidate"] = candidate
    result["eligible"] = bool(
        result["max_provider_without_monitor_row"] == 0
        and result["final_queued"] == 0
        and result["final_running"] == 0
        and result["final_failed"] == 0
        and (result["terminal_publish_lag_p95_seconds"] or 999) <= 0.100
        and (result["terminal_publish_lag_max_seconds"] or 999) <= 0.750
    )
    return result


def run_subprocess(candidate: str, profile: str, seed: int) -> dict:
    script = Path(__file__).resolve()
    env = os.environ.copy()
    env["PYTHONPATH"] = os.pathsep.join(
        [str(script.parents[1]), str(script.parent), env.get("PYTHONPATH", "")]
    ).rstrip(os.pathsep)
    completed = subprocess.run(
        [
            sys.executable,
            str(script),
            "--worker",
            candidate,
            "--profile",
            profile,
            "--seed",
            str(seed),
        ],
        text=True,
        capture_output=True,
        timeout=700,
        env=env,
    )
    if completed.returncode != 0:
        return {
            "candidate": candidate,
            "profile": profile,
            "eligible": False,
            "error": (completed.stderr or completed.stdout)[-4000:],
        }
    return json.loads(completed.stdout.strip().splitlines()[-1])


def median(rows: list[dict], key: str):
    values = [row[key] for row in rows if row.get(key) is not None]
    return None if not values else statistics.median(values)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profiles", nargs="+", choices=tuple(PROFILES), default=["observed"])
    parser.add_argument("--candidates", nargs="+", choices=tuple(CANDIDATES), default=list(CANDIDATES))
    parser.add_argument("--repeats", type=int, default=2)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--worker", choices=tuple(CANDIDATES), help=argparse.SUPPRESS)
    parser.add_argument("--profile", choices=tuple(PROFILES), help=argparse.SUPPRESS)
    args = parser.parse_args()

    if args.worker:
        print(json.dumps(worker(args.worker, args.profile, args.seed), sort_keys=True))
        return

    all_rows = []
    for profile in args.profiles:
        for candidate in args.candidates:
            runs = [
                run_subprocess(candidate, profile, args.seed + repeat)
                for repeat in range(args.repeats)
            ]
            all_rows.append(
                {
                    "candidate": candidate,
                    "profile": profile,
                    "eligible": all(row.get("eligible", False) for row in runs),
                    "admitted_median": median(runs, "all_jobs_admitted_seconds"),
                    "elapsed_median": median(runs, "elapsed_seconds"),
                    "p95_median": median(runs, "terminal_publish_lag_p95_seconds"),
                    "p95_worst": max(
                        (row.get("terminal_publish_lag_p95_seconds") or 999 for row in runs),
                        default=999,
                    ),
                    "max_missing": max(
                        (row.get("max_provider_without_monitor_row", 999) for row in runs),
                        default=999,
                    ),
                    "runs": runs,
                }
            )

    candidates = []
    for candidate in args.candidates:
        rows = [row for row in all_rows if row["candidate"] == candidate]
        if rows and all(row["eligible"] for row in rows):
            candidates.append(
                (
                    sum(row["admitted_median"] for row in rows),
                    sum(row["elapsed_median"] for row in rows),
                    candidate,
                )
            )
    selected = min(candidates)[2] if candidates else None
    payload = {"selected": selected, "rows": all_rows}
    if args.json:
        print(json.dumps(payload, sort_keys=True))
        return

    print(f"{'profile':<15} {'candidate':<12} {'ok':<5} {'admitted':>10} {'elapsed':>10} {'p95':>9} {'p95-worst':>10} {'missing':>8}")
    for row in all_rows:
        print(
            f"{row['profile']:<15} {row['candidate']:<12} {str(row['eligible']):<5} "
            f"{(row['admitted_median'] or 0):>10.3f} {(row['elapsed_median'] or 0):>10.3f} "
            f"{(row['p95_median'] or 0):>9.3f} {row['p95_worst']:>10.3f} {row['max_missing']:>8}"
        )
    print(f"selected: {selected or '(none)'}")


if __name__ == "__main__":
    main()
