"""Compare API startup strategies using the same telemetry as ``mwf top``.

Each repetition runs in a fresh Python process against the copied ten-node
``explode`` Hoeflein component. The child samples ``top_snapshot`` while the
variable-latency mock provider is live, so strategy selection includes the same
writer backlog and lifecycle-rate signals shown by ``mwf top`` without allowing
one candidate's threads or caches to affect the next candidate.

A strategy is eligible only when every repetition has no missing monitor rows,
no final queued/running/failed residue, and exact output-to-terminal p95 <= 50ms.
The eligible strategy with the lowest median all-jobs-admitted time wins; first
provider response and total elapsed time are deterministic tie breakers.
"""
from __future__ import annotations

import argparse
import json
import os
import statistics
import subprocess
import sys
import time
from dataclasses import asdict
from pathlib import Path

from micro_workflow_manager.cli.top import top_snapshot

from reproduce_explode_ghost import run_case


COMMON = {
    "MWF_API_PREFETCH": "0",
    "MWF_API_TERMINAL_MICROBATCH": "1",
    "MWF_API_MAX_ADMISSION_BURST": "96",
}

CANDIDATES = {
    "single": {**COMMON, "MWF_API_STARTUP_STRATEGY": "single"},
    "adaptive": {**COMMON, "MWF_API_STARTUP_STRATEGY": "adaptive"},
    "event": {
        "MWF_API_STARTUP_STRATEGY": "event",
        "MWF_API_EVENT_DRAIN_SECONDS": "0.010",
        **COMMON,
    },
    "balanced": {
        "MWF_API_STARTUP_STRATEGY": "balanced",
        "MWF_API_EVENT_DRAIN_SECONDS": "0.015",
        **COMMON,
    },
    "latency": {
        "MWF_API_STARTUP_STRATEGY": "latency",
        "MWF_API_EVENT_DRAIN_SECONDS": "0.050",
        **COMMON,
    },
}


def run_candidate(name: str, jobs: int, handlers: int, seed: int) -> dict:
    environment = CANDIDATES[name]
    previous = {key: os.environ.get(key) for key in environment}
    os.environ.update(environment)
    top_samples = []
    last_sample = 0.0

    def observe(workflow, nodes):
        nonlocal last_sample
        now_value = time.monotonic()
        if now_value - last_sample < 0.50:
            return
        last_sample = now_value
        top_samples.append(
            top_snapshot(workflow, nodes, window_seconds=1.0, recent_events=0)
        )

    try:
        result = run_case(
            jobs,
            handlers=handlers,
            seed=seed,
            sample_seconds=0.005,
            observer=observe,
        )
    finally:
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value

    peak_start_rate = max(
        (
            sum(node["starts_per_second"] for node in sample["nodes"])
            for sample in top_samples
        ),
        default=0.0,
    )
    peak_finish_rate = max(
        (
            sum(node["finishes_per_second"] for node in sample["nodes"])
            for sample in top_samples
        ),
        default=0.0,
    )
    peak_writer_backlog = max(
        (sample["mutation_writer"].get("durability_backlog", 0) for sample in top_samples),
        default=0,
    )
    row = {
        "strategy": name,
        **asdict(result),
        "top_peak_starts_per_second": round(peak_start_rate, 3),
        "top_peak_finishes_per_second": round(peak_finish_rate, 3),
        "top_peak_writer_backlog": peak_writer_backlog,
    }
    row["eligible"] = bool(
        row["max_provider_without_monitor_row"] == 0
        and row["final_queued"] == 0
        and row["final_running"] == 0
        and row["final_failed"] == 0
        and (row["terminal_publish_lag_p95_seconds"] or float("inf")) <= 0.050
    )
    return row


def _worker_command(strategy: str, jobs: int, handlers: int, seed: int) -> dict:
    script = Path(__file__).resolve()
    environment = os.environ.copy()
    root = str(script.parents[1])
    benchmark_dir = str(script.parent)
    environment["PYTHONPATH"] = os.pathsep.join(
        [root, benchmark_dir, environment.get("PYTHONPATH", "")]
    ).rstrip(os.pathsep)
    completed = subprocess.run(
        [
            sys.executable,
            str(script),
            "--worker",
            strategy,
            "--jobs-per-handler",
            str(jobs),
            "--handlers",
            str(handlers),
            "--seed",
            str(seed),
        ],
        text=True,
        capture_output=True,
        check=True,
        timeout=120,
        env=environment,
    )
    return json.loads(completed.stdout.strip().splitlines()[-1])


def _median(rows: list[dict], key: str):
    values = [row[key] for row in rows if row.get(key) is not None]
    return None if not values else statistics.median(values)


def aggregate(strategy: str, repetitions: list[dict]) -> dict:
    keys = (
        "first_provider_response_seconds",
        "first_output_seconds",
        "first_monitor_terminal_seconds",
        "all_jobs_admitted_seconds",
        "elapsed_seconds",
        "max_output_not_terminal",
        "terminal_publish_lag_p50_seconds",
        "terminal_publish_lag_p95_seconds",
        "terminal_publish_lag_max_seconds",
        "top_peak_starts_per_second",
        "top_peak_finishes_per_second",
        "top_peak_writer_backlog",
    )
    row = {"strategy": strategy, "repetitions": len(repetitions)}
    row.update({key: _median(repetitions, key) for key in keys})
    row["eligible"] = all(item["eligible"] for item in repetitions)
    row["worst_terminal_publish_lag_p95_seconds"] = max(
        item["terminal_publish_lag_p95_seconds"] for item in repetitions
    )
    row["worst_terminal_publish_lag_max_seconds"] = max(
        item["terminal_publish_lag_max_seconds"] for item in repetitions
    )
    row["max_provider_without_monitor_row"] = max(
        item["max_provider_without_monitor_row"] for item in repetitions
    )
    row["max_final_residue"] = max(
        item["final_queued"] + item["final_running"] + item["final_failed"]
        for item in repetitions
    )
    row["samples"] = repetitions
    return row


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--jobs-per-handler", type=int, default=128)
    parser.add_argument("--handlers", type=int, default=10)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--worker", choices=tuple(CANDIDATES), help=argparse.SUPPRESS)
    args = parser.parse_args()

    if args.worker:
        print(json.dumps(run_candidate(args.worker, args.jobs_per_handler, args.handlers, args.seed), sort_keys=True))
        return
    if args.repeats < 1:
        parser.error("--repeats must be >= 1")

    rows = []
    for strategy in CANDIDATES:
        repetitions = [
            _worker_command(
                strategy,
                args.jobs_per_handler,
                args.handlers,
                args.seed + repeat,
            )
            for repeat in range(args.repeats)
        ]
        rows.append(aggregate(strategy, repetitions))

    eligible = [row for row in rows if row["eligible"]]
    selected = min(
        eligible,
        key=lambda row: (
            row["all_jobs_admitted_seconds"] or float("inf"),
            row["first_provider_response_seconds"] or float("inf"),
            row["elapsed_seconds"] or float("inf"),
        ),
    ) if eligible else None

    payload = {"selected": selected and selected["strategy"], "rows": rows}
    if args.json:
        print(json.dumps(payload, sort_keys=True))
        return

    print(
        f"{'strategy':<10} {'eligible':<8} {'first':>8} {'admitted':>9} "
        f"{'elapsed':>8} {'p95-med':>9} {'p95-worst':>10} {'ghost-med':>9} "
        f"{'top-start/s':>11} {'writer':>7}"
    )
    for row in rows:
        print(
            f"{row['strategy']:<10} {str(row['eligible']):<8} "
            f"{row['first_provider_response_seconds']:>8.3f} "
            f"{row['all_jobs_admitted_seconds']:>9.3f} "
            f"{row['elapsed_seconds']:>8.3f} "
            f"{row['terminal_publish_lag_p95_seconds']:>9.3f} "
            f"{row['worst_terminal_publish_lag_p95_seconds']:>10.3f} "
            f"{row['max_output_not_terminal']:>9.0f} "
            f"{row['top_peak_starts_per_second']:>11.1f} "
            f"{row['top_peak_writer_backlog']:>7.0f}"
        )
    print(f"selected: {selected['strategy'] if selected else '(none)'}")


if __name__ == "__main__":
    main()
