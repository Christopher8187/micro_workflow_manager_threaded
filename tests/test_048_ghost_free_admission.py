from __future__ import annotations

import asyncio
import json
import math
import threading
import time
from datetime import datetime

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
    "exploderemark",
    "explodetheorem",
)


def _percentile(values: list[float], fraction: float) -> float:
    values = sorted(values)
    index = min(len(values) - 1, max(0, math.ceil(len(values) * fraction) - 1))
    return values[index]


def test_balanced_high_concurrency_has_no_ghost_visibility_regression(
    tmp_path,
    monkeypatch,
):
    """A bounded, non-stress regression for output/state divergence.

    The old admission loop could leave provider-completed/output-backed jobs
    monitor-visible as non-terminal for around a second. This test creates an
    uneven high-limit Hoeflein wave, samples SQLite while it runs, and checks the
    exact output-write -> durable terminal-event latency without relying on a
    long autostart-cycle stress environment.
    """
    monkeypatch.setenv("MWF_API_STARTUP_STRATEGY", "balanced")
    monkeypatch.setenv("MWF_API_MAX_ADMISSION_BURST", "512")
    monkeypatch.setenv("MWF_API_ADMISSION_TARGET_ROUNDS", "4")
    monkeypatch.setenv("MWF_API_EVENT_DRAIN_SECONDS", "0.010")
    monkeypatch.setenv("MWF_API_TERMINAL_MICROBATCH", "1")

    close_shared_http_transport()
    lock = threading.Lock()
    provider_completed = 0
    output_times: dict[tuple[str, int], float] = {}

    async def provider(request: httpx.Request) -> httpx.Response:
        nonlocal provider_completed
        payload = json.loads(request.content)
        await asyncio.sleep(0.001 + ((payload["value"] * 7 + payload["node"]) % 13) * 0.0007)
        with lock:
            provider_completed += 1
        return httpx.Response(200, json={"ok": True}, request=request)

    configure_shared_http_transport(
        http2=True,
        streams_per_connection=1024,
        transport=httpx.MockTransport(provider),
    )

    workflow = MicroWorkflow(tmp_path, runner="api")
    workflow.active_job_restart_enabled = True
    edges = []
    for name in HANDLERS:
        edges.extend((("explode", name), (name, "explode")))
    workflow.graph(edges)

    explode = NodeRouter(
        "explode",
        runner="threaded",
        max_threads=1,
        wait_for=list(HANDLERS),
    )

    @explode.task
    def run_explode(ctx):
        return None

    workflow.include_router(explode)
    routers = [explode]
    limits = (3000, 5000, 12000, 5000, 9000, 6000, 5000, 5000)
    counts = (40, 64, 96, 48, 128, 72, 52, 100)
    for node_index, (name, limit, count) in enumerate(zip(HANDLERS, limits, counts)):
        router = NodeRouter(name, runner="api", max_threads=limit, wait_for=["explode"])

        def make_handler(index):
            def run_handler(ctx, value):
                return shared_http_transport.post_json(
                    "https://mock.local/explode",
                    timeout=10,
                    json={"value": value, "node": index},
                )

            return run_handler

        router.task(timeout=20)(make_handler(node_index))
        workflow.include_router(router)
        routers.append(router)
        workflow.add_jobs(None, name, [{"value": value} for value in range(count)])

    original_write_output = workflow.storage.write_output

    def write_output(node_name, job_id, data):
        result = original_write_output(node_name, job_id, data)
        with lock:
            output_times[(node_name, int(job_id))] = time.time()
        return result

    workflow.storage.write_output = write_output
    stop = threading.Event()
    max_missing_row = 0

    def sample():
        nonlocal max_missing_row
        while not stop.wait(0.002):
            with lock:
                completed = provider_completed
            rows = workflow.storage.db_connection().execute(
                "SELECT status, COUNT(*) AS count FROM jobs GROUP BY status"
            ).fetchall()
            counts_by_status = {str(row["status"]): int(row["count"]) for row in rows}
            visible = (
                counts_by_status.get("running", 0)
                + counts_by_status.get("done", 0)
                + counts_by_status.get("failed", 0)
            )
            max_missing_row = max(max_missing_row, completed - visible)
        workflow.storage.close_thread_connection()

    monitor = threading.Thread(target=sample, name="ghost-regression-monitor")
    monitor.start()
    errors = []

    def run():
        try:
            workflow.run_node("explode", ignore_readiness=True)
        except BaseException as error:  # pragma: no cover - surfaced below
            errors.append(error)

    started = time.perf_counter()
    worker = threading.Thread(target=run, name="ghost-regression-workflow")
    worker.start()
    worker.join(timeout=20)
    stop.set()
    monitor.join(timeout=2)
    close_shared_http_transport()

    assert not worker.is_alive(), "high-concurrency ghost regression test timed out"
    assert not errors
    assert time.perf_counter() - started < 20
    workflow.storage.flush_db_mutations()

    status_rows = workflow.storage.db_connection().execute(
        "SELECT status, COUNT(*) AS count FROM jobs GROUP BY status"
    ).fetchall()
    final = {str(row["status"]): int(row["count"]) for row in status_rows}
    assert final.get("queued", 0) == 0
    assert final.get("running", 0) == 0
    assert final.get("failed", 0) == 0
    assert final.get("done", 0) == sum(counts)
    assert max_missing_row == 0

    terminal_rows = workflow.storage.db_connection().execute(
        "SELECT node_name, job_id, time FROM job_events WHERE event='done'"
    ).fetchall()
    lags = []
    for row in terminal_rows:
        output_time = output_times[(str(row["node_name"]), int(row["job_id"]))]
        terminal_time = datetime.fromisoformat(str(row["time"])).timestamp()
        lags.append(max(0.0, terminal_time - output_time))
    assert len(lags) == sum(counts)
    assert _percentile(lags, 0.95) < 0.250
    assert max(lags) < 0.750


def test_balanced_admits_a_small_27_job_tail_without_plateau_stall(tmp_path, monkeypatch):
    monkeypatch.setenv("MWF_API_STARTUP_STRATEGY", "balanced")
    monkeypatch.setenv("MWF_API_MAX_ADMISSION_BURST", "512")
    close_shared_http_transport()

    async def provider(request: httpx.Request) -> httpx.Response:
        await asyncio.sleep(0.001)
        return httpx.Response(200, json={"ok": True}, request=request)

    configure_shared_http_transport(
        http2=True,
        streams_per_connection=64,
        transport=httpx.MockTransport(provider),
    )
    try:
        workflow = MicroWorkflow(tmp_path, runner="api")
        workflow.graph([])
        router = NodeRouter("explodeexercise", runner="api", max_threads=9000)

        @router.task(timeout=10)
        def run_job(ctx, value):
            return shared_http_transport.post_json(
                "https://mock.local/tail", timeout=5, json={"value": value}
            )

        workflow.include_router(router)
        workflow.add_jobs(
            None,
            "explodeexercise",
            [{"value": value} for value in range(27)],
        )
        started = time.perf_counter()
        workflow.run_node("explodeexercise", ignore_readiness=True)
        assert time.perf_counter() - started < 5
        row = workflow.storage.db_connection().execute(
            "SELECT COUNT(*) AS count FROM jobs "
            "WHERE node_name='explodeexercise' AND status='done'"
        ).fetchone()
        assert int(row["count"]) == 27
        started_events = workflow.storage.db_connection().execute(
            "SELECT COUNT(*) AS count FROM job_events "
            "WHERE node_name='explodeexercise' AND event='started'"
        ).fetchone()
        assert int(started_events["count"]) == 27
    finally:
        close_shared_http_transport()
