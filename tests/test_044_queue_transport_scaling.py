from __future__ import annotations

import asyncio
import threading
import time
from concurrent.futures import Future

import httpx

from micro_workflow_manager import MicroWorkflow, NodeRouter
from micro_workflow_manager.networking import (
    close_shared_http_transport,
    configure_shared_http_transport,
    shared_http_transport,
)
from micro_workflow_manager.runners.api import ApiRunner


class CountingFuture(Future):
    done_calls = 0

    def done(self):
        type(self).done_calls += 1
        return super().done()


def test_fiber_completions_are_callback_driven_not_full_map_scans():
    count = 1000
    futures = [CountingFuture() for _ in range(count)]
    CountingFuture.done_calls = 0

    def complete_progressively():
        time.sleep(0.02)
        for future in futures:
            future.set_result(None)
            time.sleep(0.00005)

    completer = threading.Thread(target=complete_progressively)
    completer.start()
    try:
        result = ApiRunner(max_threads=count, poll_interval=0.001).run_jobs(
            "A",
            list(range(count)),
            lambda index: (futures[index].result(), index)[1],
        )
    finally:
        completer.join(timeout=2)

    assert result == list(range(count))
    assert CountingFuture.done_calls < count * 10


def test_live_component_queue_notification_wakes_before_poll_fallback(tmp_path):
    workflow = MicroWorkflow(tmp_path, runner="threaded")
    workflow.component_queue_poll_seconds = 5.0
    workflow.graph([("router", "worker"), ("worker", "router")])
    worker_started = threading.Event()
    release_router = threading.Event()

    router = NodeRouter("router", runner="threaded", max_threads=1)
    router.create_job(number=1)

    @router.task
    def route(ctx):
        ctx.node("worker").add(value=ctx.job_id)
        assert release_router.wait(2)

    worker = NodeRouter("worker", runner="api", max_threads=1)

    @worker.task
    def work(ctx, value):
        worker_started.set()
        return value

    workflow.include_router(router)
    workflow.include_router(worker)

    run = threading.Thread(
        target=lambda: workflow.run_node("router", ignore_readiness=True)
    )
    run.start()
    try:
        assert worker_started.wait(0.5), "worker pump slept until the polling fallback"
    finally:
        release_router.set()
        run.join(timeout=3)
    assert not run.is_alive()


def test_http2_transport_shards_by_in_flight_stream_count():
    async def handler(request: httpx.Request) -> httpx.Response:
        await asyncio.sleep(0.08)
        return httpx.Response(200, json={"ok": True}, request=request)

    close_shared_http_transport()
    configure_shared_http_transport(
        http2=True,
        streams_per_connection=3,
        transport=httpx.MockTransport(handler),
    )
    try:
        results = ApiRunner(max_threads=10, poll_interval=0.001).run_jobs(
            "A",
            list(range(10)),
            lambda index: shared_http_transport.post_json(
                "https://example.test/chat",
                timeout=1,
                json={"index": index},
            ),
        )
        snapshot = shared_http_transport.snapshot()
    finally:
        close_shared_http_transport()

    assert len(results) == 10
    assert snapshot["http2"] is True
    assert snapshot["streams_per_connection"] == 3
    assert snapshot["client_count"] == 4
    assert max(snapshot["peak_in_flight_per_client"]) <= 3
    assert sum(snapshot["peak_in_flight_per_client"]) == 10


def test_transport_sharding_does_not_cap_node_concurrency():
    release: Future[None] = Future()
    active = 0
    peak = 0
    lock = threading.Lock()

    def run_one(value):
        nonlocal active, peak
        with lock:
            active += 1
            peak = max(peak, active)
            if peak == 50:
                release.set_result(None)
        release.result()
        return value

    result = ApiRunner(max_threads=50, poll_interval=0.001).run_jobs(
        "A", list(range(50)), run_one
    )
    assert result == list(range(50))
    assert peak == 50
