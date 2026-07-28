from __future__ import annotations

import asyncio
import threading
import time
from concurrent.futures import Future, TimeoutError as FutureTimeoutError

import httpx
import pytest

from micro_workflow_manager import MicroWorkflow
from micro_workflow_manager.networking import (
    close_shared_http_transport,
    configure_shared_http_transport,
    shared_http_transport,
)
from micro_workflow_manager.runners.api import ApiRunner


def test_cooperative_future_result_preserves_periodic_timeout_semantics():
    future: Future[str] = Future()
    timer = threading.Timer(0.12, lambda: future.set_result("done"))
    timer.start()
    ticks = 0

    def job(_):
        nonlocal ticks
        while True:
            try:
                return future.result(timeout=0.02)
            except FutureTimeoutError:
                ticks += 1

    try:
        assert ApiRunner(max_threads=1, poll_interval=0.002).run_jobs("A", [1], job) == ["done"]
    finally:
        timer.join(timeout=1)
    assert ticks >= 3


def test_framework_http_wait_suspends_checkpoint_watchdog(tmp_path):
    async def handler(request: httpx.Request) -> httpx.Response:
        await asyncio.sleep(0.15)
        return httpx.Response(200, json={"ok": True}, request=request)

    close_shared_http_transport()
    configure_shared_http_transport(transport=httpx.MockTransport(handler))
    workflow = MicroWorkflow(tmp_path, runner="api")
    workflow.graph([("A", "B")])

    @workflow.task("A", runner="api", max_threads=20, timeout=2.0, checkpoint_timeout=0.03)
    def a(ctx):
        ctx.checkpoint("before network", timeout=0.03)
        return shared_http_transport.post_json(
            "https://example.test/chat",
            timeout=0.5,
            json={"x": 1},
            wait_name="mock model request",
        )

    @workflow.task("B")
    def b(ctx):
        return None

    workflow.start("A", job_id=1)
    try:
        workflow.run_node("A", ignore_readiness=True)
    finally:
        close_shared_http_transport()
    assert workflow.storage.job_status_counts("A").get("done") == 1


def test_network_wait_does_not_suspend_total_task_timeout(tmp_path):
    async def handler(request: httpx.Request) -> httpx.Response:
        await asyncio.sleep(0.3)
        return httpx.Response(200, json={"ok": True}, request=request)

    close_shared_http_transport()
    configure_shared_http_transport(transport=httpx.MockTransport(handler))
    workflow = MicroWorkflow(tmp_path, runner="api")
    workflow.graph([("A", "B")])

    @workflow.task("A", runner="api", max_threads=1, timeout=0.06, checkpoint_timeout=0.02)
    def a(ctx):
        ctx.checkpoint("before network", timeout=0.02)
        return shared_http_transport.post_json(
            "https://example.test/slow",
            timeout=1.0,
            json={},
            wait_name="slow model request",
        )

    @workflow.task("B")
    def b(ctx):
        return None

    workflow.start("A", job_id=1)
    started = time.monotonic()
    try:
        with pytest.raises(Exception):
            workflow.run_node("A", ignore_readiness=True)
    finally:
        close_shared_http_transport()
    assert time.monotonic() - started < 0.5
    assert workflow.storage.node_job_summary("A")["counts"].get("failed") == 1


def test_many_framework_network_waits_do_not_cascade_checkpoint_cancellations(tmp_path):
    async def handler(request: httpx.Request) -> httpx.Response:
        await asyncio.sleep(0.08)
        return httpx.Response(200, json={"ok": True}, request=request)

    close_shared_http_transport()
    configure_shared_http_transport(transport=httpx.MockTransport(handler))
    workflow = MicroWorkflow(tmp_path, runner="api")
    workflow.graph([("A", "B")])

    @workflow.task("A", runner="api", max_threads=100, timeout=3.0, checkpoint_timeout=0.02)
    def a(ctx):
        ctx.checkpoint("model request started", timeout=0.02)
        return shared_http_transport.post_json(
            "https://example.test/model",
            timeout=0.5,
            json={"job": ctx.job_id},
            wait_name="model request",
        )

    @workflow.task("B")
    def b(ctx):
        return None

    for job_id in range(1, 101):
        workflow.start("A", job_id=job_id)
    try:
        workflow.run_node("A", ignore_readiness=True)
    finally:
        close_shared_http_transport()
    counts = workflow.storage.node_job_summary("A")["counts"]
    assert counts.get("done") == 100
    assert counts.get("failed", 0) == 0
