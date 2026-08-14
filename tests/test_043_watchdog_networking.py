from __future__ import annotations

import asyncio
import threading
import time
from concurrent.futures import Future, TimeoutError as FutureTimeoutError
from threading import Event

import httpx
import pytest

from micro_workflow_manager import MicroWorkflow
from micro_workflow_manager.networking import (
    close_shared_http_transport,
    configure_shared_http_transport,
    shared_http_transport,
)
from micro_workflow_manager.network.transport import (
    SharedHTTPTransport,
    network_attempt_context,
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


def test_external_wait_replay_renews_same_per_attempt_transport_lease(tmp_path):
    workflow = MicroWorkflow(tmp_path, runner="api")
    workflow.graph([])
    watch = workflow.scheduler_supervisor.create_attempt(
        node_name="A",
        job_id=1,
        task_name="run",
        attempt=1,
        repeat_index=1,
        generation=0,
        execution_id=None,
        cancellation_event=Event(),
        total_timeout=10.0,
        checkpoint_timeout=1.0,
    )
    supervisor = workflow.scheduler_supervisor
    supervisor.begin_external_wait(
        watch,
        name="model request",
        timeout=0.5,
        cleanup_grace=0.1,
    )
    original_deadline = watch.external_wait_deadline
    original_lease = watch.external_wait_timeout

    supervisor.renew_external_wait(
        watch,
        reason="cohort_stream_stall",
    )

    assert watch.external_wait_timeout == original_lease == pytest.approx(0.6)
    assert watch.external_wait_deadline > original_deadline
    assert watch.external_wait_attempt == 2
    assert watch.external_wait_renewals == 1
    assert watch.external_wait_last_renewal_reason == "cohort_stream_stall"
    runtime = supervisor._runtime_payload(watch, state="running")
    assert runtime["external_wait_attempt"] == 2
    assert runtime["external_wait_renewals"] == 1
    assert runtime["external_wait_last_renewal_reason"] == "cohort_stream_stall"

    supervisor.end_external_wait(watch)
    supervisor.finish_attempt(watch, state="succeeded")


def test_deferred_external_wait_arms_only_at_first_physical_dispatch(tmp_path):
    workflow = MicroWorkflow(tmp_path, runner="api")
    workflow.graph([])
    watch = workflow.scheduler_supervisor.create_attempt(
        node_name="A",
        job_id=1,
        task_name="run",
        attempt=1,
        repeat_index=1,
        generation=0,
        execution_id=None,
        cancellation_event=Event(),
        total_timeout=10.0,
        checkpoint_timeout=0.01,
    )
    supervisor = workflow.scheduler_supervisor
    supervisor.begin_external_wait(
        watch,
        name="model request",
        timeout=0.5,
        cleanup_grace=0.1,
        defer_lease_start=True,
    )

    assert watch.external_wait_depth == 1
    assert watch.external_wait_attempt == 0
    assert watch.external_wait_deadline is None

    supervisor.renew_external_wait(
        watch,
        reason="initial_transport_attempt",
    )
    initial_deadline = watch.external_wait_deadline
    assert initial_deadline is not None
    assert watch.external_wait_attempt == 1
    assert watch.external_wait_renewals == 0
    assert watch.external_wait_last_renewal_reason is None

    supervisor.renew_external_wait(
        watch,
        reason="transport_error",
    )
    assert watch.external_wait_deadline >= initial_deadline
    assert watch.external_wait_attempt == 2
    assert watch.external_wait_renewals == 1
    assert watch.external_wait_last_renewal_reason == "transport_error"

    supervisor.end_external_wait(watch)
    supervisor.finish_attempt(watch, state="succeeded")


def test_external_wait_starts_when_physical_dispatch_starts(monkeypatch):
    calls = []

    class Supervisor:
        def begin_external_wait(
            self,
            watch,
            *,
            name,
            timeout,
            defer_lease_start=False,
        ):
            calls.append(
                ("begin", watch, name, timeout, defer_lease_start)
            )

        def renew_external_wait(self, watch, *, reason):
            calls.append(("renew", watch, reason))

        def end_external_wait(self, watch):
            calls.append(("end", watch))

    class Storage:
        project_dir = "probe"
        publish_network_manager_snapshot = None

    class Workflow:
        storage = Storage()
        scheduler_supervisor = Supervisor()

    def submit_request(method, url, **kwargs):
        assert calls == [
            ("begin", watch, "model request", 0.5, True),
        ]
        kwargs["attempt_callback"](1, None)
        future = Future()
        future.set_result(httpx.Response(200, json={"ok": True}))
        return future

    monkeypatch.setattr(
        "micro_workflow_manager.network.transport.network_manager.submit_request",
        submit_request,
    )
    watch = object()
    ctx = type("Context", (), {"current_node": "A", "job_id": 1})()
    with network_attempt_context(Workflow(), ctx, watch):
        response = SharedHTTPTransport().request(
            "POST",
            "https://example.test/model",
            timeout=(0.2, 0.5),
            wait_name="model request",
            json={},
        )

    assert response.status_code == 200
    assert calls == [
        ("begin", watch, "model request", 0.5, True),
        ("renew", watch, "initial_transport_attempt"),
        ("end", watch),
    ]


def test_external_wait_ends_when_network_submission_fails(monkeypatch):
    calls = []

    class Supervisor:
        def begin_external_wait(self, watch, **kwargs):
            calls.append(("begin", watch, kwargs["defer_lease_start"]))

        def end_external_wait(self, watch):
            calls.append(("end", watch))

    class Storage:
        project_dir = "probe"
        publish_network_manager_snapshot = None

    class Workflow:
        storage = Storage()
        scheduler_supervisor = Supervisor()

    monkeypatch.setattr(
        "micro_workflow_manager.network.transport.network_manager.submit_request",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("submit failed")),
    )
    watch = object()
    ctx = type("Context", (), {"current_node": "A", "job_id": 1})()
    with network_attempt_context(Workflow(), ctx, watch):
        with pytest.raises(RuntimeError, match="submit failed"):
            SharedHTTPTransport().request(
                "POST",
                "https://example.test/model",
                timeout=0.5,
                json={},
            )

    assert calls == [
        ("begin", watch, True),
        ("end", watch),
    ]


def test_async_external_wait_uses_physical_dispatch_lifecycle(monkeypatch):
    calls = []

    class Supervisor:
        def begin_external_wait(self, watch, **kwargs):
            calls.append(("begin", watch, kwargs["defer_lease_start"]))

        def renew_external_wait(self, watch, *, reason):
            calls.append(("renew", watch, reason))

        def end_external_wait(self, watch):
            calls.append(("end", watch))

    class Storage:
        project_dir = "probe"
        publish_network_manager_snapshot = None

    class Workflow:
        storage = Storage()
        scheduler_supervisor = Supervisor()

    def submit_request(method, url, **kwargs):
        kwargs["attempt_callback"](1, None)
        future = Future()
        future.set_result(httpx.Response(200, json={"ok": True}))
        return future

    monkeypatch.setattr(
        "micro_workflow_manager.network.transport.network_manager.submit_request",
        submit_request,
    )
    watch = object()
    ctx = type("Context", (), {"current_node": "A", "job_id": 1})()

    async def perform_request():
        with network_attempt_context(Workflow(), ctx, watch):
            return await SharedHTTPTransport().async_request(
                "POST",
                "https://example.test/model",
                timeout=0.5,
                json={},
            )

    response = asyncio.run(perform_request())
    assert response.status_code == 200
    assert calls == [
        ("begin", watch, True),
        ("renew", watch, "initial_transport_attempt"),
        ("end", watch),
    ]


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
