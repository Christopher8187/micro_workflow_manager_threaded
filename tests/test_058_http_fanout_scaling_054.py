import asyncio
import threading

import httpx

from micro_workflow_manager import MicroWorkflow, NodeRouter
from micro_workflow_manager.models import Job
from micro_workflow_manager.networking import (
    close_shared_http_transport,
    configure_shared_http_transport,
    shared_http_transport,
)
from micro_workflow_manager.runners.api import ApiRunner


def test_preclaimed_api_burst_records_first_task_started_in_claim_batch(tmp_path):
    workflow = MicroWorkflow(tmp_path, runner="api")
    workflow.active_job_restart_enabled = True
    workflow.graph([])
    router = NodeRouter("A", runner="api", max_threads=16)

    @router.task
    def work(ctx, value):
        return value

    workflow.include_router(router)
    workflow.add_jobs(None, "A", [{"value": value} for value in range(16)])
    workflow.run_node("A")

    for job_id in range(1, 17):
        names = [event["event"] for event in workflow.storage.read_job_events("A", job_id)]
        assert names.count("task_started") == 1
        assert names.index("started") < names.index("task_started") < names.index("done")


def test_node_pump_defers_per_job_node_status_queries(tmp_path, monkeypatch):
    workflow = MicroWorkflow(tmp_path, runner="api")
    workflow.active_job_restart_enabled = True
    workflow.graph([])
    router = NodeRouter("A", runner="api", max_threads=64)

    @router.task
    def work(ctx, value):
        return value

    workflow.include_router(router)
    workflow.add_jobs(None, "A", [{"value": value} for value in range(128)])

    original = workflow.storage.get_node_status
    calls = 0

    def counted(node_name):
        nonlocal calls
        calls += 1
        return original(node_name)

    monkeypatch.setattr(workflow.storage, "get_node_status", counted)
    workflow.run_node("A")

    # The node pump owns status publication. The old path did at least one
    # get_node_status() per completed job, creating 128 avoidable SQLite reads.
    assert calls < 24


def test_http1_transport_uses_small_elastic_shards_by_default(monkeypatch):
    async def handler(request: httpx.Request) -> httpx.Response:
        await asyncio.sleep(0.08)
        return httpx.Response(200, content=b"ok", request=request)

    monkeypatch.delenv("MWF_HTTP1_CONNECTIONS_PER_SHARD", raising=False)
    close_shared_http_transport()
    configure_shared_http_transport(
        http2=False,
        streams_per_connection=100,
        transport=httpx.MockTransport(handler),
    )
    try:
        results = ApiRunner(max_threads=50, poll_interval=0.001).run_jobs(
            "A",
            list(range(50)),
            lambda _index: shared_http_transport.request(
                "GET", "https://example.test/", timeout=1
            ).status_code,
        )
        snapshot = shared_http_transport.snapshot()
    finally:
        close_shared_http_transport()

    assert results == [200] * 50
    assert snapshot["http2"] is False
    assert snapshot["streams_per_connection"] == 100
    assert snapshot["http1_connections_per_shard"] == 16
    assert snapshot["shard_capacity"] == 16
    assert snapshot["client_count"] == 4
    assert max(snapshot["peak_in_flight_per_client"]) <= 16
    assert sum(snapshot["peak_in_flight_per_client"]) == 50


def test_http1_shard_capacity_is_configurable_without_capping_concurrency():
    release = threading.Event()
    entered = 0
    lock = threading.Lock()

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal entered
        with lock:
            entered += 1
            if entered == 40:
                release.set()
        while not release.is_set():
            await asyncio.sleep(0.001)
        return httpx.Response(200, content=b"ok", request=request)

    close_shared_http_transport()
    configure_shared_http_transport(
        http2=False,
        streams_per_connection=100,
        http1_connections_per_shard=8,
        transport=httpx.MockTransport(handler),
    )
    try:
        results = ApiRunner(max_threads=40, poll_interval=0.001).run_jobs(
            "A",
            list(range(40)),
            lambda _index: shared_http_transport.request(
                "GET", "https://example.test/", timeout=2
            ).status_code,
        )
        snapshot = shared_http_transport.snapshot()
    finally:
        close_shared_http_transport()

    assert results == [200] * 40
    assert snapshot["shard_capacity"] == 8
    assert snapshot["client_count"] == 5
    assert max(snapshot["peak_in_flight_per_client"]) <= 8


def test_preclaimed_burst_does_not_record_task_started_before_required_param_validation(tmp_path):
    workflow = MicroWorkflow(tmp_path, runner="api")
    workflow.active_job_restart_enabled = True
    workflow.graph([])
    router = NodeRouter("A", runner="api", max_threads=8)

    @router.task
    def work(ctx, required_value):
        return required_value

    workflow.include_router(router)
    # Bypass NodeRouter.create_job validation deliberately: this is a durable
    # queued job recovered from storage with malformed params. The optimized
    # claim path must preserve the old trace semantics and not announce that
    # the task started before invocation validation succeeds.
    workflow.storage.create_job(Job(node_name="A", job_id=1, params={}))
    try:
        workflow.run_node("A")
    except Exception:
        pass

    events = workflow.storage.read_job_events("A", 1)
    names = [event["event"] for event in events]
    assert "started" in names
    assert "task_started" not in names
    assert "failed" in names


def test_wide_fanout_does_not_republish_terminal_status_for_each_sibling(tmp_path, monkeypatch):
    workflow = MicroWorkflow(tmp_path, runner="api")
    workflow.active_job_restart_enabled = True
    children = [f"H{i:02d}" for i in range(8)]
    workflow.graph([("source", child) for child in children])

    source = NodeRouter("source", runner="threaded", max_threads=1)
    source.create_job(params={})

    @source.task
    def publish(ctx):
        for child in children:
            ctx.node(child).add_many([{"value": i} for i in range(4)])
        return len(children)

    workflow.include_router(source)
    for child in children:
        router = NodeRouter(child, runner="api", max_threads=4)

        @router.task
        def work(ctx, value):
            return value

        workflow.include_router(router)

    counts = {}
    original_one = workflow.storage.set_node_status
    original_many = workflow.storage.set_node_statuses

    def one(node_name, status):
        counts[(node_name, status)] = counts.get((node_name, status), 0) + 1
        return original_one(node_name, status)

    def many(statuses):
        for node_name, status in statuses.items():
            counts[(node_name, status)] = counts.get((node_name, status), 0) + 1
        return original_many(statuses)

    monkeypatch.setattr(workflow.storage, "set_node_status", one)
    monkeypatch.setattr(workflow.storage, "set_node_statuses", many)
    workflow.run()

    for child in children:
        assert counts.get((child, "running"), 0) == 1
        assert counts.get((child, "done"), 0) == 1


def test_programmatic_wide_fanout_retains_ephemeral_router_identity(tmp_path):
    import gc

    workflow = MicroWorkflow(tmp_path, runner="direct")
    names = [f"N{i:03d}" for i in range(100)]
    workflow.graph([("source", name) for name in names])

    source = NodeRouter("source", runner="direct")
    source.create_job(params={})

    @source.task
    def root(ctx):
        return "ok"

    workflow.include_router(source)
    for name in names:
        router = NodeRouter(name, runner="direct")

        @router.task
        def work(ctx):
            return "ok"

        workflow.include_router(router)
        # Force the exact lifetime pattern that made bare id(router)
        # deduplication unsafe.
        del router
        gc.collect()

    assert all(workflow.nodes[name].main_task is not None for name in names)
    assert len(workflow._included_routers) == 101
