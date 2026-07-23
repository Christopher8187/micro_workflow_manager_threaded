from __future__ import annotations

import asyncio
import threading
import time
from contextlib import contextmanager
from concurrent.futures import Future

import httpx
import pytest

from micro_workflow_manager import MicroWorkflow, NodeRouter
from micro_workflow_manager.fibers import cancellation_scope
from micro_workflow_manager.monitor import workflow_snapshot
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


@pytest.mark.parametrize("runner", ["api", "threaded"])
def test_queue_pumps_preload_job_metadata_in_batches(tmp_path, monkeypatch, runner):
    workflow = MicroWorkflow(tmp_path, runner=runner)
    workflow.graph([("A", "sink")])

    @workflow.task("A", runner=runner, max_threads=32)
    def run_a(ctx, value):
        return value

    @workflow.task("sink")
    def sink(ctx):
        return None

    count = 100
    workflow.add_jobs(None, "A", [{"value": value} for value in range(count)])
    monkeypatch.setattr(
        workflow.storage,
        "read_job_metadata",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("queue admission must not query metadata per job")
        ),
    )

    workflow.run_node("A", ignore_readiness=True)
    assert workflow.storage.job_status_counts("A")["done"] == count


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


def test_api_admission_does_not_rescan_every_waiter_after_each_burst():
    count = 2000
    release: Future[None] = Future()
    entered = 0
    cancellation_checks = 0

    def check_cancelled():
        nonlocal cancellation_checks
        cancellation_checks += 1

    def run_one(value):
        nonlocal entered
        entered += 1
        if entered == count:
            release.set_result(None)
        with cancellation_scope(check_cancelled):
            release.result()
        return value

    result = ApiRunner(max_threads=count, poll_interval=0.05).run_jobs(
        "A",
        list(range(count)),
        run_one,
    )
    assert result == list(range(count))
    assert cancellation_checks < count * 6


def test_supervised_completion_wave_releases_file_fence_before_group_commit(
    tmp_path,
    monkeypatch,
):
    count = 400
    workflow = MicroWorkflow(tmp_path, runner="api")
    workflow.graph([("A", "sink")])
    release: Future[None] = Future()
    all_entered = threading.Event()
    entered = 0

    @workflow.task("A", runner="api", max_threads=count, timeout=30)
    def run_a(ctx, value):
        nonlocal entered
        entered += 1
        if entered == count:
            all_entered.set()
        release.result()
        return value

    @workflow.task("sink")
    def sink(ctx):
        return None

    workflow.add_jobs(None, "A", [{"value": value} for value in range(count)])
    workflow.active_job_restart_enabled = True

    active_fences = 0
    peak_fences = 0
    original = workflow.storage.filesystem_interprocess_lock

    @contextmanager
    def counted_fence(namespace, name):
        nonlocal active_fences, peak_fences
        with original(namespace, name):
            if namespace == "execution-fences":
                active_fences += 1
                peak_fences = max(peak_fences, active_fences)
            try:
                yield
            finally:
                if namespace == "execution-fences":
                    active_fences -= 1

    monkeypatch.setattr(
        workflow.storage,
        "filesystem_interprocess_lock",
        counted_fence,
    )

    worker = threading.Thread(
        target=lambda: workflow.run_node("A", ignore_readiness=True)
    )
    worker.start()
    assert all_entered.wait(10)
    release.set_result(None)
    worker.join(timeout=20)

    assert not worker.is_alive()
    assert workflow.storage.job_status_counts("A")["done"] == count
    assert peak_fences < 10


def test_api_terminal_commits_outrank_admission_and_reach_monitor(tmp_path, monkeypatch):
    count = 200
    workflow = MicroWorkflow(tmp_path, runner="api")
    workflow.graph([("A", "sink")])

    @workflow.task("A", runner="api", max_threads=count, timeout=30)
    def run_a(ctx, value):
        return value

    @workflow.task("sink")
    def sink(ctx):
        return None

    workflow.add_jobs(None, "A", [{"value": value} for value in range(count)])
    workflow.active_job_restart_enabled = True

    priorities = {"claim": set(), "finalize": set()}
    original = workflow.storage.submit_db_mutation
    original_grouped = workflow.storage.submit_grouped_db_mutation

    def record_priority(operation, *, wait=True, priority=10):
        name = getattr(operation, "__name__", "")
        if name in priorities:
            priorities[name].add(priority)
        return original(operation, wait=wait, priority=priority)

    def record_grouped(
        group_key, item, operation, *, wait=True, priority=10, collect_seconds=0.001
    ):
        if isinstance(group_key, tuple) and group_key[:1] == ("terminal",):
            priorities["finalize"].add(priority)
        if isinstance(group_key, tuple) and group_key[:1] == ("execution-claims",):
            priorities["claim"].add(priority)
        return original_grouped(
            group_key,
            item,
            operation,
            wait=wait,
            priority=priority,
            collect_seconds=collect_seconds,
        )

    monkeypatch.setattr(workflow.storage, "submit_db_mutation", record_priority)
    monkeypatch.setattr(
        workflow.storage,
        "submit_grouped_db_mutation",
        record_grouped,
    )
    workflow.run_node("A", ignore_readiness=True)

    row = next(
        row for row in workflow_snapshot(workflow)["nodes"] if row["node"] == "A"
    )
    assert priorities["claim"] == {10}
    assert priorities["finalize"] == {5}
    assert row["done"] == count
    assert row["running"] == 0
    assert row["queued"] == 0


def test_asymmetric_hoeflein_wave_admits_large_nodes_and_drains_cleanly(tmp_path):
    """A large component member must not starve behind smaller peer nodes."""
    workflow = MicroWorkflow(tmp_path, runner="api")
    workflow.active_job_restart_enabled = True
    workflow.graph([
        ("hub", "small"),
        ("small", "hub"),
        ("hub", "medium"),
        ("medium", "hub"),
        ("hub", "large"),
        ("large", "hub"),
    ])

    distribution = {
        "hub": 10,
        "small": 40,
        "medium": 80,
        "large": 120,
    }
    total = sum(distribution.values())
    release: Future[None] = Future()
    all_entered = threading.Event()
    entered = 0
    entered_lock = threading.Lock()

    def run(ctx, value):
        nonlocal entered
        with entered_lock:
            entered += 1
            if entered == total:
                all_entered.set()
        release.result()
        return value

    routers = []
    for node_name, count in distribution.items():
        router = NodeRouter(node_name, runner="api", max_threads=total)
        routers.append(router)
        router.task(run)
        workflow.include_router(router)
        workflow.add_jobs(
            None,
            node_name,
            [{"value": value} for value in range(count)],
        )

    worker = threading.Thread(
        target=lambda: workflow.run_node("hub", ignore_readiness=True)
    )
    worker.start()
    try:
        assert all_entered.wait(10), (
            f"only {entered}/{total} component jobs reached their handlers"
        )
    finally:
        release.set_result(None)
        worker.join(timeout=20)

    assert not worker.is_alive()
    for node_name, count in distribution.items():
        summary = workflow.storage.node_job_summary(node_name)["counts"]
        assert summary["done"] == count
        assert summary["queued"] == 0
        assert summary["running"] == 0


def test_dense_api_source_uses_bounded_admission_slices_and_sparse_source_resets():
    class DenseSource:
        def __init__(self, count):
            self.remaining = list(range(count))
            self.requests = []
            self.returned_counts = []

        def pull(self, max_items):
            self.requests.append(max_items)
            result = self.remaining[:max_items]
            del self.remaining[:max_items]
            self.returned_counts.append(len(result))
            return result

    dense = DenseSource(2000)
    results = ApiRunner(max_threads=2000, poll_interval=0.001).run_job_source(
        "A",
        dense,
        lambda value: value,
    )
    assert results == list(range(2000))
    assert dense.requests == [64, 128, 256, 512, 512, 512, 512, 16, 16]
    assert dense.returned_counts == [64, 128, 256, 512, 512, 512, 16, 0, 0]

    class SparseSource:
        def __init__(self):
            self.calls = 0
            self.requests = []

        def pull(self, max_items):
            self.requests.append(max_items)
            self.calls += 1
            if self.calls == 1:
                return list(range(64))
            if self.calls == 2:
                return list(range(64, 67))
            return []

    sparse = SparseSource()
    results = ApiRunner(max_threads=2000, poll_interval=0.001).run_job_source(
        "A",
        sparse,
        lambda value: value,
    )
    assert results == list(range(67))
    assert sparse.requests[:4] == [64, 128, 16, 16]


def test_completed_api_futures_are_serviced_during_dense_admission():
    count = 512
    futures = [Future() for _ in range(count)]
    entered = 0
    first_completion_observed_at = None

    def run_one(index):
        nonlocal entered, first_completion_observed_at
        entered += 1
        if entered == 16:
            for future in futures[:16]:
                future.set_result(None)
        if entered == count:
            for future in futures[16:]:
                future.set_result(None)
        futures[index].result()
        if index < 16 and first_completion_observed_at is None:
            first_completion_observed_at = entered
        return index

    results = ApiRunner(max_threads=count, poll_interval=0.001).run_jobs(
        "A",
        list(range(count)),
        run_one,
    )

    assert results == list(range(count))
    assert first_completion_observed_at is not None
    assert first_completion_observed_at <= 64


def test_terminal_registration_flushes_while_next_admission_pull_is_blocked(tmp_path):
    """A finished job must not need the node fiber scheduler to run again."""
    from micro_workflow_manager.models import DONE, Job, now
    from micro_workflow_manager.storage import FileStorage

    storage = FileStorage(tmp_path)
    storage.create_job(Job(node_name="A", job_id=1, params={}))
    generation, execution_id = storage.claim_job_executions_batch(
        "A",
        [1],
        started_at=now(),
    )[0]

    second_pull_started = threading.Event()
    release_second_pull = threading.Event()

    class BlockingRefreshableSource:
        def __init__(self):
            self.calls = 0

        def pull(self, max_items):
            self.calls += 1
            if self.calls == 1:
                return [(generation, execution_id)]
            second_pull_started.set()
            assert release_second_pull.wait(2)
            return []

    def finish(item):
        lease_generation, lease_execution_id = item
        storage.finalize_job_execution(
            "A",
            1,
            lease_generation,
            lease_execution_id,
            DONE,
            started_at=now(),
            finished_at=now(),
            duration_seconds=0.0,
            generation=lease_generation,
            execution_id=lease_execution_id,
        )
        return 1

    result = []
    error = []

    def run():
        try:
            result.extend(
                ApiRunner(max_threads=2, poll_interval=0.001).run_job_source(
                    "A",
                    BlockingRefreshableSource(),
                    finish,
                )
            )
        except BaseException as exc:  # pragma: no cover - surfaced below
            error.append(exc)

    worker = threading.Thread(target=run)
    worker.start()
    try:
        assert second_pull_started.wait(1)
        deadline = time.monotonic() + 0.5
        while time.monotonic() < deadline:
            if storage.get_job_status("A", 1) == DONE:
                break
            time.sleep(0.005)
        assert storage.get_job_status("A", 1) == DONE
    finally:
        release_second_pull.set()
        worker.join(timeout=3)

    assert not worker.is_alive()
    assert error == []
    assert result == [1]
