from __future__ import annotations

import threading
import time

import pytest

from micro_workflow_manager import MicroWorkflow, NodeRouter


def test_threaded_pull_source_never_blocks_workers_on_payload_loader(tmp_path):
    """Slow source pulls run on the feeder, never under a worker source lock."""
    workflow = MicroWorkflow(tmp_path, runner="threaded")
    workflow.graph([])

    router = NodeRouter("A", runner="threaded", max_threads=8)

    @router.task
    def run(ctx, value):
        time.sleep(0.01)
        return value

    workflow.include_router(router)
    for value in range(128):
        workflow.add_job(None, "A", value=value)

    original = workflow.storage.load_jobs_batch
    pull_threads: list[str] = []

    def slow_load(node_name, job_ids):
        pull_threads.append(threading.current_thread().name)
        time.sleep(0.04)
        return original(node_name, job_ids)

    workflow.storage.load_jobs_batch = slow_load  # type: ignore[method-assign]
    workflow.run_node("A", ignore_readiness=True)

    assert workflow.storage.job_status_counts("A")["done"] == 128
    assert pull_threads
    # The invariant is structural, not a wall-clock guess: slow payload reads
    # must execute only on bounded prefetch workers.  In particular, no
    # ``mwf-thread-*`` task worker may own the filesystem loader.  Total test
    # time is intentionally not asserted because SQLite lifecycle bookkeeping
    # varies substantially on loaded VPS/CI filesystems.
    assert all(name.startswith("mwf-job-prefetch") for name in pull_threads)
    assert not any(name.startswith("mwf-thread-") for name in pull_threads)


def test_hoeflein_late_feedback_is_consumed_after_member_queue_drains(tmp_path):
    """A component member remains live across a temporarily empty queue."""
    workflow = MicroWorkflow(tmp_path, runner="threaded")
    workflow.graph([("A", "B"), ("B", "A")])
    observed: list[tuple[str, int]] = []
    lock = threading.Lock()
    pump_calls = {"A": 0, "B": 0}
    original_run_queued = workflow.run_queued_node_jobs

    def count_pumps(node_name, *args, **kwargs):
        pump_calls[node_name] += 1
        return original_run_queued(node_name, *args, **kwargs)

    workflow.run_queued_node_jobs = count_pumps  # type: ignore[method-assign]

    a = NodeRouter("A", runner="threaded", max_threads=4)
    a.create_job(params={"depth": 0})

    @a.task
    def run_a(ctx, depth):
        with lock:
            observed.append(("A", depth))
        if depth == 0:
            ctx.node("B").add(autostart=True, depth=0)
        return depth

    b = NodeRouter("B", runner="api", max_threads=8)

    @b.task
    def run_b(ctx, depth):
        # Long enough for A's initial queue slice to drain completely.
        ctx.sleep(0.12)
        with lock:
            observed.append(("B", depth))
        ctx.node("A").add(autostart=True, depth=1)
        return depth

    workflow.include_router(a)
    workflow.include_router(b)
    workflow.run_component({"A", "B"}, ignore_readiness=True)

    assert observed.count(("A", 0)) == 1
    assert observed.count(("B", 0)) == 1
    assert observed.count(("A", 1)) == 1
    assert pump_calls == {"A": 1, "B": 1}
    assert workflow.storage.get_node_status("A") == "done"
    assert workflow.storage.get_node_status("B") == "done"


def test_empty_ordinary_member_stays_running_while_hoeflein_component_is_active(tmp_path):
    """Monitor state reports resident SCC members as running, not queued/done."""
    workflow = MicroWorkflow(tmp_path, runner="threaded")
    workflow.graph([("A", "B"), ("B", "A")])
    release = threading.Event()
    a_started = threading.Event()

    a = NodeRouter("A", runner="threaded", max_threads=2)
    a.create_job(params={"seed": 1})

    @a.task
    def run_a(ctx, seed):
        a_started.set()
        assert release.wait(1.0)
        return seed

    b = NodeRouter("B", runner="api", max_threads=4)

    @b.task
    def run_b(ctx):
        return None

    workflow.include_router(a)
    workflow.include_router(b)

    errors = []

    def run_component():
        try:
            workflow.run_component({"A", "B"}, ignore_readiness=True)
        except BaseException as error:  # pragma: no cover - surfaced below
            errors.append(error)

    thread = threading.Thread(target=run_component)
    thread.start()
    assert a_started.wait(1.0)
    deadline = time.monotonic() + 1.0
    while workflow.storage.get_node_status("B") != "running" and time.monotonic() < deadline:
        time.sleep(0.005)
    assert workflow.storage.get_node_status("B") == "running"
    assert workflow.storage.job_status_counts("B").get("queued", 0) == 0
    release.set()
    thread.join(timeout=2.0)
    assert not thread.is_alive()
    assert not errors


def test_component_scheduler_error_joins_live_member_jobs_before_failure(tmp_path):
    """A forced scheduler stop cannot leave a failed component with live jobs."""
    workflow = MicroWorkflow(tmp_path, runner="threaded")
    workflow.graph([("A", "B"), ("B", "A")])
    b_started = threading.Event()

    a = NodeRouter("A", runner="threaded", max_threads=4)
    a.create_job(params={"seed": 1})

    @a.task
    def run_a(ctx, seed):
        for index in range(40):
            ctx.node("B").add(autostart=True, index=index)
        return seed

    b = NodeRouter("B", runner="api", max_threads=40)

    @b.task
    def run_b(ctx, index):
        b_started.set()
        ctx.sleep(0.15)
        return index

    workflow.include_router(a)
    workflow.include_router(b)

    original = workflow.storage.nodes_by_job_status
    injected = {"raised": False}

    def unstable_job_observation(node_names, statuses):
        if b_started.is_set() and not injected["raised"]:
            injected["raised"] = True
            raise OSError("synthetic VPS scheduler I/O failure")
        return original(node_names, statuses)

    workflow.storage.nodes_by_job_status = unstable_job_observation  # type: ignore[method-assign]

    with pytest.raises(OSError, match="synthetic VPS"):
        workflow.run_component({"A", "B"}, ignore_readiness=True)

    assert injected["raised"]
    for node in ("A", "B"):
        counts = workflow.storage.job_status_counts(node)
        assert counts.get("running", 0) == 0, (node, counts)
        assert workflow.storage.get_node_status(node) == "failed"


def test_ordinary_hoeflein_members_are_resident_before_first_feedback(tmp_path):
    """An ordinary SCC member is live before a sibling first queues work for it."""
    workflow = MicroWorkflow(tmp_path, runner="threaded")
    workflow.graph([("A", "B"), ("B", "A")])
    b_pump_started = threading.Event()
    observed: list[bool] = []

    original_run_queued = workflow.run_queued_node_jobs

    def observed_run_queued(node_name, *args, **kwargs):
        if node_name == "B":
            b_pump_started.set()
        return original_run_queued(node_name, *args, **kwargs)

    workflow.run_queued_node_jobs = observed_run_queued  # type: ignore[method-assign]

    a = NodeRouter("A", runner="threaded", max_threads=2)
    a.create_job(params={"seed": 1})

    @a.task
    def run_a(ctx, seed):
        # 0.5.2 started B only after B had durable queued work. A real Hoeflein
        # component keeps the ordinary B pump resident even while B is empty.
        observed.append(b_pump_started.wait(0.20))
        ctx.node("B").add(autostart=True, value=seed)
        return seed

    b = NodeRouter("B", runner="api", max_threads=4)

    @b.task
    def run_b(ctx, value):
        return value

    workflow.include_router(a)
    workflow.include_router(b)
    workflow.run_component({"A", "B"}, ignore_readiness=True)

    assert observed == [True]
    assert workflow.storage.job_status_counts("B").get("done", 0) == 1


def test_live_consumer_subscribes_before_sibling_handler_is_released(tmp_path):
    workflow = MicroWorkflow(tmp_path, runner="threaded")
    workflow.graph([("producer", "consumer"), ("consumer", "producer")])
    consumer_subscribed = threading.Event()
    producer_observed: list[bool] = []

    original_subscribe = workflow.storage.subscribe_queue_changes

    def observed_subscribe(callback, *, node_name=None):
        unsubscribe = original_subscribe(callback, node_name=node_name)
        if node_name == "consumer":
            consumer_subscribed.set()
        return unsubscribe

    workflow.storage.subscribe_queue_changes = observed_subscribe  # type: ignore[method-assign]

    producer = NodeRouter("producer", runner="threaded", max_threads=1)
    producer.create_job(params={"value": 1})

    @producer.task
    def run_producer(ctx, value):
        producer_observed.append(consumer_subscribed.is_set())
        ctx.node("consumer").add(value=value)
        return value

    consumer = NodeRouter("consumer", runner="api", max_threads=4)

    @consumer.task
    def run_consumer(ctx, value):
        return value

    workflow.include_router(producer)
    workflow.include_router(consumer)
    workflow.run_component({"producer", "consumer"}, ignore_readiness=True)

    assert producer_observed == [True]
    assert workflow.storage.job_status_counts("consumer").get("done", 0) == 1


def test_threaded_payload_loader_oserror_propagates_and_component_joins(tmp_path):
    """EMFILE in a threaded source is never converted into a phantom job."""
    workflow = MicroWorkflow(tmp_path, runner="threaded")
    workflow.graph([("A", "B"), ("B", "A")])

    a = NodeRouter("A", runner="threaded", max_threads=8)
    a.create_job(params={"value": 1})

    @a.task
    def run_a(ctx, value):
        return value

    b = NodeRouter("B", runner="api", max_threads=8)

    @b.task
    def run_b(ctx, value):
        ctx.sleep(0.05)
        return value

    workflow.include_router(a)
    workflow.include_router(b)

    original = workflow.storage.load_jobs_batch
    raised = {"value": False}

    def failing_load(node_name, job_ids):
        if node_name == "A" and not raised["value"]:
            raised["value"] = True
            raise OSError(24, "Too many open files")
        return original(node_name, job_ids)

    workflow.storage.load_jobs_batch = failing_load  # type: ignore[method-assign]

    with pytest.raises(OSError) as caught:
        workflow.run_component({"A", "B"}, ignore_readiness=True)

    assert caught.value.errno == 24
    assert raised["value"]
    for node_name in ("A", "B"):
        counts = workflow.storage.job_status_counts(node_name)
        assert counts.get("running", 0) == 0
        assert workflow.storage.get_node_status(node_name) == "failed"


def test_failed_component_cleanup_never_leaves_stale_running_rows(tmp_path):
    """Terminal SCC failure converts abandoned RUNNING leases into retryable failures."""
    workflow = MicroWorkflow(tmp_path, runner="threaded")
    workflow.graph([("A", "B"), ("B", "A")])

    a = NodeRouter("A", runner="threaded")
    b = NodeRouter("B", runner="api")

    @a.task
    def run_a(ctx):
        return None

    @b.task
    def run_b(ctx):
        return None

    workflow.include_router(a)
    workflow.include_router(b)
    job = workflow.add_job(None, "B")
    workflow.storage.set_job_status("B", job.job_id, "running", synthetic=True)

    workflow._finalize_failed_component({"A", "B"}, OSError(24, "Too many open files"))

    counts = workflow.storage.job_status_counts("B")
    assert counts.get("running", 0) == 0
    assert counts.get("failed", 0) == 1
    status = workflow.storage.read_job_status_data("B", job.job_id)
    assert status["recovered_after_component_abort"] is True
    assert workflow.storage.get_node_status("A") == "failed"
    assert workflow.storage.get_node_status("B") == "failed"


def test_node_scoped_queue_wakeup_does_not_wake_unrelated_live_members(tmp_path):
    workflow = MicroWorkflow(tmp_path, runner="threaded")
    workflow.graph([("A", "B"), ("B", "A"), ("A", "C"), ("C", "A")])
    hits = {"B": 0, "C": 0}

    unsubscribe_b = workflow.storage.subscribe_queue_changes(
        lambda: hits.__setitem__("B", hits["B"] + 1), node_name="B"
    )
    unsubscribe_c = workflow.storage.subscribe_queue_changes(
        lambda: hits.__setitem__("C", hits["C"] + 1), node_name="C"
    )
    try:
        workflow.storage.notify_queue_change("B")
        assert hits == {"B": 1, "C": 0}
        workflow.storage.notify_queue_change("C")
        assert hits == {"B": 1, "C": 1}
    finally:
        unsubscribe_b()
        unsubscribe_c()


def test_node_status_writes_share_the_single_sqlite_mutation_lane(tmp_path):
    workflow = MicroWorkflow(tmp_path, runner="threaded")
    workflow.graph([])
    calls = []
    original = workflow.storage.submit_db_mutation

    def observed(operation, **kwargs):
        calls.append(kwargs.get("priority"))
        return original(operation, **kwargs)

    workflow.storage.submit_db_mutation = observed  # type: ignore[method-assign]
    workflow.storage.set_node_status("A", "queued")
    workflow.storage.set_node_statuses({"A": "running", "B": "queued"})
    assert calls == [5, 5]
