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
from micro_workflow_manager.runners.api import ApiRunner, _LaneCoordinator
from micro_workflow_manager.workflow.component_scheduler import allocate_api_pumps


def test_preclaimed_api_burst_records_first_task_started_in_claim_batch(tmp_path, monkeypatch):
    workflow = MicroWorkflow(tmp_path, runner="api")
    workflow.active_job_restart_enabled = True
    workflow.graph([])
    router = NodeRouter("A", runner="api", max_threads=16)

    @router.task
    def work(ctx, value, errors):
        assert errors == []
        return value

    workflow.include_router(router)
    workflow.add_jobs(None, "A", [{"value": value} for value in range(16)])

    appended_task_starts = []
    claim_observations = []
    append_job_event = workflow.storage.append_job_event
    claim_job_executions_batch = workflow.storage.claim_job_executions_batch

    def reject_separate_task_start(node_name, job_id, event, **data):
        if event == "task_started":
            appended_task_starts.append((node_name, job_id, data))
            raise AssertionError("first main task start must share its execution-claim writer")
        return append_job_event(node_name, job_id, event, **data)

    def observe_claim(node_name, job_ids, **options):
        result = claim_job_executions_batch(node_name, job_ids, **options)
        observed = {
            job_id: [
                event["event"]
                for event in workflow.storage.read_job_events(node_name, job_id)
            ]
            for job_id in job_ids
        }
        assert all(events.count("task_started") == 1 for events in observed.values())
        claim_observations.append((tuple(job_ids), observed))
        return result

    monkeypatch.setattr(workflow.storage, "append_job_event", reject_separate_task_start)
    monkeypatch.setattr(workflow.storage, "claim_job_executions_batch", observe_claim)
    workflow.run_node("A")

    assert appended_task_starts == []
    assert sorted(job_id for job_ids, _ in claim_observations for job_id in job_ids) == list(range(1, 17))
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


def test_api_runner_defaults_to_fixed_limit_adaptive_sharding(monkeypatch):
    class DenseSource:
        def remaining_hint(self):
            return 10000

    monkeypatch.delenv("MWF_API_STARTUP_STRATEGY", raising=False)
    runner = ApiRunner(max_threads=4096)
    assert runner.startup_strategy == "adaptive"
    assert runner.startup_lanes(DenseSource()) == 12
    assert ApiRunner(max_threads=63).startup_lanes(DenseSource()) == 1
    assert ApiRunner(max_threads=200).startup_lanes(DenseSource()) == 4
    assert ApiRunner(max_threads=400).startup_lanes(DenseSource()) == 7
    assert ApiRunner(max_threads=800).startup_lanes(DenseSource()) == 12
    assert ApiRunner(max_threads=1400).startup_lanes(DenseSource()) == 12
    assert ApiRunner(max_threads=10000).startup_lanes(DenseSource()) == 12


def test_sharded_live_api_pumps_survive_a_sparse_start_and_admit_later_work():
    class InitiallyEmptyPerLaneSource:
        def __init__(self):
            self.items = list(range(8))
            self.pulls_by_thread = {}
            self.lock = threading.Lock()
            self.wait_count = 0

        def pull(self, max_items):
            thread_id = threading.get_ident()
            with self.lock:
                pulls = self.pulls_by_thread.get(thread_id, 0) + 1
                self.pulls_by_thread[thread_id] = pulls
                # FiberRuntime probes twice before consulting wait_for_change.
                # Model the real Hoeflein startup where a lane begins before
                # the fan-out has published work for it.
                if pulls <= 2:
                    return []
                result = self.items[:max_items]
                del self.items[:max_items]
                return result

        def remaining_hint(self):
            with self.lock:
                return len(self.items)

        def wait_for_change(self, _timeout):
            with self.lock:
                self.wait_count += 1
                return bool(self.items)

    source = InitiallyEmptyPerLaneSource()
    results = ApiRunner(
        max_threads=8,
        poll_interval=0.001,
        startup_strategy="lanes:2",
    ).run_job_source("A", source, lambda value: value)

    assert sorted(results) == list(range(8))
    assert source.wait_count >= 1


def test_simultaneous_api_pump_vector_guarantees_one_and_uses_shared_budget():
    limits = {
        "explodeclaim": 200,
        "explodecontext": 400,
        "explodedefinition": 800,
        "explodeexample": 500,
        "explodeexercise": 1400,
        "explodeexplanation": 400,
        "explodejas": 400,
        "explodenotation": 200,
        "exploderemark": 400,
        "explodetheorem": 600,
    }

    allocations = allocate_api_pumps(limits, logical_processors=16)

    assert allocations == {
        "explodeclaim": 1,
        "explodecontext": 2,
        "explodedefinition": 3,
        "explodeexample": 2,
        "explodeexercise": 4,
        "explodeexplanation": 2,
        "explodejas": 2,
        "explodenotation": 1,
        "exploderemark": 2,
        "explodetheorem": 2,
    }
    assert sum(allocations.values()) == 21
    assert all(allocations[name] >= 1 for name in limits)


def test_api_pump_vector_never_starves_a_node_when_nodes_exceed_cpu_budget():
    limits = {f"H{index:02d}": 400 for index in range(30)}
    allocations = allocate_api_pumps(limits, logical_processors=16)

    assert allocations == {name: 1 for name in limits}
    assert sum(allocations.values()) == len(limits)


def test_later_dag_wave_remains_inside_running_nodes_global_pump_budget(tmp_path, monkeypatch):
    from micro_workflow_manager.workflow import component_scheduler

    monkeypatch.setattr(component_scheduler.os, "cpu_count", lambda: 16)
    monkeypatch.delenv("MWF_API_STARTUP_STRATEGY", raising=False)
    workflow = MicroWorkflow(tmp_path, runner="api", persist_graph=False)
    workflow.graph([("A", "C")])
    allocations: dict[str, int] = {}
    allocation_calls = []
    allocation_lock = threading.Lock()
    b_started = threading.Event()
    b_finished = threading.Event()
    release_b = threading.Event()
    c_started = threading.Event()

    def work(ctx):
        if ctx.current_node == "A":
            assert b_started.wait(20)
        elif ctx.current_node == "B":
            b_started.set()
            assert release_b.wait(20)
            b_finished.set()
        else:
            try:
                assert b_started.is_set() and not b_finished.is_set()
                c_started.set()
            finally:
                release_b.set()
        return ctx.current_node

    for name in ("A", "B", "C"):
        router = NodeRouter(name, runner="api", max_threads=1400)
        router.task(work)
        workflow.include_routers(router)
        workflow.add_job(None, name)

    run_component = workflow._run_component

    def observe_allocations(component, ignore_readiness=False, wait_deadlock_resolver=None,
                            api_pump_allocations=None, **kwargs):
        with allocation_lock:
            allocation_calls.append(tuple(sorted(component)))
            allocations.update(api_pump_allocations or {})
        return run_component(component, ignore_readiness, wait_deadlock_resolver,
                             api_pump_allocations, **kwargs)

    monkeypatch.setattr(workflow, "_run_component", observe_allocations)
    try:
        assert set(workflow.run()) == {"A", "B", "C"}
        assert c_started.is_set() and b_finished.is_set()
        assert sorted(allocation_calls) == [('A',), ('B',), ('C',)]
        assert allocations["A"] + allocations["B"] == 21
        # B's non-preemptive pumps remain charged when the later C wave starts.
        assert allocations["B"] + allocations["C"] == 21
        assert allocations["C"] >= 1
        sessions = workflow.storage.list_execution_sessions()
        assert len(sessions) == 1 and sessions[0]['status'] == 'terminal'
        assert sessions[0]['outcome'] == 'done'
        for name in ("A", "B", "C"):
            assert workflow.storage.get_job_status(name, 1) == 'done'
            assert workflow.storage.read_job_current_owner(name, 1)['session_id'] == sessions[0]['session_id']
            assert workflow.storage.read_job_control(name, 1)['active_execution_id'] is None
            assert workflow.storage.get_component_reservation((name,)) is None
    finally:
        release_b.set()
        workflow.storage.close_database_connections()


def test_adaptive_lane_shards_conserve_the_declared_concurrency_exactly():
    configured_limit = 1400
    coordinator = _LaneCoordinator(4, lambda: configured_limit)

    allocations = [coordinator.limit_for(lane) for lane in range(4)]
    assert sum(allocations) == configured_limit
    assert max(allocations) - min(allocations) <= 1

    # When a pump drains first, its share is redistributed instead of lost or
    # added. The live node limit therefore remains exactly the user's 1400.
    coordinator.unregister(3)
    remaining = [lane for lane in range(4) if lane != 3]
    allocations = [coordinator.limit_for(lane) for lane in remaining]
    assert sum(allocations) == configured_limit
    assert max(allocations) - min(allocations) <= 1


def test_execution_claim_defaults_match_terminal_priority():
    import inspect

    from micro_workflow_manager.storage.execution_claims import JobExecutionClaimStorageMixin
    from micro_workflow_manager.storage.priorities import (
        ADMISSION_PRIORITY,
        RUNTIME_CRITICAL_PRIORITY,
        TERMINAL_PRIORITY,
    )

    single = inspect.signature(JobExecutionClaimStorageMixin.claim_job_execution)
    batch = inspect.signature(JobExecutionClaimStorageMixin.claim_job_executions_batch)
    assert ADMISSION_PRIORITY == RUNTIME_CRITICAL_PRIORITY == TERMINAL_PRIORITY == 5
    assert single.parameters["priority"].default == ADMISSION_PRIORITY
    assert batch.parameters["priority"].default == ADMISSION_PRIORITY
