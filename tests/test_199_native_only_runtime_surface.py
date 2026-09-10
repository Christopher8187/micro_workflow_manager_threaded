from __future__ import annotations

import time

import httpx
import pytest

from micro_workflow_manager import MicroWorkflow, NodeRouter
from micro_workflow_manager.runners.api import ApiRunner
from micro_workflow_manager.network.manager import NetworkManager


def _close(storage):
    storage.db_mutation_barrier()
    deadline = time.perf_counter() + 10
    while storage.mutation_writer_diagnostics()["writer_alive"]:
        assert time.perf_counter() < deadline, "Mutation writer did not retire"
        time.sleep(0.01)
    storage.close_database_connections()


@pytest.mark.parametrize("strategy", ["serial", "legacy"])
@pytest.mark.parametrize("entry", ["explicit", "environment"])
def test_obsolete_api_startup_strategy_refuses_before_handler_entry(
    monkeypatch, strategy, entry,
):
    monkeypatch.delenv("MWF_API_STARTUP_STRATEGY", raising=False)
    options = {}
    if entry == "environment":
        monkeypatch.setenv("MWF_API_STARTUP_STRATEGY", strategy)
    else:
        options["startup_strategy"] = strategy
    runner = ApiRunner(max_threads=4, **options)
    entered = []

    with pytest.raises(ValueError) as failure:
        runner.run_jobs("A", [1], lambda job: entered.append(job))

    assert str(failure.value) == (
        "API startup strategy must be elastic, balanced, event, latency, "
        "adaptive, single, or lanes:<count>"
    )
    assert entered == []


@pytest.mark.parametrize(
    "strategy,limit,lanes",
    [("single", 4, 1), ("event", 4, 1), ("latency", 4, 1),
     ("lanes:2", 4, 2), ("lanes:8", 2, 2)],
)
def test_current_api_startup_strategy_keeps_lanes_and_job_results(
    strategy, limit, lanes,
):
    runner = ApiRunner(max_threads=limit, startup_strategy=strategy)

    assert runner.startup_lanes() == lanes
    assert sorted(runner.run_jobs("A", [1, 2, 3], lambda job: job * 10)) == [10, 20, 30]


@pytest.mark.parametrize(
    "strategy,startup_windows,completion_batch",
    [
        ("single", "1", "16"),
        ("event", "1", "12"),
        ("latency", "1", "8"),
        ("balanced", "auto:1-2", "12"),
        ("elastic", "auto:1-4", "16"),
        ("adaptive", "auto:1-12", "16"),
        ("lanes:2", "2", "16"),
    ],
)
def test_current_api_strategy_is_recorded_in_native_execution_session(
    tmp_path, monkeypatch, strategy, startup_windows, completion_batch,
):
    monkeypatch.setenv("MWF_API_STARTUP_STRATEGY", strategy)
    workflow = MicroWorkflow(tmp_path, runner="api", persist_graph=False)
    try:
        workflow.graph([("A", "B")])
        router = NodeRouter("A", runner="api", max_threads=2)

        @router.task
        def work(ctx):
            return 42

        workflow.include_routers(router)
        job = workflow.add_job(None, "A")
        assert workflow.run_job("A", job.job_id) == 42

        sessions = workflow.storage.list_execution_sessions()
        assert len(sessions) == 1
        session = sessions[0]
        assert session["status"] == "terminal"
        assert session["outcome"] == "done"
        assert session["details"]["api_startup_strategy"] == strategy
        assert session["details"]["api_startup_windows"] == startup_windows
        assert session["details"]["api_completion_service_batch"] == completion_batch
        assert workflow.storage.get_execution_session(session["session_id"]) == session
    finally:
        _close(workflow.storage)


@pytest.mark.parametrize("architecture", ["legacy", "central"])
@pytest.mark.parametrize("entry", ["explicit", "environment"])
def test_obsolete_network_architecture_refuses_without_changing_configuration(
    monkeypatch, architecture, entry,
):
    monkeypatch.delenv("MWF_NETWORK_ARCHITECTURE", raising=False)
    options = {}
    if entry == "environment":
        monkeypatch.setenv("MWF_NETWORK_ARCHITECTURE", architecture)
    else:
        options["architecture"] = architecture
    manager = NetworkManager()
    before = manager.snapshot()
    try:
        with pytest.raises(ValueError) as failure:
            manager.configure(
                transport=httpx.MockTransport(
                    lambda request: httpx.Response(200, request=request)
                ),
                **options,
            )

        assert str(failure.value) == (
            "network architecture must be 'manager' or 'direct'"
        )
        assert manager.snapshot() == before
    finally:
        manager.close()


@pytest.mark.parametrize(
    "architecture,entry",
    [
        ("manager", "explicit"),
        ("direct", "explicit"),
        ("manager", "environment"),
        ("direct", "environment"),
        ("manager", "default"),
    ],
)
def test_current_network_architecture_keeps_configuration_and_request_results(
    monkeypatch, architecture, entry,
):
    monkeypatch.delenv("MWF_NETWORK_ARCHITECTURE", raising=False)
    options = {}
    if entry == "environment":
        monkeypatch.setenv("MWF_NETWORK_ARCHITECTURE", architecture)
    elif entry == "explicit":
        options["architecture"] = architecture
    requests = []

    def handler(request):
        requests.append((request.method, str(request.url)))
        return httpx.Response(200, json={"value": 42}, request=request)

    manager = NetworkManager()
    try:
        manager.configure(transport=httpx.MockTransport(handler), **options)
        assert manager.snapshot()["architecture"] == architecture

        response = manager.submit_request(
            "GET", "https://example.test/native-mode", expect_json=True,
        ).result(timeout=10)

        assert response.status_code == 200
        assert response.json() == {"value": 42}
        assert requests == [("GET", "https://example.test/native-mode")]
        snapshot = manager.snapshot()
        assert snapshot["architecture"] == architecture
        assert snapshot["requests_enqueued"] == 1
    finally:
        manager.close()
