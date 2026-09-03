import asyncio
import gzip
import json
import socket
import ssl
import threading
import time

import httpx
import pytest

from benchmarks.local_http_delay_server import H2Session, h1_transfer
from micro_workflow_manager import MicroWorkflow, NodeRouter
from micro_workflow_manager.models import Job
from micro_workflow_manager.networking import (
    close_shared_http_transport,
    configure_shared_http_transport,
    shared_http_transport,
)
from micro_workflow_manager.runners.api import ApiRunner
from micro_workflow_manager.network.manager import NetworkManager
from micro_workflow_manager.network.types import CohortStreamStall


def test_network_manager_reuses_one_default_ssl_context_for_every_client_shard(monkeypatch):
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    calls = []

    def build_context(*, verify, cert, trust_env):
        calls.append((verify, cert, trust_env))
        return context

    monkeypatch.setattr(httpx, "create_ssl_context", build_context)
    manager = NetworkManager()
    manager.configure(http2=True, streams_per_connection=32)
    first = manager._new_client_shard()
    second = manager._new_client_shard()
    try:
        assert manager._client_kwargs["verify"] is context
        assert calls == [(True, None, True)]
    finally:
        asyncio.run(first.client.aclose())
        asyncio.run(second.client.aclose())


def test_http_clients_enable_fast_tcp_keepalive_without_changing_request_timeout():
    manager = NetworkManager()
    manager.configure(http2=True, streams_per_connection=32)
    shard = manager._new_client_shard()
    try:
        options = shard.client._transport._pool._socket_options
        assert (socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1) in options
        expected = {
            name: value
            for name, value in (
                ("TCP_KEEPIDLE", 30),
                ("TCP_KEEPINTVL", 10),
                ("TCP_KEEPCNT", 3),
            )
            if hasattr(socket, name)
        }
        for name, value in expected.items():
            assert (socket.IPPROTO_TCP, getattr(socket, name), value) in options
        snapshot = manager.snapshot()
        assert snapshot["tcp_keepalive"] is True
        assert snapshot["tcp_keepalive_idle_seconds"] == 30
        assert snapshot["tcp_keepalive_interval_seconds"] == 10
        assert snapshot["tcp_keepalive_probes"] == 3
    finally:
        asyncio.run(shard.client.aclose())


def test_available_http2_shards_pack_work_before_using_idle_connection():
    manager = NetworkManager()
    manager.configure(
        http2=True,
        streams_per_connection=4,
        transport=httpx.MockTransport(lambda request: httpx.Response(200, request=request)),
    )
    first = manager._new_client_shard()
    second = manager._new_client_shard()

    async def acquire_four():
        return [await manager._acquire_client() for _ in range(4)]

    selected = asyncio.run(acquire_four())
    try:
        assert [shard.shard_id for shard in selected] == [1, 1, 1, 1]
        assert first.in_flight == 4
        assert second.in_flight == 0
    finally:
        asyncio.run(first.client.aclose())
        asyncio.run(second.client.aclose())


def test_complete_json_recovers_from_missing_stream_terminal(monkeypatch):
    monkeypatch.setenv("MWF_JSON_TERMINAL_GRACE_SECONDS", "0.02")
    stream_closed = threading.Event()

    class MissingTerminal(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield b'{"ok":true}'
            await asyncio.Event().wait()

        async def aclose(self):
            stream_closed.set()

    async def handler(request):
        return httpx.Response(
            200,
            headers={"content-type": "application/json"},
            stream=MissingTerminal(),
            request=request,
        )

    manager = NetworkManager()
    manager.configure(
        transport=httpx.MockTransport(handler),
        streams_per_connection=4,
    )
    future = manager.submit_request(
        "POST", "https://example.test/", expect_json=True
    )
    try:
        response = future.result(timeout=2)
        snapshot = manager.snapshot()
        assert response.json() == {"ok": True}
        assert stream_closed.wait(1)
        assert snapshot["json_stream_recoveries"] == 1
        assert snapshot["retired_shards"] == 1
        assert snapshot["client_count"] == 0
        assert snapshot["json_terminal_grace_seconds"] == 0.02
    finally:
        manager.close()


def test_normally_terminated_json_keeps_connection_reusable(monkeypatch):
    monkeypatch.setenv("MWF_JSON_TERMINAL_GRACE_SECONDS", "0.02")

    class NormalTerminal(httpx.AsyncByteStream):
        async def __aiter__(self):
            encoded = gzip.compress(b'{"ok":true}')
            yield encoded[: len(encoded) // 2]
            yield encoded[len(encoded) // 2 :]

    async def handler(request):
        return httpx.Response(
            200,
            headers={
                "content-type": "application/json",
                "content-encoding": "gzip",
            },
            stream=NormalTerminal(),
            request=request,
        )

    manager = NetworkManager()
    manager.configure(
        transport=httpx.MockTransport(handler),
        streams_per_connection=4,
    )
    future = manager.submit_request(
        "POST", "https://example.test/", expect_json=True
    )
    try:
        assert future.result(timeout=2).json() == {"ok": True}
        snapshot = manager.snapshot()
        assert snapshot["json_stream_recoveries"] == 0
        assert snapshot["retired_shards"] == 0
        assert snapshot["client_count"] == 1
    finally:
        manager.close()


def test_expect_json_accepts_already_buffered_custom_transport_response():
    manager = NetworkManager()
    manager.configure(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, json={"ok": True}, request=request)
        ),
        streams_per_connection=4,
    )
    future = manager.submit_request(
        "POST", "https://example.test/", expect_json=True
    )
    try:
        assert future.result(timeout=2).json() == {"ok": True}
        snapshot = manager.snapshot()
        assert snapshot["json_stream_recoveries"] == 0
        assert snapshot["retired_shards"] == 0
    finally:
        manager.close()


def test_json_response_exposes_openrouter_generation_id_in_diagnostics():
    release = threading.Event()

    class DelayedJSON(httpx.AsyncByteStream):
        async def __aiter__(self):
            while not release.is_set():
                await asyncio.sleep(0.005)
            yield b'{"ok":true}'

    async def handler(request):
        return httpx.Response(
            200,
            headers={
                "content-type": "application/json",
                "x-generation-id": "gen-test-123",
            },
            stream=DelayedJSON(),
            request=request,
        )

    manager = NetworkManager()
    manager.configure(
        transport=httpx.MockTransport(handler),
        streams_per_connection=4,
    )
    future = manager.submit_request(
        "POST",
        "https://example.test/model",
        expect_json=True,
        node_name="A",
        job_id=7,
    )
    try:
        deadline = time.monotonic() + 2
        snapshot = manager.snapshot()
        while (
            not snapshot["shards"]
            or snapshot["shards"][0]["oldest_generation_id"] != "gen-test-123"
        ):
            if time.monotonic() >= deadline:
                pytest.fail("generation id did not become observable")
            time.sleep(0.01)
            snapshot = manager.snapshot()
        release.set()
        response = future.result(timeout=2)
        assert response.extensions["mwf_generation_id"] == "gen-test-123"
    finally:
        release.set()
        manager.close()


def test_cohort_stalled_stream_retries_on_fresh_shard_without_failing_job(monkeypatch):
    monkeypatch.setenv("MWF_HTTP2_COHORT_STALL_SECONDS", "0.05")
    monkeypatch.setenv("MWF_HTTP2_COHORT_TERMINALS", "2")
    monkeypatch.setenv("MWF_HTTP2_COHORT_RETRIES", "1")
    first_attempt_started = threading.Event()
    stalled_calls = 0

    class NeverResponds(httpx.AsyncByteStream):
        async def __aiter__(self):
            first_attempt_started.set()
            await asyncio.Event().wait()
            yield b""  # pragma: no cover

    class HealthyJSON(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield b'{"ok":true}'

    async def handler(request):
        nonlocal stalled_calls
        if request.url.path == "/stalled":
            stalled_calls += 1
            stream = NeverResponds() if stalled_calls == 1 else HealthyJSON()
            return httpx.Response(
                200,
                headers={"content-type": "application/json"},
                stream=stream,
                request=request,
            )
        return httpx.Response(200, content=b"ok", request=request)

    manager = NetworkManager()
    manager.configure(
        transport=httpx.MockTransport(handler),
        streams_per_connection=8,
    )
    stalled = manager.submit_request(
        "POST", "https://example.test/stalled", expect_json=True
    )
    try:
        assert first_attempt_started.wait(1)
        siblings = [
            manager.submit_request("GET", f"https://example.test/fast/{index}")
            for index in range(4)
        ]
        assert [item.result(timeout=2).status_code for item in siblings] == [200] * 4
        assert stalled.result(timeout=2).json() == {"ok": True}
        snapshot = manager.snapshot()
        assert stalled_calls == 2
        assert snapshot["cohort_stream_retries"] == 1
        assert snapshot["retired_shards"] == 1
        assert snapshot["client_count"] == 1
        assert snapshot["shards"][0]["requests_failed"] == 0
    finally:
        manager.close()


def test_mass_cohort_recovery_shares_replacement_shards_without_limiting_requests(
    monkeypatch,
):
    """A recovery wave must not allocate one TLS client per stalled stream."""
    monkeypatch.setenv("MWF_HTTP2_COHORT_RETRIES", "1")
    request_count = 64
    manager = NetworkManager()
    manager.configure(
        http2=True,
        streams_per_connection=8,
        http2_stream_safety_cap=8,
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, content=b"ok", request=request)
        ),
    )
    first_attempt_count = 0
    all_first_attempts_acquired = asyncio.Event()
    retry_active = 0
    retry_peak = 0
    all_retries_active = asyncio.Event()

    async def synthetic_request(request, shard, kwargs, active):
        nonlocal first_attempt_count, retry_active, retry_peak
        if active["cohort_retries"] == 0:
            first_attempt_count += 1
            if first_attempt_count == request_count:
                all_first_attempts_acquired.set()
            await all_first_attempts_acquired.wait()
            raise CohortStreamStall("synthetic poisoned cohort")

        retry_active += 1
        retry_peak = max(retry_peak, retry_active)
        if retry_active == request_count:
            all_retries_active.set()
        try:
            await asyncio.wait_for(all_retries_active.wait(), timeout=2)
            return httpx.Response(
                200,
                content=b"ok",
                request=httpx.Request(request.method, request.url),
            )
        finally:
            retry_active -= 1

    monkeypatch.setattr(manager, "_request_with_progress", synthetic_request)
    futures = [
        manager.submit_request("GET", f"https://example.test/{index}")
        for index in range(request_count)
    ]
    try:
        assert [future.result(timeout=5).status_code for future in futures] == [
            200
        ] * request_count
        snapshot = manager.snapshot()
        assert retry_peak == request_count
        assert snapshot["cohort_stream_retries"] == request_count
        assert snapshot["retired_shards"] == request_count // 8
        assert snapshot["client_count"] == request_count // 8
        assert max(snapshot["peak_in_flight_per_client"]) == 8
        assert manager._next_shard_id == 2 * (request_count // 8) + 1
        assert snapshot["recovery_shards_created"] == request_count // 8
        assert snapshot["recovery_shard_reuses"] == (
            request_count - request_count // 8
        )
        assert "active_request_limit" not in snapshot
    finally:
        manager.close()


def test_cohort_recovery_reuses_an_existing_healthy_shard(monkeypatch):
    monkeypatch.setenv("MWF_HTTP2_COHORT_RETRIES", "1")
    manager = NetworkManager()
    manager.configure(
        http2=True,
        streams_per_connection=4,
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, content=b"ok", request=request)
        ),
    )
    healthy = manager._new_client_shard()
    poisoned = manager._new_client_shard()
    manager._next_client_index = 1
    selected_shards = []

    async def fail_poisoned_once(request, shard, kwargs, active):
        selected_shards.append(shard.shard_id)
        if active["cohort_retries"] == 0:
            raise CohortStreamStall("synthetic poisoned cohort")
        return httpx.Response(
            200,
            content=b"ok",
            request=httpx.Request(request.method, request.url),
        )

    monkeypatch.setattr(manager, "_request_with_progress", fail_poisoned_once)
    future = manager.submit_request("GET", "https://example.test/")
    try:
        assert future.result(timeout=2).status_code == 200
        snapshot = manager.snapshot()
        assert selected_shards == [poisoned.shard_id, healthy.shard_id]
        assert manager._next_shard_id == 3
        assert snapshot["retired_shards"] == 1
        assert snapshot["client_count"] == 1
        assert snapshot["recovery_shard_reuses"] == 1
        assert snapshot["recovery_shards_created"] == 0
    finally:
        manager.close()


def test_client_selection_packs_quiet_tail_onto_busiest_healthy_shard():
    manager = NetworkManager()
    manager.configure(
        http2=True,
        streams_per_connection=8,
        http2_stream_safety_cap=8,
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, content=b"ok", request=request)
        ),
    )
    sparse = manager._new_client_shard()
    busiest = manager._new_client_shard()
    idle = manager._new_client_shard()
    sparse.in_flight = 1
    busiest.in_flight = 5

    try:
        selected = asyncio.run(manager._acquire_client())
        assert selected is busiest
        assert busiest.in_flight == 6
        assert sparse.in_flight == 1
        assert idle.in_flight == 0
    finally:
        manager.close()


def test_quiet_tail_cohort_uses_all_available_same_shard_peers():
    manager = NetworkManager()
    manager._cohort_stall_seconds = 300.0
    manager._cohort_terminal_evidence = 16
    shard = type(
        "Shard",
        (),
        {
            "in_flight": 1,
            "requests_completed": 5,
            "requests_failed": 0,
            "last_terminal_at": 250.0,
            "shard_id": 7,
        },
    )()
    active = {
        "attempt_started_at": 0.0,
        "cohort_terminal_baseline": 0,
    }

    reason = manager._cohort_stream_stall_reason(active, shard, 301.0)

    assert reason is not None
    assert "5 newer sibling requests terminated" in reason


def test_cohort_detection_accepts_newer_sibling_completion_in_same_clock_tick():
    manager = NetworkManager()
    manager._cohort_stall_seconds = 0.05
    manager._cohort_terminal_evidence = 2
    shard = type(
        "Shard",
        (),
        {
            "in_flight": 1,
            "requests_completed": 2,
            "requests_failed": 0,
            "last_terminal_at": 100.0,
            "shard_id": 7,
        },
    )()
    active = {
        "attempt_started_at": 100.0,
        "cohort_terminal_baseline": 0,
    }

    reason = manager._cohort_stream_stall_reason(active, shard, 100.1)

    assert reason is not None
    assert "2 newer sibling requests terminated" in reason


def test_single_tail_peer_is_not_enough_for_early_cohort_replay():
    manager = NetworkManager()
    manager._cohort_stall_seconds = 300.0
    manager._cohort_terminal_evidence = 16
    shard = type(
        "Shard",
        (),
        {
            "in_flight": 1,
            "requests_completed": 1,
            "requests_failed": 0,
            "last_terminal_at": 250.0,
            "shard_id": 7,
        },
    )()
    active = {
        "attempt_started_at": 0.0,
        "cohort_terminal_baseline": 0,
    }

    assert manager._cohort_stream_stall_reason(active, shard, 301.0) is None


def test_silent_json_stream_replays_at_existing_read_timeout(monkeypatch):
    monkeypatch.setenv("MWF_HTTP_TRANSPORT_RETRIES", "1")
    calls = 0
    attempts = []

    class NeverResponds(httpx.AsyncByteStream):
        async def __aiter__(self):
            await asyncio.Event().wait()
            yield b""

    async def handler(request):
        nonlocal calls
        calls += 1
        stream = (
            NeverResponds()
            if calls == 1
            else httpx.ByteStream(b'{"ok":true}')
        )
        return httpx.Response(
            200,
            headers={"content-type": "application/json"},
            stream=stream,
            request=request,
        )

    manager = NetworkManager()
    manager.configure(
        http2=True,
        streams_per_connection=4,
        transport=httpx.MockTransport(handler),
    )
    future = manager.submit_request(
        "POST",
        "https://example.test/model",
        timeout=httpx.Timeout(0.05),
        expect_json=True,
        attempt_callback=lambda attempt, reason: attempts.append(
            (attempt, reason)
        ),
    )
    try:
        assert future.result(timeout=2).json() == {"ok": True}
        snapshot = manager.snapshot()
        assert calls == 2
        assert attempts == [(1, None), (2, "transport_error")]
        assert snapshot["transport_error_retries"] == 1
    finally:
        manager.close()


def test_network_manager_reports_each_physical_replay_for_lease_renewal(monkeypatch):
    """A hidden transport replay must be visible to the scheduler lease owner."""
    monkeypatch.setenv("MWF_HTTP2_COHORT_RETRIES", "1")
    manager = NetworkManager()
    manager.configure(
        http2=True,
        streams_per_connection=4,
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, content=b"ok", request=request)
        ),
    )
    observed_attempts = []

    async def fail_once(request, shard, kwargs, active):
        if active["cohort_retries"] == 0:
            raise CohortStreamStall("synthetic poisoned cohort")
        return httpx.Response(
            200,
            content=b"ok",
            request=httpx.Request(request.method, request.url),
        )

    monkeypatch.setattr(manager, "_request_with_progress", fail_once)
    future = manager.submit_request(
        "GET",
        "https://example.test/",
        attempt_callback=lambda attempt, reason: observed_attempts.append(
            (attempt, reason)
        ),
    )
    try:
        assert future.result(timeout=2).status_code == 200
        assert observed_attempts == [
            (1, None),
            (2, "cohort_stream_stall"),
        ]
    finally:
        manager.close()


def test_connection_protocol_error_retires_and_retries_on_shared_pool(monkeypatch):
    monkeypatch.setenv("MWF_HTTP_TRANSPORT_RETRIES", "1")
    manager = NetworkManager()
    manager.configure(
        http2=True,
        streams_per_connection=8,
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, content=b"ok", request=request)
        ),
    )
    attempts = 0

    async def fail_once(request, shard, kwargs, active):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise httpx.RemoteProtocolError("poisoned connection")
        return httpx.Response(
            200,
            content=b"ok",
            request=httpx.Request(request.method, request.url),
        )

    monkeypatch.setattr(manager, "_request_with_progress", fail_once)
    future = manager.submit_request("GET", "https://example.test/")
    try:
        assert future.result(timeout=2).status_code == 200
        snapshot = manager.snapshot()
        assert attempts == 2
        assert snapshot["transport_error_retries"] == 1
        assert snapshot["retired_shards"] == 1
        assert snapshot["client_count"] == 1
        assert snapshot["shards"][0]["requests_failed"] == 0
    finally:
        manager.close()


def test_connection_error_retires_and_retries_on_shared_pool(monkeypatch):
    monkeypatch.setenv("MWF_HTTP_TRANSPORT_RETRIES", "1")
    manager = NetworkManager()
    manager.configure(
        http2=True,
        streams_per_connection=8,
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, content=b"ok", request=request)
        ),
    )
    attempts = 0

    async def fail_once(request, shard, kwargs, active):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise httpx.ConnectError("route unavailable")
        return httpx.Response(
            200,
            content=b"ok",
            request=httpx.Request(request.method, request.url),
        )

    monkeypatch.setattr(manager, "_request_with_progress", fail_once)
    future = manager.submit_request("GET", "https://example.test/")
    try:
        assert future.result(timeout=2).status_code == 200
        snapshot = manager.snapshot()
        assert attempts == 2
        assert snapshot["transport_error_retries"] == 1
        assert snapshot["retired_shards"] == 1
        assert snapshot["client_count"] == 1
    finally:
        manager.close()


def test_cohort_stall_uses_nonterminal_age_despite_irrelevant_socket_progress(monkeypatch):
    """Sibling HTTP/2 frames must not make an old stream look healthy.

    A read on one multiplexed stream can keep waking as the connection receives
    frames for other streams.  The poisoned stream is identified by remaining
    nonterminal while its newer same-shard cohort finishes, not by a quiet read
    iterator.
    """
    monkeypatch.setenv("MWF_HTTP2_COHORT_STALL_SECONDS", "0.05")
    monkeypatch.setenv("MWF_HTTP2_COHORT_TERMINALS", "2")
    monkeypatch.setenv("MWF_HTTP2_COHORT_RETRIES", "1")
    first_attempt_started = threading.Event()
    stalled_calls = 0

    class IrrelevantProgress(httpx.AsyncByteStream):
        async def __aiter__(self):
            first_attempt_started.set()
            while True:
                await asyncio.sleep(0.005)
                yield b" "

    class HealthyJSON(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield b'{"ok":true}'

    async def handler(request):
        nonlocal stalled_calls
        if request.url.path == "/stalled":
            stalled_calls += 1
            stream = IrrelevantProgress() if stalled_calls == 1 else HealthyJSON()
            return httpx.Response(
                200,
                headers={"content-type": "application/json"},
                stream=stream,
                request=request,
            )
        return httpx.Response(200, content=b"ok", request=request)

    manager = NetworkManager()
    manager.configure(
        transport=httpx.MockTransport(handler),
        streams_per_connection=8,
    )
    stalled = manager.submit_request(
        "POST", "https://example.test/stalled", expect_json=True
    )
    try:
        assert first_attempt_started.wait(1)
        siblings = [
            manager.submit_request("GET", f"https://example.test/fast/{index}")
            for index in range(4)
        ]
        assert [item.result(timeout=2).status_code for item in siblings] == [200] * 4
        assert stalled.result(timeout=2).json() == {"ok": True}
        snapshot = manager.snapshot()
        assert stalled_calls == 2
        assert snapshot["cohort_stream_retries"] == 1
        assert snapshot["retired_shards"] == 1
        assert snapshot["client_count"] == 1
        assert snapshot["shards"][0]["requests_failed"] == 0
    finally:
        manager.close()


def test_cohort_evidence_does_not_expire_during_quiet_workflow_tail():
    manager = NetworkManager()
    manager.configure(
        http2=True,
        streams_per_connection=8,
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, request=request)
        ),
    )
    shard = manager._new_client_shard()
    shard.requests_completed = 16
    shard.last_terminal_at = 110.0
    active = {
        "attempt_started_at": 100.0,
        "cohort_terminal_baseline": 0,
    }
    try:
        reason = manager._cohort_stream_stall_reason(active, shard, 401.0)
        assert reason is not None
        assert "16 newer sibling requests terminated" in reason
    finally:
        asyncio.run(shard.client.aclose())


def test_http2_stream_width_is_safely_capped_and_observable(monkeypatch):
    monkeypatch.delenv("MWF_HTTP2_STREAM_SAFETY_CAP", raising=False)
    close_shared_http_transport()
    configure_shared_http_transport(http2=True, streams_per_connection=80)
    try:
        snapshot = shared_http_transport.snapshot()
    finally:
        close_shared_http_transport()

    assert snapshot["requested_streams_per_connection"] == 80
    assert snapshot["streams_per_connection"] == 32
    assert snapshot["shard_capacity"] == 32
    assert snapshot["http2_stream_safety_cap"] == 32
    assert "active_request_limit" not in snapshot


def test_http2_stream_safety_cap_is_explicitly_overridable(monkeypatch):
    monkeypatch.setenv("MWF_HTTP2_STREAM_SAFETY_CAP", "80")
    close_shared_http_transport()
    configure_shared_http_transport(http2=True, streams_per_connection=80)
    try:
        snapshot = shared_http_transport.snapshot()
    finally:
        close_shared_http_transport()

    assert snapshot["requested_streams_per_connection"] == 80
    assert snapshot["streams_per_connection"] == 80
    assert snapshot["http2_stream_safety_cap"] == 80


def test_network_manager_dispatches_all_admitted_requests_without_a_global_gate():
    lock = threading.Lock()
    active = 0
    peak = 0
    request_count = 24
    all_dispatched = asyncio.Event()

    async def handler(request):
        nonlocal active, peak
        with lock:
            active += 1
            peak = max(peak, active)
            if active == request_count:
                all_dispatched.set()
        try:
            await asyncio.wait_for(all_dispatched.wait(), timeout=2)
            return httpx.Response(200, content=b"ok", request=request)
        finally:
            with lock:
                active -= 1

    close_shared_http_transport()
    configure_shared_http_transport(
        transport=httpx.MockTransport(handler),
        streams_per_connection=3,
        architecture="manager",
    )
    try:
        results = ApiRunner(max_threads=request_count, poll_interval=0.001).run_jobs(
            "A",
            list(range(request_count)),
            lambda _index: shared_http_transport.request(
                "GET", "https://example.test/", timeout=2
            ).status_code,
        )
        snapshot = shared_http_transport.snapshot()
    finally:
        close_shared_http_transport()

    assert results == [200] * request_count
    assert peak == request_count
    assert "active_request_limit" not in snapshot
    assert snapshot["client_count"] == 8


def test_network_manager_is_default_and_coalesces_ingress_wakeups():
    async def handler(request):
        await asyncio.sleep(0.01)
        return httpx.Response(200, content=b"ok", request=request)

    close_shared_http_transport()
    configure_shared_http_transport(
        transport=httpx.MockTransport(handler),
        streams_per_connection=16,
        architecture="manager",
    )
    try:
        results = ApiRunner(max_threads=64, poll_interval=0.001).run_jobs(
            "A",
            list(range(128)),
            lambda _index: shared_http_transport.request(
                "GET", "https://example.test/", timeout=2
            ).status_code,
        )
        snapshot = shared_http_transport.snapshot()
    finally:
        close_shared_http_transport()

    assert results == [200] * 128
    assert snapshot["architecture"] == "manager"
    assert snapshot["requests_enqueued"] == 128
    assert 0 < snapshot["ingress_wakeups"] <= 128


def test_cancelling_running_network_future_aborts_underlying_http_task():
    started = threading.Event()
    aborted = threading.Event()

    async def handler(request):
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            aborted.set()

    manager = NetworkManager()
    manager.configure(
        transport=httpx.MockTransport(handler),
        streams_per_connection=4,
    )
    future = manager.submit_request(
        "GET",
        "https://example.test/",
        project_key="probe",
        node_name="A",
    )
    try:
        assert started.wait(2)
        snapshot = manager.snapshot()
        assert snapshot["active_phase_counts"] == {"client_acquired": 1}
        assert snapshot["shards"][0]["in_flight"] == 1
        assert snapshot["shards"][0]["active_nodes"] == {"A": 1}

        future.cancel()
        assert aborted.wait(2)
        with pytest.raises(asyncio.CancelledError):
            future.result(timeout=2)
    finally:
        manager.close()


def test_network_manager_snapshot_is_batched_into_sqlite(tmp_path):
    workflow = MicroWorkflow(tmp_path, runner="api")
    workflow.graph([])
    router = NodeRouter("A", runner="api", max_threads=1)

    @router.task
    def work(ctx):
        return "ok"

    workflow.include_router(router)
    workflow.storage.publish_network_manager_snapshot(
        [{
            "node_name": "A",
            "submitted": 10,
            "dispatched": 10,
            "completed": 9,
            "failed": 1,
            "bytes_received": 4096,
            "in_flight": 0,
            "peak_in_flight": 4,
            "max_ingress_delay_seconds": 0.2,
            "max_request_seconds": 1.0,
            "average_request_seconds": 0.4,
            "last_error": "boom",
            "_manager": {
                "active_phase_counts": {"http2.receive_response_body.started": 2},
                "oldest_active_seconds": 12.5,
                "shards": [{"shard_id": 1, "in_flight": 2}],
            },
        }],
        123.0,
    )
    workflow.storage.flush_db_mutations()
    row = workflow.storage.network_manager_state()["A"]
    assert row["submitted"] == 10
    assert row["completed"] == 9
    assert row["failed"] == 1
    assert row["updated_at"] == 123.0
    diagnostic = json.loads(
        (tmp_path / ".mwf" / "network_manager.json").read_text(encoding="utf-8")
    )
    assert diagnostic["updated_at"] == 123.0
    assert diagnostic["oldest_active_seconds"] == 12.5
    assert diagnostic["shards"] == [{"shard_id": 1, "in_flight": 2}]

    # A fresh process/run starts manager counters from zero. Persist that fresh
    # high-water state instead of leaking the previous run's peaks into monitor.
    workflow.storage.publish_network_manager_snapshot(
        [{
            "node_name": "A",
            "submitted": 1,
            "dispatched": 1,
            "completed": 1,
            "failed": 0,
            "bytes_received": 2,
            "in_flight": 0,
            "peak_in_flight": 1,
            "max_ingress_delay_seconds": 0.01,
            "max_request_seconds": 0.02,
            "average_request_seconds": 0.02,
            "last_error": None,
        }],
        456.0,
    )
    workflow.storage.flush_db_mutations()
    fresh = workflow.storage.network_manager_state()["A"]
    assert fresh["peak_in_flight"] == 1
    assert fresh["max_ingress_delay_seconds"] == 0.01
    assert fresh["max_request_seconds"] == 0.02
    assert fresh["updated_at"] == 456.0


def test_network_state_schema_migrates_to_v4(tmp_path):
    workflow = MicroWorkflow(tmp_path, runner="direct")
    row = workflow.storage.db_connection().execute(
        "SELECT value FROM metadata WHERE key='database_schema_version'"
    ).fetchone()
    assert int(row["value"]) == 4
    tables = {
        str(row[0])
        for row in workflow.storage.db_connection().execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    }
    assert "network_state" in tables


def test_refreshable_queue_rowid_plan_avoids_temp_sort(tmp_path):
    workflow = MicroWorkflow(tmp_path, runner="api")
    workflow.graph([])
    router = NodeRouter("A", runner="api", max_threads=1)

    @router.task
    def work(ctx):
        return "ok"

    workflow.include_router(router)
    for job_id in range(1, 5):
        workflow.storage.create_job(Job(node_name="A", job_id=job_id, params={}))
    plan = workflow.storage.db_connection().execute(
        "EXPLAIN QUERY PLAN SELECT rowid AS source_rowid, job_id FROM jobs NOT INDEXED "
        "WHERE rowid>? AND node_name=? AND status=? ORDER BY rowid LIMIT ?",
        (0, "A", "queued", 2),
    ).fetchall()
    details = " ".join(str(row["detail"]) for row in plan).upper()
    assert "TEMP B-TREE" not in details


class _Writer:
    def __init__(self):
        self.data = bytearray()
    def write(self, data):
        self.data.extend(data)
    async def drain(self):
        return None


def test_h1_only_chunk_is_bandwidth_paced():
    async def run():
        writer = _Writer()
        started = time.monotonic()
        await h1_transfer(writer, 4096, 4096, 0, 4096)
        return time.monotonic() - started
    elapsed = asyncio.run(run())
    assert 0.90 <= elapsed <= 1.50


def test_h2_only_chunk_is_bandwidth_paced():
    class FakeSession:
        def __init__(self):
            self.finished = None
        async def headers(self, stream_id, headers, end_stream=False):
            return None
        async def data(self, stream_id, payload, end_stream=False):
            if end_stream:
                self.finished = time.monotonic()

    async def run():
        session = FakeSession()
        started = time.monotonic()
        await H2Session.serve_stream(
            session,
            1,
            "/transfer?bytes=4096&bps=4096&delay_ms=0&chunk=4096",
        )
        return session.finished - started
    elapsed = asyncio.run(run())
    assert 0.90 <= elapsed <= 1.50
