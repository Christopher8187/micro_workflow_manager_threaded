from __future__ import annotations
import asyncio
import time
from concurrent.futures import TimeoutError as FutureTimeoutError
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any, Callable
import httpx
from .manager import network_manager

TimeoutValue = float | int | tuple[float | int, float | int] | httpx.Timeout
_CURRENT_NETWORK_ATTEMPT = ContextVar("mwf_current_network_attempt", default=None)

@contextmanager
def network_attempt_context(workflow, ctx, watch):
    token = _CURRENT_NETWORK_ATTEMPT.set((workflow, ctx, watch))
    try: yield
    finally: _CURRENT_NETWORK_ATTEMPT.reset(token)

def normalize_httpx_timeout(timeout: TimeoutValue) -> httpx.Timeout:
    if isinstance(timeout, httpx.Timeout): return timeout
    if isinstance(timeout, tuple):
        if len(timeout) != 2: raise ValueError("timeout tuple must be (connect_seconds, read_seconds)")
        connect, read = timeout
    else: connect = read = timeout
    connect, read = float(connect), float(read)
    if connect <= 0 or read <= 0: raise ValueError("timeout values must be positive")
    return httpx.Timeout(connect=connect, read=read, write=read, pool=connect)

def timeout_budget_seconds(timeout: TimeoutValue) -> float:
    value = normalize_httpx_timeout(timeout)
    values = [x for x in (value.connect, value.read, value.write, value.pool) if isinstance(x, (int, float))]
    return max(map(float, values), default=30.0)

def close_shared_http_transport(): network_manager.close()

def configure_shared_http_transport(*, http2=False, streams_per_connection=100,
                                    http2_stream_safety_cap=None,
                                    http1_connections_per_shard=None,
                                    architecture=None, state_flush_interval=2.0,
                                    tcp_keepalive=None,
                                    tcp_keepalive_idle_seconds=None,
                                    tcp_keepalive_interval_seconds=None,
                                    tcp_keepalive_probes=None,
                                    **client_kwargs):
    """Configure the backend manager; ordinary MWF applications need no manager wiring."""
    network_manager.configure(http2=http2, streams_per_connection=streams_per_connection,
        http2_stream_safety_cap=http2_stream_safety_cap,
        http1_connections_per_shard=http1_connections_per_shard,
        architecture=architecture, state_flush_interval=state_flush_interval,
        tcp_keepalive=tcp_keepalive,
        tcp_keepalive_idle_seconds=tcp_keepalive_idle_seconds,
        tcp_keepalive_interval_seconds=tcp_keepalive_interval_seconds,
        tcp_keepalive_probes=tcp_keepalive_probes, **client_kwargs)

class SharedHTTPTransport:
    def snapshot(self): return network_manager.snapshot()

    @staticmethod
    def _metadata():
        attempt = _CURRENT_NETWORK_ATTEMPT.get()
        if attempt is None: return None, None, None, None, None
        workflow, ctx, watch = attempt
        return (
            attempt,
            str(workflow.storage.project_dir),
            getattr(ctx, "current_node", None),
            getattr(ctx, "job_id", None),
            getattr(workflow.storage, "publish_network_manager_snapshot", None),
        )

    def request(self, method, url, *, timeout=30, heartbeat_callback: Callable[[float], None] | None=None,
                heartbeat_interval=15.0, wait_name=None, expect_json=False, **kwargs):
        timeout_obj = normalize_httpx_timeout(timeout); kwargs["timeout"] = timeout_obj
        attempt, project, node, job_id, sink = self._metadata()
        if attempt is not None:
            workflow, _ctx, watch = attempt
            workflow.scheduler_supervisor.begin_external_wait(
                watch,
                name=wait_name or f"HTTP {method.upper()} {url}",
                timeout=timeout_budget_seconds(timeout_obj),
                defer_lease_start=True,
            )

        def physical_attempt(attempt_number: int, reason: str | None) -> None:
            if attempt is None:
                return
            workflow, _ctx, watch = attempt
            workflow.scheduler_supervisor.renew_external_wait(
                watch,
                reason=reason or "initial_transport_attempt",
            )

        future = None
        started = time.monotonic(); interval = max(0.1, float(heartbeat_interval))
        try:
            future = network_manager.submit_request(
                method, url, project_key=project, node_name=node, job_id=job_id,
                expect_json=expect_json, state_sink=sink,
                attempt_callback=physical_attempt if attempt is not None else None,
                **kwargs
            )
            while True:
                try: return future.result(timeout=interval if heartbeat_callback else None)
                except FutureTimeoutError:
                    if heartbeat_callback is not None: heartbeat_callback(time.monotonic() - started)
        except BaseException:
            if future is not None:
                future.cancel()
            raise
        finally:
            if attempt is not None:
                workflow, _ctx, watch = attempt
                workflow.scheduler_supervisor.end_external_wait(watch)

    def request_json(self, method, url, **kwargs):
        response = self.request(method, url, expect_json=True, **kwargs); response.raise_for_status(); return response.json()
    def post_json(self, url, **kwargs): return self.request_json("POST", url, **kwargs)
    async def async_request(self, method, url, *, timeout=30, expect_json=False, **kwargs):
        timeout_obj = normalize_httpx_timeout(timeout); kwargs["timeout"] = timeout_obj
        attempt, project, node, job_id, sink = self._metadata()
        if attempt is not None:
            workflow, _ctx, watch = attempt
            workflow.scheduler_supervisor.begin_external_wait(
                watch,
                name=f"HTTP {method.upper()} {url}",
                timeout=timeout_budget_seconds(timeout_obj),
                defer_lease_start=True,
            )

        def physical_attempt(attempt_number: int, reason: str | None) -> None:
            if attempt is None:
                return
            workflow, _ctx, watch = attempt
            workflow.scheduler_supervisor.renew_external_wait(
                watch,
                reason=reason or "initial_transport_attempt",
            )

        future = None
        try:
            future = network_manager.submit_request(
                method, url, project_key=project, node_name=node, job_id=job_id,
                expect_json=expect_json, state_sink=sink,
                attempt_callback=physical_attempt if attempt is not None else None,
                **kwargs
            )
            return await asyncio.wrap_future(future)
        except BaseException:
            if future is not None:
                future.cancel()
            raise
        finally:
            if attempt is not None:
                workflow, _ctx, watch = attempt
                workflow.scheduler_supervisor.end_external_wait(watch)

shared_http_transport = SharedHTTPTransport()
