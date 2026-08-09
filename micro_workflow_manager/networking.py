from __future__ import annotations

import asyncio
import atexit
import threading
import time
from contextlib import contextmanager
from contextvars import ContextVar
from concurrent.futures import Future, TimeoutError as FutureTimeoutError
from dataclasses import dataclass
from typing import Any, Callable, Coroutine, TypeVar

import httpx


T = TypeVar("T")
TimeoutValue = float | int | tuple[float | int, float | int] | httpx.Timeout


_CURRENT_NETWORK_ATTEMPT: ContextVar[tuple[Any, Any, Any] | None] = ContextVar(
    "mwf_current_network_attempt", default=None
)


@contextmanager
def network_attempt_context(workflow, ctx, watch):
    token = _CURRENT_NETWORK_ATTEMPT.set((workflow, ctx, watch))
    try:
        yield
    finally:
        _CURRENT_NETWORK_ATTEMPT.reset(token)


def normalize_httpx_timeout(timeout: TimeoutValue) -> httpx.Timeout:
    if isinstance(timeout, httpx.Timeout):
        return timeout
    if isinstance(timeout, tuple):
        if len(timeout) != 2:
            raise ValueError("timeout tuple must be (connect_seconds, read_seconds)")
        connect, read = timeout
    else:
        connect = read = timeout
    connect_value = float(connect)
    read_value = float(read)
    if connect_value <= 0 or read_value <= 0:
        raise ValueError("timeout values must be positive")
    return httpx.Timeout(
        connect=connect_value,
        read=read_value,
        write=read_value,
        pool=connect_value,
    )


def timeout_budget_seconds(timeout: TimeoutValue) -> float:
    value = normalize_httpx_timeout(timeout)
    candidates = [value.connect, value.read, value.write, value.pool]
    finite = [float(item) for item in candidates if isinstance(item, (int, float))]
    return max(finite or [30.0])


@dataclass(slots=True)
class _ClientShard:
    client: httpx.AsyncClient
    in_flight: int = 0
    peak_in_flight: int = 0


class _AsyncHTTPRuntime:
    """One process-wide asyncio loop and an elastic pool of HTTP clients."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._ready = threading.Event()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._clients: list[_ClientShard] = []
        self._client_kwargs: dict[str, Any] = {}
        self._http2 = False
        self._streams_per_connection = 100

    def configure(
        self,
        *,
        http2: bool = False,
        streams_per_connection: int = 100,
        **client_kwargs: Any,
    ) -> None:
        if type(http2) is not bool:
            raise ValueError("http2 must be a bool")
        if type(streams_per_connection) is not int or streams_per_connection < 1:
            raise ValueError("streams_per_connection must be an integer >= 1")
        with self._lock:
            if self._clients:
                raise RuntimeError("shared HTTP client is already active")
            self._client_kwargs = dict(client_kwargs)
            self._http2 = http2
            self._streams_per_connection = streams_per_connection

    def _thread_main(self) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self._loop = loop
        self._ready.set()
        try:
            loop.run_forever()
        finally:
            pending = asyncio.all_tasks(loop)
            for task in pending:
                task.cancel()
            if pending:
                loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            loop.close()
            self._loop = None
            self._clients = []

    def ensure_started(self) -> asyncio.AbstractEventLoop:
        with self._lock:
            if self._thread is None or not self._thread.is_alive():
                self._ready = threading.Event()
                self._thread = threading.Thread(
                    target=self._thread_main,
                    name="mwf-httpx-runtime",
                    daemon=True,
                )
                self._thread.start()
        self._ready.wait()
        if self._loop is None:
            raise RuntimeError("MWF HTTP runtime failed to start")
        return self._loop

    def _new_client_shard(self) -> _ClientShard:
        kwargs = dict(self._client_kwargs)
        kwargs.setdefault("follow_redirects", True)
        kwargs.setdefault("http2", self._http2)
        if "limits" not in kwargs:
            if self._http2:
                # One AsyncClient owns one HTTP/2 connection. The runtime opens
                # another shard before assigning more than the configured stream
                # count to this connection.
                kwargs["limits"] = httpx.Limits(
                    max_connections=1,
                    max_keepalive_connections=1,
                    keepalive_expiry=60.0,
                )
            else:
                kwargs["limits"] = httpx.Limits(
                    max_connections=self._streams_per_connection,
                    max_keepalive_connections=self._streams_per_connection,
                    keepalive_expiry=60.0,
                )
        shard = _ClientShard(httpx.AsyncClient(**kwargs))
        self._clients.append(shard)
        return shard

    async def acquire_client(self) -> _ClientShard:
        # All calls run on the one transport event loop, so allocation and
        # counters do not need a cross-thread lock.
        shard = next(
            (
                candidate
                for candidate in self._clients
                if candidate.in_flight < self._streams_per_connection
            ),
            None,
        )
        if shard is None:
            shard = self._new_client_shard()
        shard.in_flight += 1
        shard.peak_in_flight = max(shard.peak_in_flight, shard.in_flight)
        return shard

    async def request(self, method: str, url: str, **kwargs: Any) -> httpx.Response:
        shard = await self.acquire_client()
        try:
            return await shard.client.request(method, url, **kwargs)
        finally:
            shard.in_flight -= 1

    def submit(self, coroutine: Coroutine[Any, Any, T]) -> Future[T]:
        return asyncio.run_coroutine_threadsafe(coroutine, self.ensure_started())

    async def _close_clients(self) -> None:
        clients = [shard.client for shard in self._clients]
        self._clients = []
        if clients:
            await asyncio.gather(
                *(client.aclose() for client in clients),
                return_exceptions=True,
            )

    async def _snapshot(self) -> dict[str, Any]:
        return {
            "http2": self._http2,
            "streams_per_connection": self._streams_per_connection,
            "client_count": len(self._clients),
            "in_flight": sum(shard.in_flight for shard in self._clients),
            "peak_in_flight_per_client": [
                shard.peak_in_flight for shard in self._clients
            ],
        }

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            loop = self._loop
            thread = self._thread
            if loop is None or thread is None or not thread.is_alive():
                return {
                    "http2": self._http2,
                    "streams_per_connection": self._streams_per_connection,
                    "client_count": 0,
                    "in_flight": 0,
                    "peak_in_flight_per_client": [],
                }
        return self.submit(self._snapshot()).result(timeout=5)

    def close(self) -> None:
        with self._lock:
            loop = self._loop
            thread = self._thread
        if loop is None or thread is None or not thread.is_alive():
            return
        try:
            self.submit(self._close_clients()).result(timeout=5)
        except Exception:
            pass
        loop.call_soon_threadsafe(loop.stop)
        thread.join(timeout=5)
        with self._lock:
            self._thread = None


_RUNTIME = _AsyncHTTPRuntime()
atexit.register(_RUNTIME.close)


def close_shared_http_transport() -> None:
    _RUNTIME.close()


def configure_shared_http_transport(
    *,
    http2: bool = False,
    streams_per_connection: int = 100,
    **client_kwargs: Any,
) -> None:
    """Configure connection sharding without imposing workflow concurrency.

    ``max_threads`` on each API node remains the only job admission limit. This
    setting merely starts another HTTP client/connection whenever all existing
    clients already carry ``streams_per_connection`` in-flight requests.
    """
    _RUNTIME.configure(
        http2=http2,
        streams_per_connection=streams_per_connection,
        **client_kwargs,
    )


class SharedHTTPTransport:
    """Framework-owned pooled httpx transport with watchdog-aware sync bridge."""

    async def _request(self, method: str, url: str, **kwargs: Any) -> httpx.Response:
        return await _RUNTIME.request(method, url, **kwargs)

    def snapshot(self) -> dict[str, Any]:
        """Return transport configuration and current shard utilization."""
        return _RUNTIME.snapshot()

    def request(
        self,
        method: str,
        url: str,
        *,
        timeout: TimeoutValue = 30,
        heartbeat_callback: Callable[[float], None] | None = None,
        heartbeat_interval: float = 15.0,
        wait_name: str | None = None,
        recoverable_lease: bool = False,
        cleanup_grace: float = 30.0,
        **kwargs: Any,
    ) -> httpx.Response:
        if type(recoverable_lease) is not bool:
            raise ValueError("recoverable_lease must be a bool")
        cleanup_grace = float(cleanup_grace)
        if cleanup_grace <= 0:
            raise ValueError("cleanup_grace must be positive")

        timeout_obj = normalize_httpx_timeout(timeout)
        kwargs["timeout"] = timeout_obj
        transport_budget = timeout_budget_seconds(timeout_obj)
        lease_seconds = transport_budget + cleanup_grace
        future = _RUNTIME.submit(self._request(method, url, **kwargs))
        attempt = _CURRENT_NETWORK_ATTEMPT.get()
        if attempt is not None:
            workflow, _ctx, watch = attempt
            workflow.scheduler_supervisor.begin_external_wait(
                watch,
                name=wait_name or f"HTTP {method.upper()} {url}",
                timeout=transport_budget,
                cleanup_grace=cleanup_grace,
                fatal_timeout=not recoverable_lease,
            )
        started = time.monotonic()
        lease_deadline = started + lease_seconds if recoverable_lease else None
        interval = max(0.1, float(heartbeat_interval))
        try:
            while True:
                wait_timeout = interval if heartbeat_callback is not None else None
                if lease_deadline is not None:
                    remaining = lease_deadline - time.monotonic()
                    if remaining <= 0:
                        raise httpx.ReadTimeout(
                            f"{wait_name or f'HTTP {method.upper()} {url}'} exceeded "
                            f"its {lease_seconds:g}s recoverable transport lease"
                        )
                    wait_timeout = (
                        remaining
                        if wait_timeout is None
                        else min(wait_timeout, remaining)
                    )
                try:
                    return future.result(timeout=wait_timeout)
                except FutureTimeoutError:
                    if lease_deadline is not None and time.monotonic() >= lease_deadline:
                        raise httpx.ReadTimeout(
                            f"{wait_name or f'HTTP {method.upper()} {url}'} exceeded "
                            f"its {lease_seconds:g}s recoverable transport lease"
                        )
                    if heartbeat_callback is not None:
                        heartbeat_callback(time.monotonic() - started)
        except BaseException:
            future.cancel()
            raise
        finally:
            if attempt is not None:
                workflow, _ctx, watch = attempt
                workflow.scheduler_supervisor.end_external_wait(watch)

    def request_json(self, method: str, url: str, **kwargs: Any) -> Any:
        response = self.request(method, url, **kwargs)
        response.raise_for_status()
        return response.json()

    def post_json(self, url: str, **kwargs: Any) -> Any:
        return self.request_json("POST", url, **kwargs)

    async def async_request(self, method: str, url: str, *, timeout: TimeoutValue = 30, **kwargs: Any) -> httpx.Response:
        kwargs["timeout"] = normalize_httpx_timeout(timeout)
        future = _RUNTIME.submit(self._request(method, url, **kwargs))
        return await asyncio.wrap_future(future)


shared_http_transport = SharedHTTPTransport()
