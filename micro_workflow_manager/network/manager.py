from __future__ import annotations

import asyncio
import atexit
import os
import queue
import ssl
import threading
import time
import weakref
from concurrent.futures import Future
from dataclasses import dataclass
from typing import Any, Callable

import httpx


@dataclass(slots=True)
class _ClientShard:
    client: httpx.AsyncClient
    in_flight: int = 0
    peak_in_flight: int = 0


class NetworkFuture(Future):
    def __init__(self) -> None:
        super().__init__()
        self.submitted_at = time.monotonic()
        self.dispatched_at: float | None = None
        self.completed_at: float | None = None
        self.node_name: str | None = None
        self.project_key: str | None = None


@dataclass(slots=True)
class NetworkRequest:
    method: str
    url: str
    kwargs: dict[str, Any]
    future: NetworkFuture
    project_key: str | None = None
    node_name: str | None = None
    state_sink: Callable[[list[dict[str, Any]], float], None] | None = None


@dataclass(slots=True)
class _Counters:
    submitted: int = 0
    dispatched: int = 0
    completed: int = 0
    failed: int = 0
    bytes_received: int = 0
    in_flight: int = 0
    peak_in_flight: int = 0
    max_ingress_delay_seconds: float = 0.0
    max_request_seconds: float = 0.0
    total_request_seconds: float = 0.0
    last_error: str | None = None

    def row(self, node_name: str) -> dict[str, Any]:
        return {
            "node_name": node_name,
            "submitted": self.submitted,
            "dispatched": self.dispatched,
            "completed": self.completed,
            "failed": self.failed,
            "bytes_received": self.bytes_received,
            "in_flight": self.in_flight,
            "peak_in_flight": self.peak_in_flight,
            "max_ingress_delay_seconds": self.max_ingress_delay_seconds,
            "max_request_seconds": self.max_request_seconds,
            "average_request_seconds": (
                self.total_request_seconds / self.completed if self.completed else 0.0
            ),
            "last_error": self.last_error,
        }


class NetworkManager:
    """Process-wide event-driven owner for all outbound API traffic.

    Node pumps enqueue requests and wait on Futures; this manager owns the event
    loop, persistent httpx clients, connection sharding, request dispatch, and
    low-frequency network-state persistence. In manager mode cross-thread
    ingress wakeups are coalesced per burst. Direct mode keeps the old
    run_coroutine_threadsafe-per-request shape for A/B benchmarks.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._ready = threading.Event()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._clients: list[_ClientShard] = []
        self._client_kwargs: dict[str, Any] = {}
        self._http2 = False
        self._requested_streams_per_connection = 100
        self._streams_per_connection = 100
        self._http2_stream_safety_cap = 32
        self._http1_connections_per_shard = 16
        self._architecture = "manager"
        self._state_flush_interval = 2.0
        self._ingress: queue.SimpleQueue[NetworkRequest] = queue.SimpleQueue()
        self._ingress_scheduled = False
        self._requests_enqueued = 0
        self._ingress_wakeups = 0
        self._stats: dict[tuple[str, str], _Counters] = {}
        self._sinks: dict[str, weakref.ReferenceType] = {}

    def configure(self, *, http2=False, streams_per_connection=100,
                  http2_stream_safety_cap=None,
                  http1_connections_per_shard=None, architecture=None,
                  state_flush_interval=2.0, **client_kwargs: Any) -> None:
        if type(http2) is not bool:
            raise ValueError("http2 must be a bool")
        if type(streams_per_connection) is not int or streams_per_connection < 1:
            raise ValueError("streams_per_connection must be an integer >= 1")
        if http2_stream_safety_cap is None:
            try:
                http2_stream_safety_cap = int(
                    os.getenv("MWF_HTTP2_STREAM_SAFETY_CAP", "32")
                )
            except ValueError as error:
                raise ValueError(
                    "MWF_HTTP2_STREAM_SAFETY_CAP must be an integer >= 1"
                ) from error
        if type(http2_stream_safety_cap) is not int or http2_stream_safety_cap < 1:
            raise ValueError("http2_stream_safety_cap must be an integer >= 1")
        if http1_connections_per_shard is None:
            try:
                http1_connections_per_shard = int(os.getenv("MWF_HTTP1_CONNECTIONS_PER_SHARD", "16"))
            except ValueError as error:
                raise ValueError("MWF_HTTP1_CONNECTIONS_PER_SHARD must be an integer >= 1") from error
        if type(http1_connections_per_shard) is not int or http1_connections_per_shard < 1:
            raise ValueError("http1_connections_per_shard must be an integer >= 1")
        architecture = str(architecture or os.getenv("MWF_NETWORK_ARCHITECTURE", "manager")).strip().lower()
        architecture = {"legacy": "direct", "central": "manager"}.get(architecture, architecture)
        if architecture not in {"manager", "direct"}:
            raise ValueError("network architecture must be 'manager' or 'direct'")
        state_flush_interval = float(state_flush_interval)
        if not 0 < state_flush_interval <= 2.0:
            raise ValueError("state_flush_interval must be > 0 and <= 2 seconds")
        normalized_client_kwargs = dict(client_kwargs)
        verify = normalized_client_kwargs.get("verify", True)
        if (
            "transport" not in normalized_client_kwargs
            and not isinstance(verify, ssl.SSLContext)
        ):
            # httpx otherwise loads the same CA bundle once per AsyncClient.
            # A resumed dense workflow can need dozens of connection shards at
            # once, and doing that work serially on the manager event loop made
            # admitted requests appear frozen for roughly 9-12 seconds.  SSL
            # contexts are intentionally shareable across clients/connections.
            verify = normalized_client_kwargs.pop("verify", True)
            cert = normalized_client_kwargs.pop("cert", None)
            normalized_client_kwargs["verify"] = httpx.create_ssl_context(
                verify=verify,
                cert=cert,
                trust_env=normalized_client_kwargs.get("trust_env", True),
            )
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                raise RuntimeError("shared network manager is already active")
            self._client_kwargs = normalized_client_kwargs
            self._http2 = http2
            self._requested_streams_per_connection = streams_per_connection
            self._http2_stream_safety_cap = http2_stream_safety_cap
            self._streams_per_connection = (
                min(streams_per_connection, http2_stream_safety_cap)
                if http2
                else streams_per_connection
            )
            self._http1_connections_per_shard = http1_connections_per_shard
            self._architecture = architecture
            self._state_flush_interval = state_flush_interval

    def _shard_capacity(self) -> int:
        return self._streams_per_connection if self._http2 else min(
            self._streams_per_connection, self._http1_connections_per_shard
        )

    def _thread_main(self) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self._loop = loop
        loop.create_task(self._state_flush_loop())
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
                self._thread = threading.Thread(target=self._thread_main, name="mwf-network-manager", daemon=True)
                self._thread.start()
        self._ready.wait()
        if self._loop is None:
            raise RuntimeError("MWF network manager failed to start")
        return self._loop

    def _new_client_shard(self) -> _ClientShard:
        kwargs = dict(self._client_kwargs)
        kwargs.setdefault("follow_redirects", True)
        kwargs.setdefault("http2", self._http2)
        if "limits" not in kwargs:
            capacity = self._shard_capacity()
            kwargs["limits"] = httpx.Limits(
                max_connections=1 if self._http2 else capacity,
                max_keepalive_connections=1 if self._http2 else capacity,
                keepalive_expiry=60.0,
            )
        shard = _ClientShard(httpx.AsyncClient(**kwargs))
        self._clients.append(shard)
        return shard

    async def _acquire_client(self) -> _ClientShard:
        capacity = self._shard_capacity()
        shard = next((x for x in self._clients if x.in_flight < capacity), None)
        if shard is None:
            shard = self._new_client_shard()
        shard.in_flight += 1
        shard.peak_in_flight = max(shard.peak_in_flight, shard.in_flight)
        return shard

    @staticmethod
    def _weak(callback: Callable) -> weakref.ReferenceType | None:
        try:
            return weakref.WeakMethod(callback)
        except TypeError:
            try:
                return weakref.ref(callback)
            except TypeError:
                return None

    def _counter(self, request: NetworkRequest) -> _Counters | None:
        if request.project_key is None or request.node_name is None:
            return None
        key = (request.project_key, request.node_name)
        counter = self._stats.setdefault(key, _Counters())
        if request.state_sink is not None and request.project_key not in self._sinks:
            reference = self._weak(request.state_sink)
            if reference is not None:
                self._sinks[request.project_key] = reference
        return counter

    async def _execute(self, request: NetworkRequest) -> None:
        future = request.future
        if not future.set_running_or_notify_cancel():
            return
        future.dispatched_at = time.monotonic()
        counter = self._counter(request)
        if counter is not None:
            counter.dispatched += 1
            counter.in_flight += 1
            counter.peak_in_flight = max(counter.peak_in_flight, counter.in_flight)
            counter.max_ingress_delay_seconds = max(
                counter.max_ingress_delay_seconds, future.dispatched_at - future.submitted_at
            )
        shard = None
        started = time.monotonic()
        try:
            shard = await self._acquire_client()
            response = await shard.client.request(request.method, request.url, **request.kwargs)
        except BaseException as error:
            future.completed_at = time.monotonic()
            if counter is not None:
                counter.failed += 1
                counter.in_flight = max(0, counter.in_flight - 1)
                counter.last_error = repr(error)
            if not future.cancelled():
                future.set_exception(error)
        else:
            completed = time.monotonic()
            future.completed_at = completed
            if counter is not None:
                duration = completed - started
                counter.completed += 1
                counter.in_flight = max(0, counter.in_flight - 1)
                counter.total_request_seconds += duration
                counter.max_request_seconds = max(counter.max_request_seconds, duration)
                counter.bytes_received += len(response.content)
            response.extensions["mwf_network_manager"] = {
                "submitted_at": future.submitted_at,
                "dispatched_at": future.dispatched_at,
                "completed_at": future.completed_at,
                "node_name": future.node_name,
            }
            if not future.cancelled():
                future.set_result(response)
        finally:
            if shard is not None:
                shard.in_flight = max(0, shard.in_flight - 1)

    def _drain_ingress(self) -> None:
        for _ in range(4096):
            try:
                request = self._ingress.get_nowait()
            except queue.Empty:
                break
            counter = self._counter(request)
            if counter is not None:
                counter.submitted += 1
            asyncio.create_task(self._execute(request))
        with self._lock:
            self._ingress_scheduled = False
            if not self._ingress.empty() and self._loop is not None:
                self._ingress_scheduled = True
                self._loop.call_soon(self._drain_ingress)

    def submit_request(self, method: str, url: str, *, project_key=None,
                       node_name=None, state_sink=None, **kwargs: Any) -> NetworkFuture:
        loop = self.ensure_started()
        future = NetworkFuture()
        future.node_name = node_name
        future.project_key = project_key
        request = NetworkRequest(method, url, kwargs, future, project_key, node_name, state_sink)
        with self._lock:
            self._requests_enqueued += 1
        if self._architecture == "direct":
            async def direct():
                counter = self._counter(request)
                if counter is not None:
                    counter.submitted += 1
                await self._execute(request)
            asyncio.run_coroutine_threadsafe(direct(), loop)
            with self._lock:
                self._ingress_wakeups += 1
            return future
        self._ingress.put(request)
        with self._lock:
            if not self._ingress_scheduled:
                self._ingress_scheduled = True
                self._ingress_wakeups += 1
                loop.call_soon_threadsafe(self._drain_ingress)
        return future

    async def _state_flush_loop(self) -> None:
        while True:
            await asyncio.sleep(self._state_flush_interval)
            self._flush_state()

    def _flush_state(self) -> None:
        grouped: dict[str, list[dict[str, Any]]] = {}
        for (project, node), counter in self._stats.items():
            grouped.setdefault(project, []).append(counter.row(node))
        updated = time.time()
        dead = []
        for project, rows in grouped.items():
            reference = self._sinks.get(project)
            callback = reference() if reference is not None else None
            if callback is None:
                dead.append(project)
                continue
            try:
                callback(rows, updated)
            except Exception:
                pass
        for project in dead:
            self._sinks.pop(project, None)

    async def _close(self) -> None:
        self._flush_state()
        clients = [x.client for x in self._clients]
        self._clients = []
        if clients:
            await asyncio.gather(*(client.aclose() for client in clients), return_exceptions=True)

    async def _snapshot(self) -> dict[str, Any]:
        return {
            "architecture": self._architecture,
            "http2": self._http2,
            "requested_streams_per_connection": self._requested_streams_per_connection,
            "streams_per_connection": self._streams_per_connection,
            "http2_stream_safety_cap": self._http2_stream_safety_cap,
            "shard_capacity": self._shard_capacity(),
            "http1_connections_per_shard": self._http1_connections_per_shard,
            "client_count": len(self._clients),
            "in_flight": sum(x.in_flight for x in self._clients),
            "peak_in_flight_per_client": [x.peak_in_flight for x in self._clients],
            "requests_enqueued": self._requests_enqueued,
            "ingress_wakeups": self._ingress_wakeups,
            "wakeups_per_request": self._ingress_wakeups / self._requests_enqueued if self._requests_enqueued else 0.0,
            "state_flush_interval": self._state_flush_interval,
        }

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            loop, thread = self._loop, self._thread
        if loop is None or thread is None or not thread.is_alive():
            return {"architecture": self._architecture,
                    "http2": self._http2,
                    "requested_streams_per_connection": self._requested_streams_per_connection,
                    "streams_per_connection": self._streams_per_connection,
                    "http2_stream_safety_cap": self._http2_stream_safety_cap,
                    "shard_capacity": self._shard_capacity(),
                    "http1_connections_per_shard": self._http1_connections_per_shard,
                    "client_count": 0,
                    "requests_enqueued": self._requests_enqueued,
                    "ingress_wakeups": self._ingress_wakeups,
                    "wakeups_per_request": 0.0,
                    "state_flush_interval": self._state_flush_interval}
        return asyncio.run_coroutine_threadsafe(self._snapshot(), loop).result(timeout=5)

    def close(self) -> None:
        with self._lock:
            loop, thread = self._loop, self._thread
        if loop is None or thread is None or not thread.is_alive():
            return
        try:
            asyncio.run_coroutine_threadsafe(self._close(), loop).result(timeout=5)
        except Exception:
            pass
        loop.call_soon_threadsafe(loop.stop)
        thread.join(timeout=5)
        with self._lock:
            self._thread = None
            self._ingress_scheduled = False
        self._stats = {}
        self._sinks = {}
        self._requests_enqueued = 0
        self._ingress_wakeups = 0


network_manager = NetworkManager()
atexit.register(network_manager.close)
