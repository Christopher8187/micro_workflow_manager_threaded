from __future__ import annotations

import asyncio
import atexit
import inspect
import queue
import socket
import threading
import time
import weakref
from typing import Any, Callable

import httpx

from .configuration import NetworkConfigurationMixin
from .diagnostics import NetworkDiagnosticsMixin
from .recovery import NetworkRecoveryMixin
from .types import (
    ClientShard,
    CohortStreamStall,
    NetworkCounters,
    NetworkFuture,
    NetworkRequest,
)


class NetworkManager(
    NetworkConfigurationMixin,
    NetworkRecoveryMixin,
    NetworkDiagnosticsMixin,
):
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
        self._clients: list[ClientShard] = []
        self._next_shard_id = 1
        self._active_requests: dict[int, dict[str, Any]] = {}
        self._client_kwargs: dict[str, Any] = {}
        self._http2 = False
        self._requested_streams_per_connection = 100
        self._streams_per_connection = 100
        self._http2_stream_safety_cap = 32
        self._http1_connections_per_shard = 16
        self._tcp_keepalive = True
        self._tcp_keepalive_idle_seconds = 30
        self._tcp_keepalive_interval_seconds = 10
        self._tcp_keepalive_probes = 3
        self._json_terminal_grace_seconds = 5.0
        self._cohort_stall_seconds = 300.0
        self._cohort_terminal_evidence = 16
        self._cohort_retry_limit = 2
        self._retired_shards = 0
        self._json_stream_recoveries = 0
        self._cohort_stream_retries = 0
        self._next_client_index = 0
        self._architecture = "manager"
        self._state_flush_interval = 2.0
        self._ingress: queue.SimpleQueue[NetworkRequest] = queue.SimpleQueue()
        self._ingress_scheduled = False
        self._requests_enqueued = 0
        self._ingress_wakeups = 0
        self._stats: dict[tuple[str, str], NetworkCounters] = {}
        self._sinks: dict[str, weakref.ReferenceType] = {}

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

    def _new_client_shard(self) -> ClientShard:
        kwargs = dict(self._client_kwargs)
        kwargs.setdefault("follow_redirects", True)
        kwargs.setdefault("http2", self._http2)
        capacity = self._shard_capacity()
        if "limits" not in kwargs:
            kwargs["limits"] = httpx.Limits(
                max_connections=1 if self._http2 else capacity,
                max_keepalive_connections=1 if self._http2 else capacity,
                keepalive_expiry=60.0,
            )
        if "transport" not in kwargs and self._tcp_keepalive:
            # A VPN/TUN path can leave a TCP connection half-open without a FIN
            # or RST. Kernel keepalive covers that connection-wide failure. The
            # separate JSON terminal recovery below covers the different case
            # where TCP and sibling HTTP/2 streams remain healthy but one stream
            # never receives its terminal event. Neither mechanism caps request
            # concurrency or changes the declared HTTP/supervisor timeout.
            options = [(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)]
            for option_name, option_value in (
                ("TCP_KEEPIDLE", self._tcp_keepalive_idle_seconds),
                ("TCP_KEEPINTVL", self._tcp_keepalive_interval_seconds),
                ("TCP_KEEPCNT", self._tcp_keepalive_probes),
            ):
                option = getattr(socket, option_name, None)
                if option is not None:
                    options.append((socket.IPPROTO_TCP, option, option_value))
            transport_kwargs = {
                "verify": kwargs.pop("verify", True),
                "trust_env": kwargs.get("trust_env", True),
                "http1": True,
                "http2": self._http2,
                "limits": kwargs.pop("limits"),
                "socket_options": options,
            }
            for name in ("cert", "proxy", "uds", "local_address", "retries"):
                if name in kwargs:
                    transport_kwargs[name] = kwargs.pop(name)
            kwargs["transport"] = httpx.AsyncHTTPTransport(**transport_kwargs)
        shard = ClientShard(
            httpx.AsyncClient(**kwargs),
            shard_id=self._next_shard_id,
            created_at=time.monotonic(),
        )
        self._next_shard_id += 1
        self._clients.append(shard)
        return shard

    @staticmethod
    def _claim_shard(shard: ClientShard) -> ClientShard:
        shard.in_flight += 1
        shard.peak_in_flight = max(shard.peak_in_flight, shard.in_flight)
        shard.requests_started += 1
        return shard

    async def _acquire_client(self, *, fresh: bool = False) -> ClientShard:
        capacity = self._shard_capacity()
        if fresh:
            return self._claim_shard(self._new_client_shard())
        shard = None
        count = len(self._clients)
        for offset in range(count):
            index = (self._next_client_index + offset) % count
            candidate = self._clients[index]
            if not candidate.retiring and candidate.in_flight < capacity:
                shard = candidate
                self._next_client_index = (index + 1) % count
                break
        if shard is None:
            shard = self._new_client_shard()
            self._next_client_index = 0
        return self._claim_shard(shard)

    async def _release_client(self, shard: ClientShard) -> None:
        shard.in_flight = max(0, shard.in_flight - 1)
        if shard.retiring and shard.in_flight == 0:
            await shard.client.aclose()
            if shard in self._clients:
                self._clients.remove(shard)
            self._retired_shards += 1

    @staticmethod
    def _weak(callback: Callable) -> weakref.ReferenceType | None:
        try:
            return weakref.WeakMethod(callback)
        except TypeError:
            try:
                return weakref.ref(callback)
            except TypeError:
                return None

    def _counter(self, request: NetworkRequest) -> NetworkCounters | None:
        if request.project_key is None or request.node_name is None:
            return None
        key = (request.project_key, request.node_name)
        counter = self._stats.setdefault(key, NetworkCounters())
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
        active_key = id(future)
        active = {
                "shard_id": None,
                "project_key": request.project_key,
                "node_name": request.node_name,
                "job_id": request.job_id,
                "started_at": started,
                "phase": "client_acquired",
                "phase_at": time.monotonic(),
                "transport_retries": 0,
        }
        self._active_requests[active_key] = active
        transport_retries = 0
        try:
            kwargs = dict(request.kwargs)
            extensions = dict(kwargs.get("extensions") or {})
            prior_trace = extensions.get("trace")

            async def trace(name: str, info: dict[str, Any]) -> None:
                active["phase"] = str(name)
                active["phase_at"] = time.monotonic()
                stream_id = info.get("stream_id")
                if isinstance(stream_id, int):
                    active["stream_id"] = stream_id
                if prior_trace is not None:
                    result = prior_trace(name, info)
                    if inspect.isawaitable(result):
                        await result

            extensions["trace"] = trace
            kwargs["extensions"] = extensions
            while True:
                shard = await self._acquire_client(fresh=transport_retries > 0)
                active.update(
                    shard_id=shard.shard_id,
                    attempt_started_at=time.monotonic(),
                    cohort_terminal_baseline=(
                        shard.requests_completed + shard.requests_failed
                    ),
                    phase="client_acquired",
                    phase_at=time.monotonic(),
                    response_bytes=0,
                    last_response_progress_at=None,
                )
                try:
                    response = await self._request_with_progress(
                        request, shard, kwargs, active
                    )
                    break
                except CohortStreamStall as error:
                    shard.cohort_stalls += 1
                    shard.retiring = True
                    shard.retired_reason = str(error)
                    shard.retired_at = time.monotonic()
                    self._cohort_stream_retries += 1
                    active["last_cohort_stall"] = str(error)
                    await self._release_client(shard)
                    shard = None
                    if transport_retries >= self._cohort_retry_limit:
                        raise
                    transport_retries += 1
                    active["transport_retries"] = transport_retries
        except BaseException as error:
            future.completed_at = time.monotonic()
            if shard is not None:
                shard.requests_failed += 1
                shard.last_terminal_at = future.completed_at
                shard.last_error = f"{type(error).__name__}: {error}"
            if counter is not None:
                counter.failed += 1
                counter.in_flight = max(0, counter.in_flight - 1)
                counter.last_error = repr(error)
            if not future.done():
                future.set_exception(error)
        else:
            completed = time.monotonic()
            future.completed_at = completed
            shard.requests_completed += 1
            shard.last_terminal_at = completed
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
            if not future.done():
                future.set_result(response)
        finally:
            self._active_requests.pop(active_key, None)
            if shard is not None:
                await self._release_client(shard)

    def _drain_ingress(self) -> None:
        for _ in range(4096):
            try:
                request = self._ingress.get_nowait()
            except queue.Empty:
                break
            counter = self._counter(request)
            if counter is not None:
                counter.submitted += 1
            task = asyncio.create_task(self._execute(request))
            request.future.bind_task(asyncio.get_running_loop(), task)
        with self._lock:
            self._ingress_scheduled = False
            if not self._ingress.empty() and self._loop is not None:
                self._ingress_scheduled = True
                self._loop.call_soon(self._drain_ingress)

    def submit_request(self, method: str, url: str, *, project_key=None,
                       node_name=None, job_id=None, expect_json=False,
                       state_sink=None, **kwargs: Any) -> NetworkFuture:
        loop = self.ensure_started()
        future = NetworkFuture()
        future.node_name = node_name
        future.job_id = job_id
        future.project_key = project_key
        request = NetworkRequest(
            method, url, kwargs, future, project_key, node_name, job_id,
            expect_json, state_sink
        )
        with self._lock:
            self._requests_enqueued += 1
        if self._architecture == "direct":
            async def direct():
                counter = self._counter(request)
                if counter is not None:
                    counter.submitted += 1
                await self._execute(request)
            task = asyncio.run_coroutine_threadsafe(direct(), loop)
            future.bind_task(loop, task)
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

    async def _close(self) -> None:
        self._flush_state()
        clients = [x.client for x in self._clients]
        self._clients = []
        if clients:
            await asyncio.gather(*(client.aclose() for client in clients), return_exceptions=True)

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
        self._active_requests = {}
        self._next_shard_id = 1
        self._next_client_index = 0
        self._requests_enqueued = 0
        self._ingress_wakeups = 0
        self._retired_shards = 0
        self._json_stream_recoveries = 0
        self._cohort_stream_retries = 0


network_manager = NetworkManager()
atexit.register(network_manager.close)
