from __future__ import annotations

import asyncio
import atexit
import threading
import time
from contextlib import contextmanager
from contextvars import ContextVar
from concurrent.futures import Future, TimeoutError as FutureTimeoutError
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


class _AsyncHTTPRuntime:
    """One process-wide asyncio loop and pooled ``httpx.AsyncClient``."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._ready = threading.Event()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._client: httpx.AsyncClient | None = None
        self._client_kwargs: dict[str, Any] = {}

    def configure(self, **client_kwargs: Any) -> None:
        with self._lock:
            if self._client is not None:
                raise RuntimeError("shared HTTP client is already active")
            self._client_kwargs = dict(client_kwargs)

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
            self._client = None

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

    async def client(self) -> httpx.AsyncClient:
        if self._client is None:
            kwargs = {
                "limits": httpx.Limits(
                    max_connections=None,
                    max_keepalive_connections=512,
                    keepalive_expiry=30.0,
                ),
                "follow_redirects": True,
            }
            kwargs.update(self._client_kwargs)
            self._client = httpx.AsyncClient(**kwargs)
        return self._client

    def submit(self, coroutine: Coroutine[Any, Any, T]) -> Future[T]:
        return asyncio.run_coroutine_threadsafe(coroutine, self.ensure_started())

    async def _close_client(self) -> None:
        client = self._client
        self._client = None
        if client is not None:
            await client.aclose()

    def close(self) -> None:
        with self._lock:
            loop = self._loop
            thread = self._thread
        if loop is None or thread is None or not thread.is_alive():
            return
        try:
            self.submit(self._close_client()).result(timeout=5)
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


def configure_shared_http_transport(**client_kwargs: Any) -> None:
    _RUNTIME.configure(**client_kwargs)


class SharedHTTPTransport:
    """Framework-owned pooled httpx transport with watchdog-aware sync bridge."""

    async def _request(self, method: str, url: str, **kwargs: Any) -> httpx.Response:
        client = await _RUNTIME.client()
        return await client.request(method, url, **kwargs)

    def request(
        self,
        method: str,
        url: str,
        *,
        timeout: TimeoutValue = 30,
        heartbeat_callback: Callable[[float], None] | None = None,
        heartbeat_interval: float = 15.0,
        wait_name: str | None = None,
        **kwargs: Any,
    ) -> httpx.Response:
        timeout_obj = normalize_httpx_timeout(timeout)
        kwargs["timeout"] = timeout_obj
        future = _RUNTIME.submit(self._request(method, url, **kwargs))
        attempt = _CURRENT_NETWORK_ATTEMPT.get()
        if attempt is not None:
            workflow, _ctx, watch = attempt
            workflow.scheduler_supervisor.begin_external_wait(
                watch,
                name=wait_name or f"HTTP {method.upper()} {url}",
                timeout=timeout_budget_seconds(timeout_obj),
            )
        started = time.monotonic()
        interval = max(0.1, float(heartbeat_interval))
        try:
            while True:
                try:
                    return future.result(timeout=interval if heartbeat_callback else None)
                except FutureTimeoutError:
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
