from __future__ import annotations

import asyncio
import contextvars
import heapq
import queue
import threading
import time as _time
from collections import deque
from concurrent.futures import Future, TimeoutError as FutureTimeoutError
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable

from greenlet import greenlet, getcurrent


_ORIGINAL_SLEEP = _time.sleep
_ORIGINAL_FUTURE_RESULT = Future.result
_RUNTIME: contextvars.ContextVar["FiberRuntime | None"] = contextvars.ContextVar(
    "mwf_fiber_runtime", default=None
)
_CANCELLATION_CHECK: contextvars.ContextVar[Callable[[], None] | None] = contextvars.ContextVar(
    "mwf_fiber_cancellation_check", default=None
)
_PATCHED = False


class FiberLocal:
    """ContextVar-backed attribute storage safe across threads and greenlets."""

    def __init__(self) -> None:
        object.__setattr__(self, "_state", contextvars.ContextVar(f"mwf_fiber_local_{id(self)}", default={}))

    def __getattr__(self, name: str) -> Any:
        state = object.__getattribute__(self, "_state").get()
        if name not in state:
            raise AttributeError(name)
        return state[name]

    def __setattr__(self, name: str, value: Any) -> None:
        state_var = object.__getattribute__(self, "_state")
        state = dict(state_var.get())
        state[name] = value
        state_var.set(state)

    def __delattr__(self, name: str) -> None:
        state_var = object.__getattribute__(self, "_state")
        state = dict(state_var.get())
        if name not in state:
            raise AttributeError(name)
        del state[name]
        state_var.set(state)


@dataclass(slots=True)
class _FutureWait:
    future: Future
    deadline: float | None
    cancellation_check: Callable[[], None] | None


@dataclass(slots=True)
class _SleepWait:
    deadline: float
    cancellation_check: Callable[[], None] | None


@dataclass(slots=True)
class _FutureWaiter:
    state: "_FiberState"
    wait_token: int
    deadline: float | None
    cancellation_check: Callable[[], None] | None
    active: bool = True


@dataclass(slots=True)
class _Resume:
    value: Any = None
    error: BaseException | None = None


@dataclass(slots=True)
class _FiberState:
    index: int
    item: Any
    fiber: greenlet
    result: Any = None
    done: bool = False
    wait_token: int = 0
    pending_future: Future | None = None


class cancellation_scope:
    def __init__(self, check: Callable[[], None] | None):
        self.check = check
        self.token = None

    def __enter__(self):
        self.token = _CANCELLATION_CHECK.set(self.check)
        return self

    def __exit__(self, exc_type, exc, tb):
        if self.token is not None:
            _CANCELLATION_CHECK.reset(self.token)
        return False


def in_fiber_runtime() -> bool:
    return _RUNTIME.get() is not None


def _switch_wait(request: _FutureWait | _SleepWait) -> Any:
    current = getcurrent()
    parent = current.parent
    if parent is None:
        raise RuntimeError("cooperative wait requires a managed greenlet")
    resumed = parent.switch(request)
    if not isinstance(resumed, _Resume):
        raise RuntimeError("fiber runtime returned an invalid resume payload")
    if resumed.error is not None:
        raise resumed.error
    return resumed.value


def await_future(future: Future, timeout: float | None = None) -> Any:
    """Wait for a concurrent future without blocking the API node-pump thread."""
    runtime = _RUNTIME.get()
    if runtime is None:
        return _ORIGINAL_FUTURE_RESULT(future, timeout=timeout)
    if future.done():
        return _ORIGINAL_FUTURE_RESULT(future, timeout=0)
    if timeout is not None:
        timeout = float(timeout)
        if timeout < 0:
            raise ValueError("timeout must be non-negative")
    deadline = None if timeout is None else _time.monotonic() + timeout
    return _switch_wait(
        _FutureWait(
            future=future,
            deadline=deadline,
            cancellation_check=_CANCELLATION_CHECK.get(),
        )
    )


def cooperative_sleep(seconds: float, *, check_interval: float = 0.1) -> bool:
    """Yield the current API fiber for ``seconds``; return False outside fibers."""
    runtime = _RUNTIME.get()
    if runtime is None:
        return False
    seconds = float(seconds)
    if seconds < 0:
        raise ValueError("seconds must be >= 0")
    if seconds == 0:
        check = _CANCELLATION_CHECK.get()
        if check is not None:
            check()
        return True
    _switch_wait(
        _SleepWait(
            deadline=_time.monotonic() + seconds,
            cancellation_check=_CANCELLATION_CHECK.get(),
        )
    )
    return True


def _patched_sleep(seconds: float) -> None:
    if cooperative_sleep(seconds):
        return None
    return _ORIGINAL_SLEEP(seconds)


def _patched_future_result(self: Future, timeout: float | None = None):
    if in_fiber_runtime():
        return await_future(self, timeout=timeout)
    return _ORIGINAL_FUTURE_RESULT(self, timeout=timeout)


def install_bridges() -> None:
    global _PATCHED
    if _PATCHED:
        return
    _time.sleep = _patched_sleep
    Future.result = _patched_future_result
    _PATCHED = True


MIN_ADMISSION_BURST = 16
MAX_ADMISSION_BURST = 64
ADMISSION_SERVICE_INTERVAL = 16


class FiberRuntime:
    """Cooperative runner for synchronous API job controllers.

    Each job is a greenlet. Framework networking and ``ctx.sleep`` suspend the
    greenlet, allowing thousands of controllers to share one node-pump thread.
    """

    def __init__(
        self,
        *,
        poll_interval: float = 0.05,
        start_burst: int = 64,
        max_admission_burst: int = MAX_ADMISSION_BURST,
        service_interval: int = ADMISSION_SERVICE_INTERVAL,
        state_writer_service_interval: int = 1,
        admission_pressure_provider: Callable[[], bool] | None = None,
        admission_pressure_wait: float = 0.005,
    ):
        if poll_interval <= 0:
            raise ValueError("poll_interval must be positive")
        if type(start_burst) is not int or start_burst < 1:
            raise ValueError("start_burst must be an integer >= 1")
        if type(max_admission_burst) is not int or max_admission_burst < 1:
            raise ValueError("max_admission_burst must be an integer >= 1")
        if type(service_interval) is not int or service_interval < 1:
            raise ValueError("service_interval must be an integer >= 1")
        if type(state_writer_service_interval) is not int or state_writer_service_interval < 1:
            raise ValueError("state_writer_service_interval must be an integer >= 1")
        if admission_pressure_provider is not None and not callable(admission_pressure_provider):
            raise TypeError("admission_pressure_provider must be callable or None")
        if admission_pressure_wait < 0:
            raise ValueError("admission_pressure_wait must be >= 0")
        self.poll_interval = float(poll_interval)
        self.start_burst = min(start_burst, max_admission_burst)
        self.max_admission_burst = max_admission_burst
        self.service_interval = service_interval
        self.state_writer_service_interval = state_writer_service_interval
        self.admission_pressure_provider = admission_pressure_provider
        self.admission_pressure_wait = float(admission_pressure_wait)
        self._parent = getcurrent()
        self._states: dict[int, _FiberState] = {}
        self._future_waiters: dict[Future, list[_FutureWaiter]] = {}
        self._future_deadlines: list[
            tuple[float, int, Future, _FutureWaiter]
        ] = []
        self._sleepers: list[tuple[float, int, _FiberState, int, Callable[[], None] | None]] = []
        self._serial = 0
        self._ready: deque[tuple[_FiberState, _Resume]] = deque()
        self._completed_futures: queue.SimpleQueue[Future] = queue.SimpleQueue()
        self._wake_event = threading.Event()
        self._active_count = 0
        self._results: dict[int, Any] = {}
        self._first_error: BaseException | None = None
        self._next_cancellation_poll = _time.monotonic() + self.poll_interval

    def _future_completed(self, future: Future) -> None:
        """Wake the owning pump without rescanning every outstanding future."""
        self._completed_futures.put(future)
        self._wake_event.set()

    def _resume_state(self, state: _FiberState, resume: _Resume | None = None) -> None:
        if state.done:
            return
        try:
            yielded = state.fiber.switch(resume or _Resume())
        except BaseException as error:
            state.done = True
            self._active_count -= 1
            self._first_error = self._first_error or error
            return

        if state.fiber.dead:
            state.done = True
            self._active_count -= 1
            self._results[state.index] = yielded
            return
        state.wait_token += 1
        wait_token = state.wait_token
        if isinstance(yielded, _FutureWait):
            state.pending_future = yielded.future
            waiters = self._future_waiters.get(yielded.future)
            if waiters is None:
                waiters = []
                self._future_waiters[yielded.future] = waiters
                yielded.future.add_done_callback(self._future_completed)
            waiter = _FutureWaiter(
                state=state,
                wait_token=wait_token,
                deadline=yielded.deadline,
                cancellation_check=yielded.cancellation_check,
            )
            waiters.append(waiter)
            if yielded.deadline is not None:
                self._serial += 1
                heapq.heappush(
                    self._future_deadlines,
                    (yielded.deadline, self._serial, yielded.future, waiter),
                )
            return
        if isinstance(yielded, _SleepWait):
            self._serial += 1
            heapq.heappush(self._sleepers, (yielded.deadline, self._serial, state, wait_token, yielded.cancellation_check))
            return
        state.done = True
        self._active_count -= 1
        self._first_error = self._first_error or RuntimeError(
            f"API fiber yielded unsupported value {type(yielded).__name__}"
        )

    def _new_state(self, index: int, item: Any, run_one: Callable[[Any], Any]) -> _FiberState:
        context = contextvars.copy_context()

        def entry(_initial_resume=None):
            def invoke():
                token = _RUNTIME.set(self)
                try:
                    return run_one(item)
                finally:
                    _RUNTIME.reset(token)
            return context.run(invoke)

        child = greenlet(entry, parent=self._parent)
        self._active_count += 1
        return _FiberState(index=index, item=item, fiber=child)

    def _check_waiter(self, state: _FiberState, wait_token: int, check: Callable[[], None] | None) -> bool:
        if state.done or state.wait_token != wait_token:
            return False
        if check is None:
            return True
        try:
            check()
        except BaseException as error:
            if state.pending_future is not None:
                state.pending_future.cancel()
            state.pending_future = None
            self._ready.append((state, _Resume(error=error)))
            return False
        return True

    def _process_futures(self) -> None:
        while True:
            try:
                future = self._completed_futures.get_nowait()
            except queue.Empty:
                break
            waiters = self._future_waiters.pop(future, None)
            if not waiters:
                continue
            try:
                value = _ORIGINAL_FUTURE_RESULT(future, timeout=0)
                resume = _Resume(value=value)
            except BaseException as error:
                resume = _Resume(error=error)
            for waiter in waiters:
                waiter.active = False
                state = waiter.state
                if (
                    state.done
                    or state.wait_token != waiter.wait_token
                    or state.pending_future is not future
                ):
                    continue
                state.pending_future = None
                self._ready.append((state, resume))
        self._wake_event.clear()
        # A callback can race with clear(). Preserve a wakeup if it completed
        # in that narrow window.
        if not self._completed_futures.empty():
            self._wake_event.set()

    def _process_deadlines(self, now_value: float) -> None:
        while self._sleepers and self._sleepers[0][0] <= now_value:
            _, _, state, wait_token, check = heapq.heappop(self._sleepers)
            if state.done or state.wait_token != wait_token:
                continue
            if check is not None:
                try:
                    check()
                except BaseException as error:
                    self._ready.append((state, _Resume(error=error)))
                    continue
            self._ready.append((state, _Resume()))

        while self._future_deadlines and self._future_deadlines[0][0] <= now_value:
            _deadline, _serial, future, waiter = heapq.heappop(
                self._future_deadlines
            )
            if not waiter.active:
                continue
            state = waiter.state
            if (
                state.done
                or state.wait_token != waiter.wait_token
                or state.pending_future is not future
            ):
                waiter.active = False
                continue
            waiter.active = False
            state.pending_future = None
            self._ready.append((state, _Resume(error=FutureTimeoutError())))

    def _poll_cancellation(self) -> None:
        for future, waiters in list(self._future_waiters.items()):
            keep: list[_FutureWaiter] = []
            for waiter in waiters:
                if not waiter.active:
                    continue
                state = waiter.state
                if (
                    state.done
                    or state.wait_token != waiter.wait_token
                    or state.pending_future is not future
                ):
                    waiter.active = False
                    continue
                check = waiter.cancellation_check
                if check is not None:
                    try:
                        check()
                    except BaseException as error:
                        waiter.active = False
                        state.pending_future = None
                        self._ready.append((state, _Resume(error=error)))
                        continue
                keep.append(waiter)
            if keep:
                self._future_waiters[future] = keep
            else:
                self._future_waiters.pop(future, None)

        rebuilt = []
        while self._sleepers:
            deadline, serial, state, wait_token, check = heapq.heappop(self._sleepers)
            if state.done or state.wait_token != wait_token:
                continue
            if check is not None:
                try:
                    check()
                except BaseException as error:
                    self._ready.append((state, _Resume(error=error)))
                    continue
            rebuilt.append((deadline, serial, state, wait_token, check))
        for item in rebuilt:
            heapq.heappush(self._sleepers, item)

    def _next_wait_timeout(self) -> float:
        now_value = _time.monotonic()
        timeout = max(0.0, self._next_cancellation_poll - now_value)
        if self._sleepers:
            timeout = min(timeout, max(0.0, self._sleepers[0][0] - now_value))
        while self._future_deadlines:
            deadline, _serial, future, waiter = self._future_deadlines[0]
            state = waiter.state
            if (
                not waiter.active
                or state.done
                or state.wait_token != waiter.wait_token
                or state.pending_future is not future
            ):
                heapq.heappop(self._future_deadlines)
                continue
            timeout = min(timeout, max(0.0, deadline - now_value))
            break
        return timeout

    def _yield_to_state_writer(self) -> None:
        provider = self.admission_pressure_provider
        if provider is None or not provider():
            return
        deadline = _time.monotonic() + self.admission_pressure_wait
        # This is driven by the mutation writer's urgent Event, not a database
        # status poll. Releasing the GIL here lets the terminal writer complete
        # and clear the event before more synchronous job setup is admitted.
        while provider() and _time.monotonic() < deadline:
            _ORIGINAL_SLEEP(0.0005)

    def run_source(
        self,
        node_name: str,
        items: Iterable,
        run_one: Callable[[Any], Any],
        *,
        limit_provider: Callable[[], int],
    ) -> list[Any]:
        pull = getattr(items, "pull", None)
        refreshable = callable(pull)
        iterator = None if refreshable else iter(items)
        source_exhausted = False
        next_index = 0

        def pull_items(capacity: int) -> list[Any]:
            nonlocal source_exhausted
            if capacity <= 0:
                return []
            if refreshable:
                return list(pull(capacity))
            values = []
            while len(values) < capacity and not source_exhausted:
                try:
                    values.append(next(iterator))
                except StopIteration:
                    source_exhausted = True
                    break
            return values

        def abandon_unstarted(values: list[Any]) -> None:
            for item in values:
                abandon = getattr(item, "abandon_unstarted", None)
                if callable(abandon):
                    try:
                        abandon()
                    except BaseException:
                        # Preserve the original handler failure. Conditional
                        # release is best-effort recovery for preclaimed work.
                        pass

        def resume_ready(max_states: int | None = None) -> int:
            serviced = 0
            while self._ready and (max_states is None or serviced < max_states):
                state, resume = self._ready.popleft()
                self._resume_state(state, resume)
                serviced += 1
                # A resumed completion writes its output and queues its durable
                # terminal event before returning here. Yield on that urgent
                # event in tiny groups, but do not drain an unbounded response
                # wave before the next admission slice.
                if serviced % self.state_writer_service_interval == 0:
                    self._yield_to_state_writer()
            if serviced % self.state_writer_service_interval:
                self._yield_to_state_writer()
            return serviced

        admission_burst = self.start_burst

        while True:
            # Provider callbacks may arrive between admission bursts. Drain them
            # before the next potentially expensive payload-load/claim pull so a
            # completed request can retire promptly instead of becoming a ghost
            # behind the next dense queue slice.
            self._process_futures()
            now_value = _time.monotonic()
            self._process_deadlines(now_value)
            if now_value >= self._next_cancellation_poll:
                self._poll_cancellation()
                self._next_cancellation_poll = now_value + self.poll_interval
            resume_ready(self.service_interval)

            added = 0
            if self._first_error is None:
                self._yield_to_state_writer()
                active = self._active_count
                limit = limit_provider()
                if type(limit) is not int or limit < 0:
                    raise ValueError("runtime API lane concurrency must be an integer >= 0")
                capacity = max(0, limit - active)
                requested = min(capacity, admission_burst)
                pulled = pull_items(requested)
                added = len(pulled)
                if requested > 0 and added == requested and active + added < limit:
                    # A full pull proves the queue is dense. Keep the next
                    # scheduler slice at the bounded dense ceiling.
                    admission_burst = min(
                        self.max_admission_burst,
                        max(min(MIN_ADMISSION_BURST, self.max_admission_burst), requested * 2),
                    )
                elif added < requested:
                    # Sparse and trickling sources return to a small probe.
                    admission_burst = min(MIN_ADMISSION_BURST, self.max_admission_burst)

                for position, item in enumerate(pulled):
                    state = self._new_state(next_index, item, run_one)
                    self._states[next_index] = state
                    next_index += 1
                    self._resume_state(state)
                    if self._first_error is not None:
                        abandon_unstarted(pulled[position + 1:])
                        break

                    # Service completed provider futures within each bounded
                    # admission slice so fast responses can publish output and
                    # terminal state instead of waiting behind every start.
                    if (position + 1) % self.service_interval == 0:
                        self._process_futures()
                        now_value = _time.monotonic()
                        self._process_deadlines(now_value)
                        if now_value >= self._next_cancellation_poll:
                            self._poll_cancellation()
                            self._next_cancellation_poll = (
                                now_value + self.poll_interval
                            )
                        resume_ready(self.service_interval)
                        if self._first_error is not None:
                            abandon_unstarted(pulled[position + 1:])
                            break

            # A failure stops admission, not fibers that have already started.
            # Continue servicing their futures, sleeps, cancellation checks,
            # and terminal publication until every active fiber has exited.
            resume_ready(self.service_interval)
            self._process_futures()
            now_value = _time.monotonic()
            self._process_deadlines(now_value)
            if now_value >= self._next_cancellation_poll:
                self._poll_cancellation()
                self._next_cancellation_poll = now_value + self.poll_interval
            resume_ready(self.service_interval)

            active = self._active_count
            if self._first_error is not None:
                if active == 0:
                    raise self._first_error
                self._wake_event.wait(self._next_wait_timeout())
                continue

            if active == 0:
                if limit == 0:
                    break
                if refreshable and added == 0:
                    # Completion callbacks can drain the last active fibers
                    # after this iteration's admission phase. Probe the live
                    # source once more before declaring the pump quiescent.
                    refill = pull_items(min(limit, MIN_ADMISSION_BURST, self.max_admission_burst))
                    if not refill:
                        waiter = getattr(items, "wait_for_change", None)
                        if callable(waiter) and waiter(self._next_wait_timeout() or self.poll_interval):
                            continue
                        break
                    admission_burst = min(MIN_ADMISSION_BURST, self.max_admission_burst)
                    for position, item in enumerate(refill):
                        state = self._new_state(next_index, item, run_one)
                        self._states[next_index] = state
                        next_index += 1
                        self._resume_state(state)
                        if self._first_error is not None:
                            abandon_unstarted(refill[position + 1:])
                            break
                    continue
                if not refreshable and source_exhausted:
                    break

            # Ready completions are in-memory events and must never wait for the
            # defensive timer. Alternate another bounded completion/admission
            # turn immediately.
            if self._ready:
                continue

            # Keep admitting in bounded bursts while capacity remains, but
            # service active fibers between every burst.
            if added > 0 and active < limit:
                continue

            self._wake_event.wait(self._next_wait_timeout())

        return [self._results[index] for index in sorted(self._results)]
