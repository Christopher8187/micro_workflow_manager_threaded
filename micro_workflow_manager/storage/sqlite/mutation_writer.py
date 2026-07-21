from __future__ import annotations

import queue
import sqlite3
from concurrent.futures import Future, InvalidStateError
from dataclasses import dataclass
from threading import Condition, Lock, Thread, current_thread
from time import monotonic
from typing import Any, Callable, Hashable, TypeVar


T = TypeVar("T")
MutationOutcome = tuple[bool, Any]
GroupedMutation = Callable[[sqlite3.Connection, list[Any]], list[MutationOutcome]]


@dataclass(slots=True)
class _MutationRequest:
    serial: int
    priority: int
    future: Future
    collect_seconds: float
    operation: Callable[[sqlite3.Connection], Any] | None = None
    group_key: Hashable | None = None
    group_item: Any = None
    group_operation: GroupedMutation | None = None


class SQLiteMutationWriter:
    """Single priority lane for all project-local SQLite mutations.

    Normal mutations share a short group-commit window. Related high-volume
    mutations, such as terminal job publication, may provide a group operation
    and a slightly longer bounded collection window. They still use this same
    queue, thread, transaction, and durability barrier; there is no second
    terminal-status daemon or hand-off queue.
    """

    def __init__(
        self,
        storage,
        *,
        max_batch: int = 1024,
        collect_seconds: float = 0.001,
    ) -> None:
        if max_batch < 1:
            raise ValueError("max_batch must be positive")
        if collect_seconds < 0:
            raise ValueError("collect_seconds must be non-negative")
        self.storage = storage
        self.max_batch = int(max_batch)
        self.collect_seconds = float(collect_seconds)
        self._queue: queue.PriorityQueue[tuple[int, int, _MutationRequest]] = (
            queue.PriorityQueue()
        )
        self._serial = 0
        self._thread: Thread | None = None
        self._guard = Lock()
        self._progress = Condition(self._guard)
        self._completed_through = 0
        self._completed_out_of_order: set[int] = set()

    def submit(
        self,
        operation: Callable[[sqlite3.Connection], T],
        *,
        wait: bool,
        priority: int,
        collect_seconds: float | None = None,
    ) -> T | Future[T]:
        request = self._new_request(
            priority=priority,
            collect_seconds=collect_seconds,
            operation=operation,
        )
        self._enqueue(request)
        return request.future.result() if wait else request.future

    def submit_grouped(
        self,
        group_key: Hashable,
        item: Any,
        operation: GroupedMutation,
        *,
        wait: bool,
        priority: int,
        collect_seconds: float,
    ) -> Any | Future:
        """Submit one item that may be applied with related queued items."""
        if group_key is None:
            raise ValueError("group_key must not be None")
        request = self._new_request(
            priority=priority,
            collect_seconds=collect_seconds,
            group_key=group_key,
            group_item=item,
            group_operation=operation,
        )
        self._enqueue(request)
        return request.future.result() if wait else request.future

    def _new_request(
        self,
        *,
        priority: int,
        collect_seconds: float | None,
        operation: Callable[[sqlite3.Connection], Any] | None = None,
        group_key: Hashable | None = None,
        group_item: Any = None,
        group_operation: GroupedMutation | None = None,
    ) -> _MutationRequest:
        if type(priority) is not int:
            raise TypeError("priority must be an integer")
        window = self.collect_seconds if collect_seconds is None else collect_seconds
        if window < 0:
            raise ValueError("collect_seconds must be non-negative")
        with self._guard:
            self._serial += 1
            serial = self._serial
        return _MutationRequest(
            serial=serial,
            priority=priority,
            future=Future(),
            collect_seconds=float(window),
            operation=operation,
            group_key=group_key,
            group_item=group_item,
            group_operation=group_operation,
        )

    def _enqueue(self, request: _MutationRequest) -> None:
        with self._guard:
            self._queue.put((request.priority, request.serial, request))
            if self._thread is None or not self._thread.is_alive():
                self._thread = Thread(
                    target=self._run,
                    name="mwf-sqlite-group-commit",
                    daemon=True,
                )
                self._thread.start()

    def barrier(self, serial: int | None = None) -> None:
        """Wait until every mutation submitted through ``serial`` is durable."""
        with self._progress:
            target = self._serial if serial is None else int(serial)
            while self._completed_through < target:
                self._progress.wait()

    def flush(self) -> None:
        """Compatibility name for a snapshot durability barrier."""
        self.barrier()

    def _collect_batch(
        self,
        first: tuple[int, int, _MutationRequest],
    ) -> list[tuple[int, int, _MutationRequest]]:
        batch = [first]
        priority = first[0]
        deadline = monotonic() + first[2].collect_seconds
        while len(batch) < self.max_batch:
            remaining = deadline - monotonic()
            if remaining <= 0:
                break
            try:
                candidate = self._queue.get(timeout=remaining)
            except queue.Empty:
                break
            if candidate[0] != priority:
                self._queue.put(candidate)
                break
            batch.append(candidate)
        return batch

    @staticmethod
    def _resolve(future: Future, succeeded: bool, value: Any) -> None:
        try:
            if succeeded:
                future.set_result(value)
            else:
                future.set_exception(value)
        except InvalidStateError:
            # Cancelling a local waiter does not undo a committed mutation.
            pass

    def _record_completed(self, requests: list[_MutationRequest]) -> None:
        with self._progress:
            self._completed_out_of_order.update(request.serial for request in requests)
            while self._completed_through + 1 in self._completed_out_of_order:
                self._completed_through += 1
                self._completed_out_of_order.remove(self._completed_through)
            self._progress.notify_all()

    @staticmethod
    def _savepoint(connection: sqlite3.Connection, index: int) -> str:
        name = f"mwf_batch_{index}"
        connection.execute(f"SAVEPOINT {name}")
        return name

    def _execute_batch(
        self,
        connection: sqlite3.Connection,
        requests: list[_MutationRequest],
    ) -> dict[int, MutationOutcome]:
        outcomes: dict[int, MutationOutcome] = {}
        grouped: dict[Hashable, list[_MutationRequest]] = {}
        units: list[tuple[str, Any]] = []

        for request in requests:
            if request.group_key is None:
                units.append(("single", request))
                continue
            group = grouped.get(request.group_key)
            if group is None:
                group = []
                grouped[request.group_key] = group
                units.append(("group", request.group_key))
            group.append(request)

        for index, (kind, value) in enumerate(units):
            savepoint = self._savepoint(connection, index)
            if kind == "single":
                request: _MutationRequest = value
                try:
                    result = request.operation(connection)
                except BaseException as error:
                    connection.execute(f"ROLLBACK TO {savepoint}")
                    connection.execute(f"RELEASE {savepoint}")
                    outcomes[request.serial] = (False, error)
                else:
                    connection.execute(f"RELEASE {savepoint}")
                    outcomes[request.serial] = (True, result)
                continue

            group_requests = grouped[value]
            operation = group_requests[0].group_operation
            try:
                group_outcomes = operation(
                    connection,
                    [request.group_item for request in group_requests],
                )
                if len(group_outcomes) != len(group_requests):
                    raise RuntimeError(
                        "grouped SQLite mutation returned the wrong number of outcomes"
                    )
            except BaseException as error:
                connection.execute(f"ROLLBACK TO {savepoint}")
                connection.execute(f"RELEASE {savepoint}")
                for request in group_requests:
                    outcomes[request.serial] = (False, error)
            else:
                connection.execute(f"RELEASE {savepoint}")
                for request, outcome in zip(group_requests, group_outcomes):
                    succeeded, result = outcome
                    if not succeeded and not isinstance(result, BaseException):
                        result = RuntimeError(str(result))
                    outcomes[request.serial] = (bool(succeeded), result)

        return outcomes

    def _finish_batch(
        self,
        requests: list[_MutationRequest],
        outcomes: dict[int, MutationOutcome],
    ) -> None:
        for request in requests:
            succeeded, value = outcomes[request.serial]
            self._resolve(request.future, succeeded, value)
        self._record_completed(requests)

    def _run(self) -> None:
        try:
            while True:
                try:
                    first = self._queue.get(timeout=0.25)
                except queue.Empty:
                    with self._guard:
                        if self._queue.empty():
                            self._thread = None
                            return
                    continue

                queued_batch = self._collect_batch(first)
                requests = [item[2] for item in queued_batch]
                try:
                    with self.storage.db_transaction() as connection:
                        outcomes = self._execute_batch(connection, requests)
                except BaseException as error:
                    outcomes = {
                        request.serial: (False, error)
                        for request in requests
                    }
                self._finish_batch(requests, outcomes)

                if self._queue.empty():
                    self.storage.close_thread_connection()
        finally:
            self.storage.close_thread_connection()
            with self._progress:
                if self._thread is current_thread():
                    self._thread = None
                self._progress.notify_all()
