from __future__ import annotations

import json
import os
import queue
import sqlite3
import time
from concurrent.futures import Future, InvalidStateError
from dataclasses import dataclass
from threading import Condition, Event, Lock, Thread, current_thread
from time import monotonic
from pathlib import Path
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
    weight: int = 1
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
        configured_claim_rows = os.environ.get("MWF_SQLITE_CLAIM_TRANSACTION_ROWS", "192")
        try:
            self.claim_transaction_rows = int(configured_claim_rows)
        except ValueError as error:
            raise ValueError(
                "MWF_SQLITE_CLAIM_TRANSACTION_ROWS must be an integer between 32 and 1024"
            ) from error
        if not 32 <= self.claim_transaction_rows <= 1024:
            raise ValueError(
                "MWF_SQLITE_CLAIM_TRANSACTION_ROWS must be an integer between 32 and 1024"
            )
        self._queue: queue.PriorityQueue[tuple[int, int, _MutationRequest]] = (
            queue.PriorityQueue()
        )
        self._serial = 0
        self._thread: Thread | None = None
        self._guard = Lock()
        self._progress = Condition(self._guard)
        self._completed_through = 0
        self._completed_out_of_order: set[int] = set()
        self._urgent_pending = Event()
        self._diagnostic_path = self.storage.project_dir / ".mwf" / "mutation_writer.json"
        self._diagnostic_last_write = 0.0
        self._active_priority: int | None = None
        self._active_batch_size = 0
        self._last_batch_seconds: float | None = None

    def submit(
        self,
        operation: Callable[[sqlite3.Connection], T],
        *,
        wait: bool,
        priority: int,
        collect_seconds: float | None = None,
        weight: int = 1,
    ) -> T | Future[T]:
        request = self._new_request(
            priority=priority,
            collect_seconds=collect_seconds,
            weight=weight,
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
        weight: int = 1,
    ) -> Any | Future:
        """Submit one item that may be applied with related queued items."""
        if group_key is None:
            raise ValueError("group_key must not be None")
        request = self._new_request(
            priority=priority,
            collect_seconds=collect_seconds,
            weight=weight,
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
        weight: int,
        operation: Callable[[sqlite3.Connection], Any] | None = None,
        group_key: Hashable | None = None,
        group_item: Any = None,
        group_operation: GroupedMutation | None = None,
    ) -> _MutationRequest:
        if type(priority) is not int:
            raise TypeError("priority must be an integer")
        if type(weight) is not int or weight < 1:
            raise ValueError("mutation weight must be an integer >= 1")
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
            weight=weight,
            operation=operation,
            group_key=group_key,
            group_item=group_item,
            group_operation=group_operation,
        )

    def _enqueue(self, request: _MutationRequest) -> None:
        if request.priority <= 5:
            self._urgent_pending.set()
        with self._guard:
            self._queue.put((request.priority, request.serial, request))
            if self._thread is None or not self._thread.is_alive():
                self._thread = Thread(
                    target=self._run,
                    name="mwf-sqlite-group-commit",
                    daemon=True,
                )
                self._thread.start()

    def urgent_state_pending(self) -> bool:
        """Cheap event-driven admission signal for terminal publication."""
        return self._urgent_pending.is_set()

    def _refresh_urgent_signal(self) -> None:
        with self._queue.mutex:
            pending = any(item[0] <= 5 for item in self._queue.queue)
        if not pending:
            self._urgent_pending.clear()

    def diagnostics(self) -> dict[str, Any]:
        """Return a lock-safe local snapshot for ``mwf top``."""
        # Keep the writer lock order consistent with enqueue and idle exit:
        # _guard first, then the PriorityQueue mutex. ``mwf top`` may call this
        # from another thread while the writer is transitioning to idle.
        with self._guard:
            with self._queue.mutex:
                queued = list(self._queue.queue)
            submitted = self._serial
            completed = self._completed_through
            writer_alive = bool(self._thread is not None and self._thread.is_alive())
            active_priority = self._active_priority
            active_batch_size = self._active_batch_size
            last_batch_seconds = self._last_batch_seconds
        priorities = sorted({item[0] for item in queued})
        return {
            "pid": os.getpid(),
            "updated_at": time.time(),
            "queued": len(queued),
            "urgent": sum(1 for item in queued if item[0] <= 5),
            "queued_by_priority": {
                str(priority): sum(1 for item in queued if item[0] == priority)
                for priority in priorities
            },
            "submitted_serial": submitted,
            "completed_through": completed,
            "durability_backlog": max(0, submitted - completed),
            "writer_alive": writer_alive,
            "active_priority": active_priority,
            "active_batch_size": active_batch_size,
            "last_batch_seconds": last_batch_seconds,
            "max_batch": self.max_batch,
            "claim_transaction_rows": self.claim_transaction_rows,
        }

    def persisted_diagnostics(self) -> dict[str, Any]:
        try:
            data = json.loads(self._diagnostic_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {}
        return data if isinstance(data, dict) else {}

    def _publish_diagnostics(self, *, force: bool = False) -> None:
        now_value = time.monotonic()
        if not force and now_value - self._diagnostic_last_write < 0.05:
            return
        self._diagnostic_last_write = now_value
        payload = self.diagnostics()
        path: Path = self._diagnostic_path
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
            temporary.write_text(
                json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
                encoding="utf-8",
            )
            os.replace(temporary, path)
        except OSError:
            # Diagnostics are advisory and must never affect workflow durability.
            pass

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
        # Priority affects admission into a transaction, but SQLite cannot
        # preempt a transaction already executing. Bound lower-priority runtime
        # and ordinary mutation slices so terminal updates can take over within
        # a small amount of work instead of waiting behind 1,024 metadata rows.
        if priority >= 20:
            batch_limit = min(self.max_batch, 128)
            weight_limit = 128
        elif priority > 5:
            batch_limit = min(self.max_batch, 256)
            # Claims can represent hundreds of job rows in one request. Bound
            # the non-preemptible transaction by work, not only request count.
            weight_limit = self.claim_transaction_rows
        else:
            # Terminal publication remains highly batched, but a bounded slice
            # prevents one completion wave from monopolizing visibility for the
            # next wave.
            batch_limit = min(self.max_batch, 128)
            weight_limit = 128
        deadline = monotonic() + first[2].collect_seconds
        batch_weight = first[2].weight
        while len(batch) < batch_limit:
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
            candidate_weight = candidate[2].weight
            if batch_weight + candidate_weight > weight_limit:
                self._queue.put(candidate)
                break
            batch.append(candidate)
            batch_weight += candidate_weight
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
                    # Publish the final heartbeat before making the writer
                    # discoverably idle. A request racing with diagnostics is
                    # observed by the guarded recheck and handled by this same
                    # thread; a request arriving after ``_thread=None`` starts a
                    # new writer. This also prevents temp-project cleanup from
                    # racing a late mutation_writer.json write.
                    self._publish_diagnostics(force=True)
                    with self._guard:
                        if self._queue.empty():
                            self._thread = None
                            return
                    continue

                queued_batch = self._collect_batch(first)
                requests = [item[2] for item in queued_batch]
                with self._guard:
                    self._active_priority = first[0]
                    self._active_batch_size = len(requests)
                self._publish_diagnostics()
                batch_started = monotonic()
                try:
                    with self.storage.db_transaction() as connection:
                        outcomes = self._execute_batch(connection, requests)
                except BaseException as error:
                    outcomes = {
                        request.serial: (False, error)
                        for request in requests
                    }
                batch_seconds = monotonic() - batch_started
                # A waiter may return as soon as its Future is resolved. Close
                # the writer connection first when this transaction drained the
                # queue so a completed workflow does not retain a transient
                # second SQLite connection merely because writer bookkeeping is
                # still finishing. A request racing in afterwards simply opens
                # a fresh connection on the next loop.
                queue_empty = self._queue.empty()
                if queue_empty:
                    self.storage.close_thread_connection()
                self._finish_batch(requests, outcomes)
                with self._guard:
                    self._active_priority = None
                    self._active_batch_size = 0
                    self._last_batch_seconds = batch_seconds
                if first[0] <= 5:
                    self._refresh_urgent_signal()

                self._publish_diagnostics()
        finally:
            self.storage.close_thread_connection()
            with self._progress:
                if self._thread is current_thread():
                    self._thread = None
                self._progress.notify_all()
