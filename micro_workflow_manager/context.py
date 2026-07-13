from __future__ import annotations

from contextlib import AbstractContextManager
from dataclasses import dataclass
from pathlib import Path
from threading import Event
from time import monotonic
from typing import Any, Callable, TypeVar

from .errors import JobRestartedError, JobTimeoutError
from .models import Job


T = TypeVar("T")


@dataclass
class PendingJob:
    """Handle returned by ``NodeHandle.add`` inside ``ctx.transaction()``."""

    result: Any = None
    committed: bool = False

    @property
    def job_id(self) -> int:
        if not self.committed or self.result is None:
            raise RuntimeError("The transactional job has not been committed yet")
        return self.result.job_id


class JobTransaction(AbstractContextManager):
    """Stage downstream job creation and commit it after a successful block.

    Staged operations use deterministic idempotency keys. If a filesystem error
    interrupts a multi-job commit, rerunning the parent job can safely finish the
    same transaction without creating duplicates.
    """

    def __init__(self, ctx: "JobContext"):
        self.ctx = ctx
        self.operations: list[tuple[Callable[[str], Any], str | None, PendingJob]] = []
        self.closed = False

    def stage_add(
        self,
        operation: Callable[[str], Any],
        idempotency_key: str | None,
    ) -> PendingJob:
        if self.closed:
            raise RuntimeError("Cannot add work to a closed transaction")
        pending = PendingJob()
        self.operations.append((operation, idempotency_key, pending))
        return pending

    def __enter__(self) -> "JobTransaction":
        if self.ctx._transaction is not None:
            raise RuntimeError("Nested job transactions are not supported")
        self.ctx.checkpoint()
        self.ctx._transaction = self
        return self

    def __exit__(self, exc_type, exc, traceback) -> bool:
        self.ctx._transaction = None
        self.closed = True
        if exc_type is not None:
            return False

        self.ctx.checkpoint()
        for index, (operation, explicit_key, pending) in enumerate(self.operations, 1):
            # Deliberately omit the execution generation. A failed or manually
            # restarted parent receives a new generation, but must reuse any
            # downstream jobs already committed by the same transaction before
            # the failure. Fresh destructive run/runfrom cleanup removes those
            # parented jobs, so stale idempotency entries naturally miss.
            key = explicit_key or (
                f"tx:{self.ctx.current_node}:{self.ctx.job_id}:{index}"
            )
            pending.result = operation(key)
            pending.committed = True
        return False


class _ExecutionChecks:
    def __init__(
        self,
        *,
        cancellation_event: Event | None,
    ):
        self._cancellation_event = cancellation_event

    def _check_local_execution(self):
        if self._cancellation_event is not None and self._cancellation_event.is_set():
            raise JobTimeoutError("The task attempt was cancelled by the scheduler watchdog")

    def is_cancelled(self) -> bool:
        try:
            self._check_local_execution()
        except (JobTimeoutError, JobRestartedError):
            return True
        return False


class NodeHandle(_ExecutionChecks):
    def __init__(
        self,
        system,
        from_node: str,
        from_job_id: int,
        to_node: str,
        execution_generation: int,
        execution_id: str | None,
        *,
        cancellation_event: Event | None = None,
        transaction_getter: Callable[[], JobTransaction | None] | None = None,
    ):
        super().__init__(cancellation_event=cancellation_event)
        self.system = system
        self.from_node = from_node
        self.from_job_id = from_job_id
        self.to_node = to_node
        self.execution_generation = execution_generation
        self.execution_id = execution_id
        self._transaction_getter = transaction_getter or (lambda: None)

    def _guarded(self, action: Callable[[], T]) -> T:
        self.checkpoint()
        if self.execution_id is None:
            return action()
        return self.system.run_job_side_effect(
            self.from_node,
            self.from_job_id,
            self.execution_generation,
            self.execution_id,
            action,
        )

    def checkpoint(self):
        """Raise if the parent job was restarted or this task timed out."""
        self._check_local_execution()
        if self.execution_id is not None:
            self.system.check_job_execution(
                self.from_node,
                self.from_job_id,
                self.execution_generation,
                self.execution_id,
            )

    raise_if_cancelled = checkpoint

    def add(
        self,
        job_id: int | None = None,
        autostart: bool = False,
        idempotency_key: str | None = None,
        **params,
    ):
        def perform(key: str | None = idempotency_key):
            return self._guarded(
                lambda: self.system.add_job(
                    from_node=self.from_node,
                    to_node=self.to_node,
                    job_id=job_id,
                    autostart=autostart,
                    _parent_job_id=self.from_job_id,
                    idempotency_key=key,
                    **params,
                )
            )

        transaction = self._transaction_getter()
        if transaction is not None:
            return transaction.stage_add(lambda key: perform(key), idempotency_key)
        return perform()

    @property
    def input_dir(self) -> Path:
        return self._guarded(lambda: self.system.storage.node_input_dir(self.to_node))

    def input_path(self, *parts: str) -> Path:
        return self._guarded(lambda: self.system.storage.input_path(self.to_node, *parts))

    def write_input(self, filename: str, content: str, *, overwrite: bool = False) -> Path:
        return self._guarded(
            lambda: self.system.storage.write_node_input_text(
                self.to_node, filename, content, overwrite=overwrite
            )
        )

    def write_input_bytes(self, filename: str, content: bytes, *, overwrite: bool = False) -> Path:
        return self._guarded(
            lambda: self.system.storage.write_node_input_bytes(
                self.to_node, filename, content, overwrite=overwrite
            )
        )

    def add_input_file(
        self,
        source: str | Path,
        filename: str | None = None,
        *,
        overwrite: bool = False,
    ) -> Path:
        return self._guarded(
            lambda: self.system.storage.copy_to_node_input(
                self.to_node, source, filename=filename, overwrite=overwrite
            )
        )

    def add_input_files(self, sources, *, overwrite: bool = False) -> list[Path]:
        return [self.add_input_file(source, overwrite=overwrite) for source in sources]

    add_file = add_input_file
    add_files = add_input_files


class JobContext(_ExecutionChecks):
    def __init__(
        self,
        system,
        current_node: str,
        current_job: Job,
        current_task: str,
        attempt: int,
        repeat_index: int,
        error: Exception | None = None,
        *,
        execution_generation: int,
        execution_id: str | None,
        cancellation_event: Event | None = None,
        attempt_watch=None,
    ):
        super().__init__(cancellation_event=cancellation_event)
        self.system = system
        self.current_node = current_node
        self.current_job = current_job
        self.current_task = current_task
        self.attempt = attempt
        self.repeat_index = repeat_index
        self.error = error
        self.execution_generation = execution_generation
        self.execution_id = execution_id
        self._attempt_watch = attempt_watch
        self._transaction: JobTransaction | None = None

    def _check_execution(self):
        """Validate cancellation/restart without reporting progress."""
        self._check_local_execution()
        if self.execution_id is not None:
            self.system.check_job_execution(
                self.current_node,
                self.job_id,
                self.execution_generation,
                self.execution_id,
            )

    def _guarded(self, action: Callable[[], T]) -> T:
        self._check_execution()
        if self.execution_id is None:
            return action()
        return self.system.run_job_side_effect(
            self.current_node,
            self.job_id,
            self.execution_generation,
            self.execution_id,
            action,
        )

    def checkpoint(
        self,
        name: str | None = None,
        *,
        timeout: float | int | None = None,
        progress: float | int | None = None,
        detail: str | None = None,
    ) -> None:
        """Report progress and refresh the scheduler-owned checkpoint deadline.

        Supported forms include::

            ctx.checkpoint()
            ctx.checkpoint("request started")
            ctx.checkpoint(name="request started", timeout=30)
            ctx.checkpoint("page complete", progress=0.5, detail="5 of 10")

        ``timeout`` is the maximum time allowed until the handler either
        completes or reaches its next checkpoint. ``progress`` is a finite
        fraction from 0 through 1. ``detail`` is optional human-readable text
        shown by ``mwf inspect``.

        A dynamic checkpoint timeout requires the handler to be on the
        scheduler-supervised execution path. The normal way to enable that is
        to declare a total ``timeout=...`` on the task or fallback. The legacy
        ``checkpoint_timeout`` configuration remains supported for backward
        compatibility.
        """
        self._check_execution()
        if self._attempt_watch is not None:
            self.system.scheduler_supervisor.report_checkpoint(
                self._attempt_watch,
                name=name,
                timeout=timeout,
                progress=progress,
                detail=detail,
            )
        self._check_execution()

    def raise_if_cancelled(self):
        self._check_execution()

    def is_cancelled(self) -> bool:
        try:
            self._check_execution()
        except (JobTimeoutError, JobRestartedError):
            return True
        return False

    def sleep(self, seconds: float, *, check_interval: float = 0.1):
        """Sleep cooperatively, waking promptly for restart or timeout."""
        if seconds < 0:
            raise ValueError("seconds must be >= 0")
        if check_interval <= 0:
            raise ValueError("check_interval must be positive")
        end = monotonic() + seconds
        while True:
            self._check_execution()
            remaining = end - monotonic()
            if remaining <= 0:
                return
            wait_for = min(check_interval, remaining)
            if self._cancellation_event is not None:
                self._cancellation_event.wait(wait_for)
            else:
                from time import sleep as _sleep
                _sleep(wait_for)

    def transaction(self) -> JobTransaction:
        """Stage downstream ``ctx.node(...).add(...)`` calls until block success."""
        return JobTransaction(self)

    @property
    def job_id(self) -> int:
        return self.current_job.job_id

    @property
    def params(self) -> dict[str, Any]:
        return self.current_job.params

    @property
    def input_dir(self) -> Path:
        self._check_execution()
        return self.system.storage.node_input_dir(self.current_node)

    @property
    def output_dir(self) -> Path:
        return self._guarded(lambda: self.system.storage.node_output_dir(self.current_node))

    @property
    def storage_dir(self) -> Path:
        return self._guarded(lambda: self.system.storage.job_dir(self.current_node, self.job_id))

    @property
    def files_dir(self) -> Path:
        return self._guarded(lambda: self.system.storage.files_dir(self.current_node, self.job_id))

    def input_path(self, *parts: str) -> Path:
        self._check_execution()
        return self.system.storage.input_path(self.current_node, *parts)

    def output_path(self, *parts: str) -> Path:
        return self._guarded(lambda: self.system.storage.output_path(self.current_node, *parts))

    def input_files(self, pattern: str = "*", recursive: bool = False, files_only: bool = True) -> list[Path]:
        self._check_execution()
        return self.system.storage.input_files(
            self.current_node, pattern=pattern, recursive=recursive, files_only=files_only
        )

    def output_files(self, pattern: str = "*", recursive: bool = False, files_only: bool = True) -> list[Path]:
        return self._guarded(
            lambda: self.system.storage.output_files(
                self.current_node, pattern=pattern, recursive=recursive, files_only=files_only
            )
        )

    def write(self, filename: str, content: str) -> Path:
        return self._guarded(
            lambda: self.system.storage.write_text(self.current_node, self.job_id, filename, content)
        )

    def write_bytes(self, filename: str, content: bytes) -> Path:
        return self._guarded(
            lambda: self.system.storage.write_bytes(self.current_node, self.job_id, filename, content)
        )

    def write_output(self, filename: str, content: str) -> Path:
        return self._guarded(
            lambda: self.system.storage.write_node_output_text(self.current_node, filename, content)
        )

    def write_output_bytes(self, filename: str, content: bytes) -> Path:
        return self._guarded(
            lambda: self.system.storage.write_node_output_bytes(self.current_node, filename, content)
        )

    def debug(self, message: str):
        self._guarded(lambda: self.system.storage.write_debug(self.current_node, message))

    def node(self, node_name: str) -> NodeHandle:
        self._check_execution()
        self.system.validate_edge(self.current_node, node_name)
        return NodeHandle(
            system=self.system,
            from_node=self.current_node,
            from_job_id=self.job_id,
            to_node=node_name,
            execution_generation=self.execution_generation,
            execution_id=self.execution_id,
            cancellation_event=self._cancellation_event,
            transaction_getter=lambda: self._transaction,
        )
