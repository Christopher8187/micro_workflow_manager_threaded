from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
import json
from threading import Event
from time import monotonic
from typing import Any, Callable, TypeVar

from .errors import JobRestartedError, JobTimeoutError
from .fibers import in_fiber_runtime
from .file_helpers import _relative_parts
from .storage.input_publication_files import InputFileChange
from .models import Job
from .paths import relative_posix
from .project_format import is_link_or_reparse_point

def _event_value(value: Any, *, depth: int = 0) -> Any:
    """Convert trace/event payloads to durable JSON without surprising callers."""
    if depth > 8:
        return repr(value)
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, bytes):
        return {"type": "bytes", "size": len(value), "preview": value[:256].hex()}
    if isinstance(value, BaseException):
        return {"type": type(value).__name__, "message": str(value), "repr": repr(value)}
    if isinstance(value, dict):
        return {str(key): _event_value(item, depth=depth + 1) for key, item in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_event_value(item, depth=depth + 1) for item in value]
    try:
        json.dumps(value)
    except (TypeError, ValueError):
        return repr(value)
    return value


def _content_preview(content: Any, *, limit: int = 4000) -> dict[str, Any]:
    if isinstance(content, bytes):
        return {"content_type": "bytes", "size": len(content), "preview": content[:256].hex()}
    text = str(content)
    return {
        "content_type": "text",
        "size": len(text),
        "preview": text if len(text) <= limit else text[:limit] + "...",
        "truncated": len(text) > limit,
    }


T = TypeVar("T")


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
        task_name: str | None = None,
        task_role: str = "main",
        attempt: int | None = None,
        repeat_index: int | None = None,
        pending_event_recorder: Callable[[Any], None] | None = None,
        execution_checker: Callable[[], None] | None = None,
    ):
        super().__init__(cancellation_event=cancellation_event)
        self.system = system
        self.from_node = from_node
        self.from_job_id = from_job_id
        self.to_node = to_node
        self.execution_generation = execution_generation
        self.execution_id = execution_id
        self.task_name = task_name
        self.task_role = task_role
        self.attempt = attempt
        self.repeat_index = repeat_index
        self._pending_event_recorder = pending_event_recorder
        self._execution_checker = execution_checker

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

    def _event_fields(self) -> dict[str, Any]:
        return {
            "task": self.task_name,
            "task_role": self.task_role,
            "attempt": self.attempt,
            "repeat_index": self.repeat_index,
        }

    def _record_event(self, event: str, **data: Any) -> None:
        self._check_local_execution()
        asynchronous = in_fiber_runtime() and self._pending_event_recorder is not None
        future = self.system.storage.append_job_event(
            self.from_node,
            self.from_job_id,
            event,
            _wait=not asynchronous,
            _execution_generation=self.execution_generation,
            _execution_id=self.execution_id,
            **self._event_fields(),
            **{key: _event_value(value) for key, value in data.items()},
        )
        if asynchronous:
            self._pending_event_recorder(future)

    def checkpoint(self):
        """Raise if the parent job was restarted or this task timed out."""
        if self._execution_checker is not None:
            self._execution_checker()
            return
        self._check_local_execution()
        if self.execution_id is not None and self._cancellation_event is None:
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
        return self._guarded(
            lambda: self.system.add_job(
                from_node=self.from_node,
                to_node=self.to_node,
                job_id=job_id,
                autostart=autostart,
                _parent_job_id=self.from_job_id,
                _parent_execution_id=self.execution_id,
                _parent_event_data=self._event_fields(),
                idempotency_key=idempotency_key,
                **params,
            )
        )

    def add_many(
        self,
        params_list: list[dict[str, Any]],
        *,
        autostart: bool = False,
        idempotency_keys: list[str | None] | None = None,
    ) -> list[Job]:
        """Create many jobs on one target node as one guarded batch.

        This is intended for high-fanout producers. It preserves one job per
        params object while paying execution-fence and node-lock overhead once.
        """
        results = self._guarded(
            lambda: self.system.add_jobs(
                from_node=self.from_node,
                to_node=self.to_node,
                params_list=params_list,
                autostart=autostart,
                _parent_job_id=self.from_job_id,
                _parent_execution_id=self.execution_id,
                idempotency_keys=idempotency_keys,
            )
        )
        self._record_event(
            "jobs_created",
            jobs=[
                {"node": self.to_node, "job_id": result.job_id, "params": params}
                for result, params in zip(results, params_list)
            ],
        )
        return results

    @property
    def input_dir(self) -> Path:
        return self.input_path()

    def input_path(self, *parts: str) -> Path:
        return self._guarded(lambda: self.system.storage.input_path(self.to_node, self._input_name(*parts)))

    def _input_name(self, *parts: str) -> str:
        producer = self.system.storage.validate_node_name(self.from_node)
        receiver = self.system.storage.validate_node_name(self.to_node)
        name = '/'.join((producer, *_relative_parts(*parts)))
        root = self.system.storage.project_dir
        target = root / 'node' / receiver / 'input' / name
        if any(is_link_or_reparse_point(path) for path in (target, *target.parents)
               if path.is_relative_to(root)):
            raise ValueError(f'Unsafe managed input path: {target}')
        return name

    def _publish_inputs(self, changes) -> list[Path]:
        current = getattr(self.system._job_context, 'execution_id', None)
        if current is not None and current != self.execution_id:
            raise RuntimeError('Input producer execution differs from the current task')
        changes = tuple(changes)
        events = []
        for change in changes:
            if change.kind == 'delete':
                events.append(None)
                continue
            details = ({'source': str(change.content), 'content_type': 'file'}
                       if change.kind == 'copy' else _content_preview(change.content))
            events.append(dict(self._event_fields(), **details))
        return self._guarded(lambda: self.system.storage.publish_managed_inputs(
            self.from_node, self.from_job_id, self.execution_generation,
            self.execution_id, self.to_node, changes, event_data=events,
        ))

    def write_input(self, filename: str, content: str, *, overwrite: bool = False) -> Path:
        path, = self._publish_inputs([InputFileChange(filename, 'text', content, overwrite)])
        return path

    def write_input_bytes(self, filename: str, content: bytes, *, overwrite: bool = False) -> Path:
        path, = self._publish_inputs([InputFileChange(filename, 'bytes', content, overwrite)])
        return path

    def write_inputs(
        self,
        entries: list[tuple[str, str]],
        *,
        overwrite: bool = False,
        encoding: str = 'utf-8',
    ) -> list[Path]:
        """Publish one batch's input bytes and producing executions together."""
        if not isinstance(entries, list):
            raise TypeError('entries must be a list of (filename, content) pairs')
        changes = []
        for entry in entries:
            if not isinstance(entry, tuple) or len(entry) != 2:
                raise TypeError('each entry must be a (filename, content) tuple')
            filename, content = entry
            if not isinstance(filename, str) or not filename:
                raise ValueError('batch input filenames must be non-empty strings')
            changes.append(InputFileChange(filename, 'text', content, overwrite, encoding))
        paths = self._publish_inputs(changes)
        return paths

    def add_input_file(
        self,
        source: str | Path,
        filename: str | None = None,
        *,
        overwrite: bool = False,
    ) -> Path:
        source = Path(source)
        path, = self._publish_inputs([
            InputFileChange(source.name if filename is None else filename, 'copy', source, overwrite),
        ])
        return path

    def add_input_files(self, sources, *, overwrite: bool = False) -> list[Path]:
        sources = [Path(source) for source in sources]
        paths = self._publish_inputs([
            InputFileChange(source.name, 'copy', source, overwrite) for source in sources
        ])
        return paths

    def append_input_text(self, filename: str, content: str, *, encoding: str = 'utf-8') -> Path:
        path, = self._publish_inputs([InputFileChange(filename, 'append', content, encoding=encoding)])
        return path

    def delete_input(self, filename: str, *, missing_ok: bool = True) -> None:
        self._publish_inputs([InputFileChange(filename, 'delete', missing_ok=missing_ok)])

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
        errors: tuple[Exception, ...] = (),
        *,
        execution_generation: int,
        execution_id: str | None,
        cancellation_event: Event | None = None,
        attempt_watch=None,
        task_role: str = "main",
    ):
        super().__init__(cancellation_event=cancellation_event)
        self.system = system
        self.current_node = current_node
        self.current_job = current_job
        self.current_task = current_task
        self.attempt = attempt
        self.repeat_index = repeat_index
        self.error = error
        self._errors = tuple(errors)
        self.execution_generation = execution_generation
        self.execution_id = execution_id
        self._attempt_watch = attempt_watch
        self.task_role = task_role
        self._pending_event_futures: list[Any] = []

    @property
    def errors(self) -> list[Exception]:
        return list(self._errors)

    def _check_execution(self):
        from .interrupt_cooperation import check_execution_without_pause, wait_for_interrupt_pauses

        check_execution_without_pause(self)
        wait_for_interrupt_pauses(self)

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

    def _event_fields(self) -> dict[str, Any]:
        return {
            "task": self.current_task,
            "task_role": self.task_role,
            "attempt": self.attempt,
            "repeat_index": self.repeat_index,
        }

    def _record_event(self, event: str, **data: Any) -> None:
        self._check_execution()
        asynchronous = in_fiber_runtime()
        future = self.system.storage.append_job_event(
            self.current_node,
            self.job_id,
            event,
            _wait=not asynchronous,
            _execution_generation=self.execution_generation,
            _execution_id=self.execution_id,
            **self._event_fields(),
            **{key: _event_value(value) for key, value in data.items()},
        )
        if asynchronous:
            self._pending_event_futures.append(future)

    def flush_pending_events(self) -> None:
        """Make every event from this attempt durable in submission order.

        API handlers enqueue observability rows without blocking their pump on
        every SQLite commit. The attempt flushes this short list before it can
        publish success or enter fallback handling, preserving the existing
        durable-before-terminal contract and chronological event order.
        """
        pending, self._pending_event_futures = self._pending_event_futures, []
        for future in pending:
            future.result()

    def trace(self, name: str | dict[str, Any] | None = None, content: Any = None, **details: Any) -> None:
        """Append one user-defined trace object to this job's ordered event journal.

        Supported forms include ``ctx.trace("llm", input=..., output=...)``,
        ``ctx.trace(name="validator", status="warning", content=...)``, and
        ``ctx.trace({"name": "request", "input": ..., "output": ...})``.
        The framework timestamp and task/fallback provenance are added
        automatically. Values that are not directly JSON serializable are
        represented safely rather than breaking the running job.
        """
        if isinstance(name, dict):
            if content is not None:
                raise TypeError("content cannot be supplied when the first trace argument is a dict")
            payload = dict(name)
            trace_name = payload.pop("name", None)
            payload.update(details)
        else:
            trace_name = name or details.pop("name", None)
            payload = dict(details)
            if content is not None:
                payload["content"] = content
        trace_name = str(trace_name or "trace").strip() or "trace"
        for reserved in (
            "time", "event", "node_name", "job_id",
            "task", "task_role", "attempt", "repeat_index",
        ):
            if reserved in payload:
                payload[f"trace_{reserved}"] = payload.pop(reserved)
        self._record_event("trace", name=trace_name, **payload)

    def _record_output(self, path: Path, content: Any) -> None:
        display_path = f"output/{relative_posix(path, self.output_dir)}"
        self._record_event("output_written", path=display_path, **_content_preview(content))

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
            from .fibers import cooperative_sleep
            if cooperative_sleep(wait_for, check_interval=check_interval):
                continue
            if self._cancellation_event is not None:
                self._cancellation_event.wait(wait_for)
            else:
                from time import sleep as _sleep
                _sleep(wait_for)

    @contextmanager
    def side_effects(self):
        """Group several restart-fenced file/queue mutations under one fence.

        This is useful for short local routing handlers that publish multiple
        files and one downstream job. Do not keep the context open across a
        network request or a long computation: the fence intentionally delays a
        second-terminal restart until the block exits.
        """
        self._check_execution()
        if self.execution_id is None:
            yield self
            return
        with self.system.storage.guard_job_execution(
            self.current_node,
            self.job_id,
            self.execution_generation,
            self.execution_id,
        ):
            self._check_execution()
            yield self
            self._check_execution()

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

    def write_output(self, filename: str, content: str) -> Path:
        path = self._guarded(
            lambda: self.system.storage.write_node_output_text(self.current_node, filename, content)
        )
        self._record_output(path, content)
        return path

    def write_output_bytes(self, filename: str, content: bytes) -> Path:
        path = self._guarded(
            lambda: self.system.storage.write_node_output_bytes(self.current_node, filename, content)
        )
        self._record_output(path, content)
        return path

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
            task_name=self.current_task,
            task_role=self.task_role,
            attempt=self.attempt,
            repeat_index=self.repeat_index,
            pending_event_recorder=self._pending_event_futures.append,
            execution_checker=self._check_execution,
        )
