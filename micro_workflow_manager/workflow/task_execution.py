from pathlib import Path
from queue import Queue
from threading import Event, Thread
from time import perf_counter
from typing import Callable, TypeVar

from ..context import JobContext
from ..errors import (
    InvalidGraphError,
    InvalidJobError,
    JobFailedError,
    JobRestartedError,
    JobTimeoutError,
)
from ..models import CANCELLED, DONE, FAILED, QUEUED, RUNNING, SKIPPED, Job, now
from ..fibers import cancellation_scope, in_fiber_runtime
from ..networking import network_attempt_context


T = TypeVar("T")


class MountedTaskExecutionMixin:
    """Mounted handler invocation, timeout supervision, retries, and fallbacks."""

    def _call_mounted_handler(self, mounted, ctx: JobContext, params: dict):
        previous_node_name = getattr(self._job_context, "node_name", None)
        previous_job_id = getattr(self._job_context, "job_id", None)
        previous_generation = getattr(self._job_context, "generation", None)
        previous_execution_id = getattr(self._job_context, "execution_id", None)
        self._job_context.node_name = ctx.current_node
        self._job_context.job_id = ctx.job_id
        self._job_context.generation = ctx.execution_generation
        self._job_context.execution_id = ctx.execution_id
        try:
            return mounted.handler(ctx, **params)
        finally:
            self._job_context.node_name = previous_node_name
            self._job_context.job_id = previous_job_id
            self._job_context.generation = previous_generation
            self._job_context.execution_id = previous_execution_id

    def _invoke_handler_with_timeout(
        self,
        mounted,
        ctx: JobContext,
        params: dict,
        watch,
    ):
        """Invoke one handler with at most one abandonable handler thread.

        The current runner worker is the controller. Untimed programmatic jobs
        execute directly. Timeout-supervised or actively restartable CLI jobs
        run the user handler in exactly one daemon thread; the controller waits
        for completion, a centralized deadline, or a changed execution lease.
        """
        supervisor = self.scheduler_supervisor

        if in_fiber_runtime():
            def check_cancelled() -> None:
                ctx.raise_if_cancelled()
            try:
                with cancellation_scope(check_cancelled), network_attempt_context(self, ctx, watch):
                    result = self._call_mounted_handler(mounted, ctx, params)
                check_cancelled()
            except BaseException as error:
                restart_error = supervisor.execution_cancel_error(watch)
                timeout_error = supervisor.timeout_error(watch)
                final_error = restart_error or timeout_error or error
                state = (
                    "superseded" if restart_error is not None
                    else "timed_out" if timeout_error is not None
                    else "failed"
                )
                supervisor.finish_attempt(watch, state=state, error=final_error)
                raise final_error
            else:
                supervisor.signal_handler_complete(watch)
                supervisor.finish_attempt(watch, state="completed")
                return result

        if not watch.abandonable:
            try:
                result = self._call_mounted_handler(mounted, ctx, params)
            except BaseException as error:
                supervisor.finish_attempt(watch, state="failed", error=error)
                raise
            else:
                supervisor.finish_attempt(watch, state="completed")
                return result

        outcomes: Queue = Queue(maxsize=1)

        def target():
            try:
                outcomes.put(("result", self._call_mounted_handler(mounted, ctx, params)))
            except BaseException as error:
                outcomes.put(("error", error))
            finally:
                supervisor.signal_handler_complete(watch)
                self.storage.close_thread_connection()

        Thread(
            target=target,
            name=f"mwf-handler-{ctx.current_node}-{ctx.job_id}-{mounted.name}",
            daemon=True,
        ).start()

        # The centralized supervisor checks all active execution leases with one
        # SQLite query. Per-controller polling caused thousands of identical
        # reads whenever a large API wave was in flight.
        watch.wake_event.wait()

        restart_error = supervisor.execution_cancel_error(watch)
        if restart_error is not None:
            supervisor.finish_attempt(watch, state="superseded", error=restart_error)
            raise restart_error

        timeout_error = supervisor.timeout_error(watch)
        if timeout_error is not None:
            supervisor.finish_attempt(watch, state="timed_out", error=timeout_error)
            raise timeout_error

        kind, payload = outcomes.get()
        if kind == "error":
            supervisor.finish_attempt(watch, state="failed", error=payload)
            raise payload

        supervisor.finish_attempt(watch, state="completed")
        return payload

    def execute_with_fallbacks(
        self,
        job: Job,
        *,
        execution_generation: int,
        execution_id: str | None,
    ):
        node = self.nodes[job.node_name]
        assert node.main_task is not None

        try:
            return self.execute_mounted_task(
                job,
                node.main_task,
                execution_generation=execution_generation,
                execution_id=execution_id,
            )

        except JobRestartedError:
            raise
        except Exception as main_error:
            terminal_error = main_error
            self.check_job_execution(
                job.node_name,
                job.job_id,
                execution_generation,
                execution_id,
            )
            self.storage.write_debug(
                job.node_name,
                f"job {job.job_id} main task failed: {main_error}",
            )

            for fallback_name in node.fallback_order:
                fallback = node.fallbacks[fallback_name]

                self.storage.write_debug(
                    job.node_name,
                    f"job {job.job_id} trying fallback {fallback_name}",
                )
                self.storage.append_job_event(
                    job.node_name,
                    job.job_id,
                    "fallback_started",
                    fallback=fallback_name,
                    previous_error=repr(main_error),
                )

                try:
                    return self.execute_mounted_task(
                        job,
                        fallback,
                        previous_error=main_error,
                        execution_generation=execution_generation,
                        execution_id=execution_id,
                    )

                except JobRestartedError:
                    raise
                except Exception as fallback_error:
                    terminal_error = fallback_error
                    self.check_job_execution(
                        job.node_name,
                        job.job_id,
                        execution_generation,
                        execution_id,
                    )
                    self.storage.write_debug(
                        job.node_name,
                        f"job {job.job_id} fallback {fallback_name} failed: {fallback_error}",
                    )

            if terminal_error is main_error:
                raise main_error
            raise terminal_error from main_error

    def execute_mounted_task(
        self,
        job: Job,
        mounted,
        previous_error: Exception | None = None,
        *,
        execution_generation: int,
        execution_id: str | None,
    ):
        attempts = mounted.retries + 1
        all_results = []

        for attempt in range(1, attempts + 1):
            try:
                repeat_results = []

                for repeat_index in range(1, mounted.repeats + 1):
                    # Validate invocation inputs before registering a scheduler
                    # watch. A malformed job must not leave an orphan deadline
                    # that can fire after the validation error is already being
                    # handled by retry/fallback logic.
                    params = {
                        key: value
                        for key, value in job.params.items()
                        if key in mounted.allowed_params
                    }

                    if "error" in mounted.allowed_params:
                        params["error"] = previous_error

                    missing = mounted.required_params - set(params)
                    if missing:
                        raise InvalidJobError(
                            f"Missing params for {job.node_name}.{mounted.name}: {missing}"
                        )

                    cancellation_event = Event()
                    watch = self.scheduler_supervisor.create_attempt(
                        node_name=job.node_name,
                        job_id=job.job_id,
                        task_name=mounted.name,
                        attempt=attempt,
                        repeat_index=repeat_index,
                        generation=execution_generation,
                        execution_id=execution_id,
                        cancellation_event=cancellation_event,
                        total_timeout=mounted.timeout,
                        checkpoint_timeout=mounted.checkpoint_timeout,
                        force_abandonable=(
                            self.active_job_restart_enabled and execution_id is not None
                        ),
                    )
                    ctx = JobContext(
                        system=self,
                        current_node=job.node_name,
                        current_job=job,
                        current_task=mounted.name,
                        attempt=attempt,
                        repeat_index=repeat_index,
                        error=previous_error,
                        execution_generation=execution_generation,
                        execution_id=execution_id,
                        cancellation_event=cancellation_event,
                        attempt_watch=watch,
                    )

                    result = self._invoke_handler_with_timeout(
                        mounted,
                        ctx,
                        params,
                        watch,
                    )
                    ctx.raise_if_cancelled()
                    repeat_results.append(result)

                all_results.extend(repeat_results)
                return all_results[0] if len(all_results) == 1 else all_results

            except JobRestartedError:
                raise
            except Exception as error:
                if attempt < attempts:
                    self.check_job_execution(
                        job.node_name,
                        job.job_id,
                        execution_generation,
                        execution_id,
                    )
                    self.storage.write_debug(
                        job.node_name,
                        f"job {job.job_id} retrying {mounted.name} "
                        f"attempt {attempt + 1}/{attempts}: {error}",
                    )
                    self.storage.append_job_event(
                        job.node_name,
                        job.job_id,
                        "retry_started",
                        task=mounted.name,
                        attempt=attempt + 1,
                        attempts=attempts,
                        previous_error=repr(error),
                    )
                    continue

                raise
