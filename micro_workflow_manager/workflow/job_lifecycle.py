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


class JobLifecycleMixin:
    """Execution lease lifecycle and terminal job publication."""

    def run_job_side_effect(
        self,
        node_name: str,
        job_id: int,
        generation: int,
        execution_id: str | None,
        action: Callable[[], T],
    ) -> T:
        """Run a JobContext mutation only while its execution lease is current."""
        if execution_id is None:
            return action()
        return self.storage.run_guarded_job_side_effect(
            node_name,
            job_id,
            generation,
            execution_id,
            action,
        )

    def check_job_execution(
        self,
        node_name: str,
        job_id: int,
        generation: int,
        execution_id: str | None,
    ):
        """Cheap cooperative cancellation check without a mutation lock."""
        if execution_id is None:
            return
        if not self.storage.job_execution_is_current(
            node_name,
            job_id,
            generation,
            execution_id,
        ):
            raise JobRestartedError(
                f"Job {node_name}/{job_id} generation {generation} was restarted"
            )

    def _run_job_unfenced(
        self,
        node_name: str,
        job_id: int,
        *,
        preloaded_job: Job | None = None,
        defer_node_status_refresh: bool = False,
    ):
        """Run a job through the original low-overhead execution path.

        This path is used for normal programmatic MicroWorkflow calls. The CLI
        enables the generation-fenced supervisor only while it owns an active
        run/runfrom sequence.
        """
        job = preloaded_job or self.storage.load_job(node_name, job_id)
        started_at = now()
        started_perf = perf_counter()
        self.storage.set_job_status(node_name, job_id, RUNNING, started_at=started_at)

        try:
            previous_node_name = getattr(self._job_context, "node_name", None)
            previous_job_id = getattr(self._job_context, "job_id", None)
            previous_generation = getattr(self._job_context, "generation", None)
            previous_execution_id = getattr(self._job_context, "execution_id", None)
            self._job_context.node_name = node_name
            self._job_context.job_id = job_id
            self._job_context.generation = 0
            self._job_context.execution_id = None
            try:
                result = self.execute_with_fallbacks(
                    job,
                    execution_generation=0,
                    execution_id=None,
                )
            finally:
                self._job_context.node_name = previous_node_name
                self._job_context.job_id = previous_job_id
                self._job_context.generation = previous_generation
                self._job_context.execution_id = previous_execution_id

            stored_files = self.storage.store_returned_files(node_name, job_id, result)
            self.storage.write_output(
                node_name,
                job_id,
                {
                    "status": DONE,
                    "stored_files": stored_files,
                    "result_type": type(result).__name__,
                    "result_repr": repr(result),
                },
            )
            self.storage.set_job_status(
                node_name,
                job_id,
                DONE,
                started_at=started_at,
                finished_at=now(),
                duration_seconds=round(perf_counter() - started_perf, 6),
            )

            if (
                not defer_node_status_refresh
                and self.storage.get_node_status(node_name) != RUNNING
            ):
                self.refresh_node_status(node_name, allow_complete=False)
            return result

        except Exception as error:
            self.storage.write_debug(node_name, f"job {job_id} failed: {error}")
            self.storage.write_output(
                node_name,
                job_id,
                {"status": FAILED, "error": repr(error)},
            )
            self.storage.set_job_status(
                node_name,
                job_id,
                FAILED,
                started_at=started_at,
                finished_at=now(),
                duration_seconds=round(perf_counter() - started_perf, 6),
            )

            if (
                not defer_node_status_refresh
                and self.storage.get_node_status(node_name) != RUNNING
            ):
                self.refresh_node_status(node_name, allow_complete=False)
            raise JobFailedError(f"Job {node_name}/{job_id} failed") from error

    def run_job(
        self,
        node_name: str,
        job_id: int,
        ignore_readiness: bool = False,
        *,
        _preloaded_job: Job | None = None,
        _preclaimed_execution: tuple[int, str, str, float] | None = None,
        _task_started_pre_recorded: bool = False,
        _defer_node_status_refresh: bool = False,
    ):
        if not ignore_readiness and not self.node_ready(node_name):
            raise InvalidGraphError(f"Node {node_name} is not ready yet")

        node = self.nodes[node_name]
        if node.main_task is None:
            raise InvalidJobError(f"Node {node_name} has no mounted task")

        if not self.active_job_restart_enabled:
            return self._run_job_unfenced(
                node_name,
                job_id,
                preloaded_job=_preloaded_job,
                defer_node_status_refresh=_defer_node_status_refresh,
            )

        # The runner worker is now the attempt controller. It invokes the
        # fallback/retry pipeline synchronously and creates only one extra
        # abandonable thread for the currently executing user handler. A
        # restart wakes this controller, which immediately loops into the new
        # generation while the stale handler remains fenced.
        preloaded_job = _preloaded_job
        preclaimed_execution = _preclaimed_execution
        task_started_pre_recorded = bool(_task_started_pre_recorded)
        claim_priority = (
            5 if (node.runner_override or self.runner) == "threaded" else 10
        )
        while True:
            job = preloaded_job or self.storage.load_job(node_name, job_id)
            # A restarted generation must reread the durable payload. The
            # preloaded object belongs only to this initial queued admission.
            preloaded_job = None
            if preclaimed_execution is None:
                started_at = now()
                started_perf = perf_counter()
                generation, execution_id = self.storage.claim_job_execution(
                    node_name,
                    job_id,
                    started_at=started_at,
                    priority=claim_priority,
                )
            else:
                (
                    generation,
                    execution_id,
                    started_at,
                    started_perf,
                ) = preclaimed_execution
                preclaimed_execution = None

            try:
                result = self.execute_with_fallbacks(
                    job,
                    execution_generation=generation,
                    execution_id=execution_id,
                    first_task_started_pre_recorded=task_started_pre_recorded,
                )
                outcome_kind, payload = "result", result
            except JobRestartedError as error:
                task_started_pre_recorded = False
                self.scheduler_supervisor.cancel_execution(
                    node_name,
                    job_id,
                    generation,
                    execution_id,
                    reason=str(error),
                )
                self.storage.write_debug(
                    node_name,
                    f"job {job_id} generation {generation} superseded; "
                    "starting the requested replacement",
                )
                continue
            except BaseException as error:
                outcome_kind, payload = "error", error
            finally:
                # The pre-recorded event belongs only to the initial preclaimed
                # main-task attempt. Any replacement generation records its own.
                task_started_pre_recorded = False

            try:
                if outcome_kind == "result":
                    with self.storage.guard_job_execution(
                        node_name, job_id, generation, execution_id
                    ):
                        result = payload
                        stored_files = self.storage.store_returned_files(
                            node_name, job_id, result
                        )
                        self.storage.write_output(
                            node_name,
                            job_id,
                            {
                                "status": DONE,
                                "stored_files": stored_files,
                                "result_type": type(result).__name__,
                                "result_repr": repr(result),
                                "generation": generation,
                            },
                        )
                    self.storage.finalize_job_execution(
                        node_name,
                        job_id,
                        generation,
                        execution_id,
                        DONE,
                        started_at=started_at,
                        finished_at=now(),
                        duration_seconds=round(perf_counter() - started_perf, 6),
                        generation=generation,
                        execution_id=execution_id,
                    )
                else:
                    error = payload
                    self.storage.write_debug(node_name, f"job {job_id} failed: {error}")
                    with self.storage.guard_job_execution(
                        node_name, job_id, generation, execution_id
                    ):
                        self.storage.write_output(
                            node_name,
                            job_id,
                            {
                                "status": FAILED,
                                "error": repr(error),
                                "generation": generation,
                            },
                        )
                    self.storage.finalize_job_execution(
                        node_name,
                        job_id,
                        generation,
                        execution_id,
                        FAILED,
                        started_at=started_at,
                        finished_at=now(),
                        duration_seconds=round(perf_counter() - started_perf, 6),
                        generation=generation,
                        execution_id=execution_id,
                    )
            except JobRestartedError:
                self.storage.write_debug(
                    node_name,
                    f"job {job_id} generation {generation} finished while a "
                    "restart was being prepared; stale completion discarded",
                )
                continue

            if (
                not _defer_node_status_refresh
                and self.storage.get_node_status(node_name) != RUNNING
            ):
                self.refresh_node_status(node_name, allow_complete=False)

            if outcome_kind == "result":
                return payload
            error = payload
            if isinstance(error, BaseException) and not isinstance(error, Exception):
                raise error
            raise JobFailedError(f"Job {node_name}/{job_id} failed") from error
