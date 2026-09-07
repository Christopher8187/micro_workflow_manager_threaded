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
    safe_exception_repr,
)
from ..models import CANCELLED, DONE, FAILED, QUEUED, RUNNING, SKIPPED, Job, now
from ..fibers import cancellation_scope, in_fiber_runtime
from ..networking import network_attempt_context
from ..storage.priorities import ADMISSION_PRIORITY
from .execution_scope import programmatic_execution


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

    def run_job(self, node_name: str, job_id: int, ignore_readiness: bool = False):
        with programmatic_execution(
            self, command='run_job', start_node=node_name,
            nodes=[node_name], selected_jobs=[job_id],
        ) as context:
            return self._run_job(node_name, job_id, ignore_readiness, execution_context=context)

    def _run_job(
        self,
        node_name: str,
        job_id: int,
        ignore_readiness: bool = False,
        *,
        execution_context,
        _preloaded_job: Job | None = None,
        _preclaimed_execution: tuple[int, str, str, float] | None = None,
        _task_started_pre_recorded: bool = False,
        _defer_node_status_refresh: bool = False,
        _expected_restart: dict | None = None,
    ):
        if not ignore_readiness and not self.node_ready(node_name):
            raise InvalidGraphError(f"Node {node_name} is not ready yet")

        node = self.nodes[node_name]
        if node.main_task is None:
            raise InvalidJobError(f"Node {node_name} has no mounted task")

        if execution_context is None:
            raise RuntimeError('Job execution requires an explicit native session scope')
        self.execution_claim_context(node_name, context=execution_context)

        # The runner worker is now the attempt controller. It invokes the
        # fallback/retry pipeline synchronously and creates only one extra
        # abandonable thread for the currently executing user handler. A
        # restart wakes this controller, which immediately loops into the new
        # generation while the stale handler remains fenced.
        preloaded_job = _preloaded_job
        preclaimed_execution = _preclaimed_execution
        task_started_pre_recorded = bool(_task_started_pre_recorded)
        claim_priority = ADMISSION_PRIORITY
        while True:
            job = preloaded_job or self.storage.load_job(node_name, job_id)
            # A restarted generation must reread the durable payload. The
            # preloaded object belongs only to this initial queued admission.
            preloaded_job = None
            if preclaimed_execution is None:
                started_at = now()
                started_perf = perf_counter()
                session_id, component = self.execution_claim_context(node_name, context=execution_context)
                generation, execution_id = self.storage.claim_job_execution(
                    node_name,
                    job_id,
                    started_at=started_at,
                    priority=claim_priority,
                    session_id=session_id,
                    component=component,
                    expected_restart=_expected_restart,
                )
                _expected_restart = None
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
                        self.storage.write_output(
                            node_name,
                            job_id,
                            {
                                "status": DONE,
                                "result_type": type(result).__name__,
                                "result_repr": repr(result),
                                "generation": generation,
                                "execution_id": execution_id,
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
                                "error": safe_exception_repr(error),
                                "generation": generation,
                                "execution_id": execution_id,
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
            except BaseException as error:
                # The handler's claim must survive an output or terminal-write
                # failure even if restart removes it from the running rows
                # before component cleanup begins.
                error.execution_attempt = (node_name, job_id, generation, execution_id)
                raise

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
            failure = JobFailedError(f"Job {node_name}/{job_id} failed")
            failure.execution_attempt = (node_name, job_id, generation, execution_id)
            raise failure from error
