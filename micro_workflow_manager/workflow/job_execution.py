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


T = TypeVar("T")


class JobExecutionMixin:
    restart_poll_interval_seconds = 0.05

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

    def _execute_job_attempt_in_thread(
        self,
        job: Job,
        generation: int,
        execution_id: str,
    ) -> tuple[Event, Queue]:
        """Start one handler attempt in an abandonable daemon thread.

        Python cannot safely force-kill an arbitrary thread that may be inside a
        third-party HTTP/C call. The supervisor can, however, stop waiting for a
        superseded generation immediately and start the replacement generation.
        The abandoned thread is fenced from all final commits and guarded
        JobContext side effects.
        """
        done = Event()
        outcomes: Queue = Queue(maxsize=1)

        def target():
            previous_node_name = getattr(self._job_context, "node_name", None)
            previous_job_id = getattr(self._job_context, "job_id", None)
            previous_generation = getattr(self._job_context, "generation", None)
            previous_execution_id = getattr(self._job_context, "execution_id", None)
            self._job_context.node_name = job.node_name
            self._job_context.job_id = job.job_id
            self._job_context.generation = generation
            self._job_context.execution_id = execution_id

            try:
                result = self.execute_with_fallbacks(
                    job,
                    execution_generation=generation,
                    execution_id=execution_id,
                )
            except BaseException as error:
                outcomes.put(("error", error))
            else:
                outcomes.put(("result", result))
            finally:
                self._job_context.node_name = previous_node_name
                self._job_context.job_id = previous_job_id
                self._job_context.generation = previous_generation
                self._job_context.execution_id = previous_execution_id
                done.set()

        Thread(
            target=target,
            name=f"mwf-attempt-{job.node_name}-{job.job_id}-g{generation}",
            daemon=True,
        ).start()
        return done, outcomes

    def _run_job_unfenced(
        self,
        node_name: str,
        job_id: int,
    ):
        """Run a job through the original low-overhead execution path.

        This path is used for normal programmatic MicroWorkflow calls. The CLI
        enables the generation-fenced supervisor only while it owns an active
        run/runfrom sequence.
        """
        job = self.storage.load_job(node_name, job_id)
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

            if self.storage.get_node_status(node_name) != RUNNING:
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

            if self.storage.get_node_status(node_name) != RUNNING:
                self.refresh_node_status(node_name, allow_complete=False)
            raise JobFailedError(f"Job {node_name}/{job_id} failed") from error

    def run_job(
        self,
        node_name: str,
        job_id: int,
        ignore_readiness: bool = False,
    ):
        if not ignore_readiness and not self.node_ready(node_name):
            raise InvalidGraphError(f"Node {node_name} is not ready yet")

        node = self.nodes[node_name]
        if node.main_task is None:
            raise InvalidJobError(f"Node {node_name} has no mounted task")

        if not self.active_job_restart_enabled:
            return self._run_job_unfenced(node_name, job_id)

        # A manual restart increments the generation. The same scheduler-owned
        # run_job call notices that fence, abandons the old handler thread, and
        # immediately loops into the replacement generation. This lets an
        # existing run/runfrom sequence continue without a competing CLI run.
        while True:
            job = self.storage.load_job(node_name, job_id)
            started_at = now()
            started_perf = perf_counter()
            generation, execution_id = self.storage.claim_job_execution(
                node_name,
                job_id,
                started_at=started_at,
            )
            done, outcomes = self._execute_job_attempt_in_thread(
                job,
                generation,
                execution_id,
            )

            superseded = False
            while not done.wait(self.restart_poll_interval_seconds):
                if not self.storage.job_execution_is_current(
                    node_name,
                    job_id,
                    generation,
                    execution_id,
                ):
                    superseded = True
                    break

            if superseded:
                self.scheduler_supervisor.cancel_execution(
                    node_name,
                    job_id,
                    generation,
                    execution_id,
                    reason=f"Job {node_name}/{job_id} generation {generation} was restarted",
                )
                self.storage.write_debug(
                    node_name,
                    f"job {job_id} generation {generation} superseded; "
                    "starting the requested replacement",
                )
                continue

            outcome_kind, payload = outcomes.get()

            try:
                with self.storage.guard_job_execution(
                    node_name,
                    job_id,
                    generation,
                    execution_id,
                ):
                    if outcome_kind == "result":
                        result = payload
                        stored_files = self.storage.store_returned_files(
                            node_name,
                            job_id,
                            result,
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
                        self.storage.set_job_status(
                            node_name,
                            job_id,
                            DONE,
                            started_at=started_at,
                            finished_at=now(),
                            duration_seconds=round(perf_counter() - started_perf, 6),
                            generation=generation,
                            execution_id=execution_id,
                        )
                    else:
                        error = payload
                        self.storage.write_debug(
                            node_name,
                            f"job {job_id} failed: {error}",
                        )
                        self.storage.write_output(
                            node_name,
                            job_id,
                            {
                                "status": FAILED,
                                "error": repr(error),
                                "generation": generation,
                            },
                        )
                        self.storage.set_job_status(
                            node_name,
                            job_id,
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

            if self.storage.get_node_status(node_name) != RUNNING:
                self.refresh_node_status(node_name, allow_complete=False)

            if outcome_kind == "result":
                return payload

            error = payload
            if isinstance(error, BaseException) and not isinstance(error, Exception):
                raise error
            raise JobFailedError(f"Job {node_name}/{job_id} failed") from error


    def _invoke_handler_with_timeout(
        self,
        mounted,
        ctx: JobContext,
        params: dict,
        watch,
    ):
        """Invoke one handler under the centralized scheduler supervisor.

        Untimed handlers still execute directly. A total timeout or checkpoint
        timeout opts the handler into one abandonable daemon thread, while the
        single scheduler supervisor owns every deadline and the CLI heartbeat.
        """
        supervisor = self.scheduler_supervisor

        if not watch.supervised:
            try:
                result = mounted.handler(ctx, **params)
            except BaseException as error:
                supervisor.finish_attempt(watch, state="failed", error=error)
                raise
            else:
                supervisor.finish_attempt(watch, state="completed")
                return result

        outcomes: Queue = Queue(maxsize=1)

        def target():
            try:
                outcomes.put(("result", mounted.handler(ctx, **params)))
            except BaseException as error:
                outcomes.put(("error", error))
            finally:
                supervisor.signal_handler_complete(watch)

        Thread(
            target=target,
            name=f"mwf-handler-{ctx.current_node}-{ctx.job_id}-{mounted.name}",
            daemon=True,
        ).start()

        # The wake event is set by either handler completion or the one central
        # scheduler watchdog. No per-attempt timeout polling/thread is needed.
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

            raise main_error

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

    def run_one(self, node_name: str, **params):
        job = self.start(
            node_name,
            autostart=False,
            **params,
        )

        result = self.run_job(
            node_name=node_name,
            job_id=job.job_id,
            ignore_readiness=True,
        )
        self.refresh_node_status(node_name, allow_complete=True)
        return result

    def run_node_once(self, node_name: str):
        return self.run_node(
            node_name,
            ignore_readiness=True,
        )

    def list_jobs(self, node_name: str, status: str | None = None):
        return self.storage.list_jobs(node_name, status=status)

    def cancel_job(self, node_name: str, job_id: int):
        self.storage.set_job_status(node_name, job_id, CANCELLED)
        self.refresh_node_status(node_name, allow_complete=False)

    def retry_job(self, node_name: str, job_id: int):
        self.storage.request_job_restart(
            node_name,
            job_id,
            reason="retry_job API",
        )
        self.storage.set_node_status(node_name, QUEUED)

    def skip_node(self, node_name: str):
        self.storage.set_node_status(node_name, SKIPPED)

    def mark_node_done(self, node_name: str):
        self.storage.set_node_status(node_name, DONE)

    def input_dir(self, node_name: str) -> Path:
        return self.storage.node_input_dir(node_name)

    def output_dir(self, node_name: str) -> Path:
        return self.storage.node_output_dir(node_name)
