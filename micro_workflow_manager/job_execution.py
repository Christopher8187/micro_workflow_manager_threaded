from pathlib import Path
from time import perf_counter

from .context import JobContext
from .errors import InvalidGraphError, InvalidJobError, JobFailedError
from .models import CANCELLED, DONE, FAILED, QUEUED, RUNNING, SKIPPED, Job, now


class JobExecutionMixin:
    def run_job(
        self,
        node_name: str,
        job_id: int,
        ignore_readiness: bool = False,
    ):
        if not ignore_readiness and not self.node_ready(node_name):
            raise InvalidGraphError(f"Node {node_name} is not ready yet")

        job = self.storage.load_job(node_name, job_id)
        node = self.nodes[node_name]

        if node.main_task is None:
            raise InvalidJobError(f"Node {node_name} has no mounted task")

        started_at = now()
        started_perf = perf_counter()
        self.storage.set_job_status(node_name, job_id, RUNNING, started_at=started_at)

        try:
            previous_node_name = getattr(self._job_context, "node_name", None)
            previous_job_id = getattr(self._job_context, "job_id", None)
            self._job_context.node_name = node_name
            self._job_context.job_id = job_id
            try:
                result = self.execute_with_fallbacks(job)
            finally:
                self._job_context.node_name = previous_node_name
                self._job_context.job_id = previous_job_id

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
                },
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

    def execute_with_fallbacks(self, job: Job):
        node = self.nodes[job.node_name]
        assert node.main_task is not None

        try:
            return self.execute_mounted_task(job, node.main_task)

        except Exception as main_error:
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

                try:
                    return self.execute_mounted_task(
                        job,
                        fallback,
                        previous_error=main_error,
                    )

                except Exception as fallback_error:
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
    ):
        attempts = mounted.retries + 1
        all_results = []

        for attempt in range(1, attempts + 1):
            try:
                repeat_results = []

                for repeat_index in range(1, mounted.repeats + 1):
                    ctx = JobContext(
                        system=self,
                        current_node=job.node_name,
                        current_job=job,
                        current_task=mounted.name,
                        attempt=attempt,
                        repeat_index=repeat_index,
                        error=previous_error,
                    )

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

                    result = mounted.handler(ctx, **params)
                    repeat_results.append(result)

                all_results.extend(repeat_results)
                return all_results[0] if len(all_results) == 1 else all_results

            except Exception as error:
                if attempt < attempts:
                    self.storage.write_debug(
                        job.node_name,
                        f"job {job.job_id} retrying {mounted.name} "
                        f"attempt {attempt + 1}/{attempts}: {error}",
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
        self.storage.set_job_status(node_name, job_id, QUEUED)
        self.storage.set_node_status(node_name, QUEUED)

    def skip_node(self, node_name: str):
        self.storage.set_node_status(node_name, SKIPPED)

    def mark_node_done(self, node_name: str):
        self.storage.set_node_status(node_name, DONE)

    def input_dir(self, node_name: str) -> Path:
        return self.storage.node_input_dir(node_name)

    def output_dir(self, node_name: str) -> Path:
        return self.storage.node_output_dir(node_name)
