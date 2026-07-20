from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from typing import Callable

from ..errors import InvalidGraphError
from ..models import FAILED, RUNNING
from ..models import Job


class DagSchedulerMixin:
    def run(self):
        if self.runner in {"threaded", "api", "process"}:
            return self.run_concurrently()

        ran = []

        while True:
            ready = self.ready_nodes()

            if not ready:
                break

            for node_name in ready:
                self.run_node(node_name)
                ran.append(node_name)

        return ran

    def run_concurrently(
        self,
        nodes: list[str] | None = None,
        ready_check: Callable[[str], bool] | None = None,
    ) -> list[str]:
        """Run ready execution units concurrently.

        A Hoeflein component is scheduled as one execution unit, not as several
        independent node schedulers. This prevents autostart cycles such as
        A -> B -> A from starting competing schedulers that fight over the same
        queue/status files or recursively wait on child jobs.
        """
        units = self.execution_components(nodes)
        if not units:
            return []

        def default_ready_check(node_name: str) -> bool:
            return self.node_ready(node_name)

        check = ready_check or default_ready_check

        def unit_ready(unit: tuple[str, ...]) -> bool:
            return any(self.storage.has_queued_jobs(node_name) for node_name in unit) and all(
                check(node_name) for node_name in unit
            )

        max_workers = max(1, len(units))
        ran: list[str] = []
        in_flight: set[tuple[str, ...]] = set()
        futures = {}

        with ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix="mwf-unit",
        ) as executor:
            while True:
                self.finalize_ready_nodes()

                ready = [
                    unit
                    for unit in units
                    if unit not in in_flight
                    and unit_ready(unit)
                ]

                for unit in ready:
                    future = executor.submit(
                        self.run_component,
                        set(unit),
                        True,
                    )
                    futures[future] = unit
                    in_flight.add(unit)

                if not futures:
                    break

                done, _ = wait(futures, return_when=FIRST_COMPLETED)

                for future in done:
                    unit = futures.pop(future)
                    in_flight.remove(unit)

                    try:
                        ran.extend(future.result())
                    except Exception:
                        for pending in futures:
                            pending.cancel()
                        wait(futures)
                        raise

        self.finalize_ready_nodes()
        return ran

    def run_node(self, node_name: str, ignore_readiness: bool = False):
        component = self.component_for(node_name)
        if not ignore_readiness and not self.component_ready(component):
            raise InvalidGraphError(f"Hoeflein component {sorted(component)} is not ready yet")

        # The programmatic API follows the same semantics as ``mwf run NODE``:
        # naming any member of a Hoeflein component pumps the whole component.
        if len(component) > 1 or self.component_is_cyclic(component):
            return self.run_component(component, ignore_readiness=True)

        self.storage.set_node_status(node_name, RUNNING)
        return self.run_queued_node_jobs(node_name=node_name, ignore_readiness=True)

    def run_queued_node_jobs(
        self,
        node_name: str,
        ignore_readiness: bool = False,
    ):
        """Run all currently queued jobs for one node using a lazy job source."""
        if not ignore_readiness and not self.node_ready(node_name):
            raise InvalidGraphError(f"Node {node_name} is not ready yet")

        node = self.nodes[node_name]

        if not self.storage.has_queued_jobs(node_name):
            self.refresh_node_status(node_name, allow_complete=True)
            return []

        self.storage.set_node_status(node_name, RUNNING)
        runner = self.make_runner(node)

        job_source = (
            self.storage.queued_job_source(node_name)
            if getattr(runner, "supports_refreshable_job_source", False)
            else self.storage.iter_queued_job_ids(node_name)
        )

        try:
            result = runner.run_job_source(
                node_name=node_name,
                job_source=job_source,
                run_one=lambda job_id: self.run_job(
                    node_name=node_name,
                    job_id=job_id,
                    ignore_readiness=True,
                ),
            )

        except Exception:
            self.storage.set_node_status(node_name, FAILED)
            raise

        self.refresh_node_status(node_name, allow_complete=True)

        return result

    def run_node_jobs(
        self,
        node_name: str,
        jobs: list[Job],
        ignore_readiness: bool = False,
    ):
        """Run a specific list of jobs from one node.

        This is the shared implementation for normal node runs and the CLI's
        job-selection mode. The supplied jobs are the only jobs executed; other
        queued jobs on the same node are left untouched.
        """
        if not ignore_readiness and not self.node_ready(node_name):
            raise InvalidGraphError(f"Node {node_name} is not ready yet")

        node = self.nodes[node_name]

        if not jobs:
            self.refresh_node_status(node_name, allow_complete=True)
            return []

        self.storage.set_node_status(node_name, RUNNING)

        runner = self.make_runner(node)

        try:
            result = runner.run_jobs(
                node_name=node_name,
                jobs=jobs,
                run_one=lambda job: self.run_job(
                    node_name=job.node_name,
                    job_id=job.job_id,
                    ignore_readiness=True,
                ),
            )

        except Exception:
            self.storage.set_node_status(node_name, FAILED)
            raise

        self.refresh_node_status(node_name, allow_complete=True)

        return result

    def run_jobs(
        self,
        node_name: str,
        job_ids: list[int],
        ignore_readiness: bool = False,
    ):
        """Run selected job IDs from one node.

        Unlike run_node(...), this does not gather every queued job. It loads the
        exact job IDs requested by the caller and runs only those jobs.
        """
        if not job_ids:
            return []

        jobs = [self.storage.load_job(node_name, job_id) for job_id in job_ids]
        return self.run_node_jobs(
            node_name=node_name,
            jobs=jobs,
            ignore_readiness=ignore_readiness,
        )
