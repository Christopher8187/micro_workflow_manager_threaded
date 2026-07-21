from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import dataclass
from time import perf_counter
from threading import Event
from typing import Callable

from ..errors import InvalidGraphError
from ..models import FAILED, RUNNING, Job, now


@dataclass(slots=True)
class _ClaimedJob:
    storage: object
    job: Job
    generation: int
    execution_id: str
    started_at: str
    started_perf: float

    def abandon_unstarted(self) -> None:
        self.storage.release_unstarted_job_execution(
            self.job.node_name,
            self.job.job_id,
            self.generation,
            self.execution_id,
        )


class _ClaimedQueuedJobSource:
    """Preload and claim each refreshable API admission burst atomically."""

    def __init__(self, storage, node_name: str, source):
        self.storage = storage
        self.node_name = node_name
        self.source = source

    def pull(self, max_items: int) -> list[_ClaimedJob]:
        jobs = self.source.pull(max_items)
        if not jobs:
            return []
        started_at = now()
        started_perf = perf_counter()
        leases = self.storage.claim_job_executions_batch(
            self.node_name,
            [job.job_id for job in jobs],
            started_at=started_at,
        )
        return [
            _ClaimedJob(
                storage=self.storage,
                job=job,
                generation=generation,
                execution_id=execution_id,
                started_at=started_at,
                started_perf=started_perf,
            )
            for job, (generation, execution_id) in zip(jobs, leases)
        ]




class _StoppingJobSource:
    """Stop a node pump from admitting more jobs after a component failure."""

    def __init__(self, source, stop_event: Event):
        self.source = source
        self.stop_event = stop_event

    def pull(self, max_items: int):
        if self.stop_event.is_set():
            return []
        pull = getattr(self.source, "pull", None)
        if not callable(pull):
            raise TypeError("wrapped source does not support pull")
        return pull(max_items)

    def __iter__(self):
        for item in self.source:
            if self.stop_event.is_set():
                return
            yield item


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
        *,
        _stop_event: Event | None = None,
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

        refreshable = bool(
            getattr(runner, "supports_refreshable_job_source", False)
        )
        preloaded = bool(getattr(runner, "prefers_preloaded_jobs", False))
        if preloaded:
            job_source = self.storage.queued_job_object_source(
                node_name,
                refreshable=refreshable,
            )
            if (
                refreshable
                and self.active_job_restart_enabled
                and getattr(runner, "preclaims_job_bursts", False)
            ):
                job_source = _ClaimedQueuedJobSource(
                    self.storage,
                    node_name,
                    job_source,
                )
        elif refreshable:
            job_source = self.storage.queued_job_source(node_name)
        else:
            job_source = self.storage.iter_queued_job_ids(node_name)

        if _stop_event is not None:
            job_source = _StoppingJobSource(job_source, _stop_event)

        def run_source_item(item):
            # A sibling node may fail after this item was pulled but before its
            # handler starts. Leave ordinary items queued and release API jobs
            # that were preclaimed as part of an admission burst.
            if _stop_event is not None and _stop_event.is_set():
                abandon = getattr(item, "abandon_unstarted", None)
                if callable(abandon):
                    abandon()
                return None

            try:
                if isinstance(item, _ClaimedJob):
                    return self.run_job(
                        node_name=item.job.node_name,
                        job_id=item.job.job_id,
                        ignore_readiness=True,
                        _preloaded_job=item.job,
                        _preclaimed_execution=(
                            item.generation,
                            item.execution_id,
                            item.started_at,
                            item.started_perf,
                        ),
                    )
                if isinstance(item, Job):
                    return self.run_job(
                        node_name=item.node_name,
                        job_id=item.job_id,
                        ignore_readiness=True,
                        _preloaded_job=item,
                    )
                return self.run_job(
                    node_name=node_name,
                    job_id=item,
                    ignore_readiness=True,
                )
            except BaseException:
                # The failed job has already published its terminal state.
                # Stop sibling admission before this node pump unwinds to the
                # component scheduler.
                if _stop_event is not None:
                    _stop_event.set()
                raise

        try:
            result = runner.run_job_source(
                node_name=node_name,
                job_source=job_source,
                run_one=run_source_item,
            )
        except Exception:
            if _stop_event is not None:
                _stop_event.set()
            # Do not perform crash-recovery scans here. Already-started jobs are
            # joined by their runner and publish through the normal SQLite lane;
            # output-backed recovery is an explicit first step of mwf resume.
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
                    _preloaded_job=job,
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
