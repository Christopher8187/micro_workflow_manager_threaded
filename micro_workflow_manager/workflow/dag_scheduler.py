from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from threading import Event
from typing import Callable

from ..errors import InvalidGraphError
from ..models import CANCELLED, FAILED, RUNNING, Job
from .admission_sources import ClaimedJob, ClaimedQueuedJobSource, StoppingJobSource
from ..storage.job_sources import (
    LiveRefreshableQueuedJobObjectSource,
    PrefetchingQueuedJobObjectSource,
)


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
        *,
        refuse_after_component: tuple[str, ...] | None = None,
        refusal_event: Event | None = None,
        wait_deadlock_resolver=None,
        wait_deadlock_blocked_components: set[tuple[str, ...]] | None = None,
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
        refuse_after = (
            tuple(refuse_after_component)
            if refuse_after_component is not None
            else None
        )

        def refusal_target_terminal() -> bool:
            if refuse_after is None:
                return False
            return all(
                self.node_complete(node_name)
                or self.storage.get_node_status(node_name) in {FAILED, CANCELLED}
                for node_name in refuse_after
            )

        def unit_ready(unit: tuple[str, ...]) -> bool:
            return any(self.storage.has_queued_jobs(node_name) for node_name in unit) and all(
                check(node_name) for node_name in unit
            )

        max_workers = max(1, len(units))
        ran: list[str] = []
        blocked_components = wait_deadlock_blocked_components if wait_deadlock_blocked_components is not None else set()
        in_flight: set[tuple[str, ...]] = set()
        futures = {}
        admission_stopped = False

        with ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix="mwf-unit",
        ) as executor:
            while True:
                self.finalize_ready_nodes(skip_components=in_flight)

                if not admission_stopped and refusal_target_terminal():
                    admission_stopped = True
                    if refusal_event is not None:
                        refusal_event.set()

                ready = [] if admission_stopped else [
                    unit
                    for unit in units
                    if unit not in in_flight
                    and unit not in blocked_components
                    and unit_ready(unit)
                ]

                for unit in ready:
                    future = executor.submit(
                        self.run_component,
                        set(unit),
                        True,
                        wait_deadlock_resolver,
                    )
                    futures[future] = unit
                    in_flight.add(unit)

                if not futures:
                    break

                done, _ = wait(futures, return_when=FIRST_COMPLETED)

                for future in done:
                    unit = futures.pop(future)
                    in_flight.remove(unit)

                    if refuse_after is not None and unit == refuse_after:
                        admission_stopped = True
                        if refusal_event is not None:
                            refusal_event.set()

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
        _live_until_event: Event | None = None,
        _defer_final_status_refresh: bool = False,
    ):
        """Run all currently queued jobs for one node using a lazy job source."""
        if not ignore_readiness and not self.node_ready(node_name):
            raise InvalidGraphError(f"Node {node_name} is not ready yet")

        node = self.nodes[node_name]

        if not self.storage.has_queued_jobs(node_name) and _live_until_event is None:
            self.refresh_node_status(node_name, allow_complete=True)
            return []

        self.storage.set_node_status(node_name, RUNNING)
        runner = self.make_runner(node)

        refreshable = bool(
            getattr(runner, "supports_refreshable_job_source", False)
        )
        if (
            refreshable
            and getattr(runner, "refreshable_only_when_live", False)
            and _live_until_event is None
        ):
            # Ordinary DAG threaded nodes keep the original finite snapshot
            # source. Refreshability is needed only for resident Hoeflein pumps;
            # enabling it globally adds rowid/live-source overhead to fast DAG
            # fan-out such as idimage -> merge/organize/... .
            refreshable = False
        preloaded = bool(getattr(runner, "prefers_preloaded_jobs", False))
        if preloaded:
            job_source = self.storage.queued_job_object_source(
                node_name,
                refreshable=refreshable,
            )
            if _live_until_event is not None:
                if not refreshable:
                    raise RuntimeError(
                        f"runner for live Hoeflein node {node_name} does not support a refreshable job source"
                    )
                job_source = LiveRefreshableQueuedJobObjectSource(
                    self.storage, job_source, _live_until_event
                )
            if getattr(runner, "prefetches_job_bursts", False):
                # API runners retain their one-lane prefetch behavior. Threaded
                # runners use a bounded multi-batch background loader so a
                # worker never holds its shared source lock while 64 payload
                # files are read from disk.
                if not refreshable or getattr(runner, "startup_lanes", lambda: 1)() == 1:
                    prefetch_workers = getattr(
                        runner, "job_prefetch_workers", lambda: 1
                    )()
                    prefetch_batches = getattr(
                        runner, "job_prefetch_batches", lambda: 1
                    )()
                    job_source = PrefetchingQueuedJobObjectSource(
                        self.storage,
                        job_source,
                        prefetch_workers=prefetch_workers,
                        prefetch_batches=prefetch_batches,
                    )
            if (
                refreshable
                and self.active_job_restart_enabled
                and getattr(runner, "preclaims_job_bursts", False)
            ):
                main_task = self.nodes[node_name].main_task
                task_started_data = None
                if main_task is not None:
                    task_started_data = {
                        "task": main_task.name,
                        "task_role": "main",
                        "attempt": 1,
                        "repeat_index": 1,
                        "previous_error": None,
                    }
                job_source = ClaimedQueuedJobSource(
                    self.storage,
                    node_name,
                    job_source,
                    task_started_data=task_started_data,
                    required_params=(main_task.required_params if main_task is not None else None),
                    allowed_params=(main_task.allowed_params if main_task is not None else None),
                )
        elif refreshable:
            job_source = self.storage.queued_job_source(node_name)
        else:
            job_source = self.storage.iter_queued_job_ids(node_name)

        if _stop_event is not None:
            job_source = StoppingJobSource(job_source, _stop_event)

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
                if isinstance(item, ClaimedJob):
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
                        _task_started_pre_recorded=item.task_started_recorded,
                        _defer_node_status_refresh=True,
                    )
                if isinstance(item, Job):
                    return self.run_job(
                        node_name=item.node_name,
                        job_id=item.job_id,
                        ignore_readiness=True,
                        _preloaded_job=item,
                        _defer_node_status_refresh=True,
                    )
                return self.run_job(
                    node_name=node_name,
                    job_id=item,
                    ignore_readiness=True,
                    _defer_node_status_refresh=True,
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
        finally:
            close_source = getattr(job_source, "close", None)
            if callable(close_source):
                close_source()

        if _live_until_event is None and not _defer_final_status_refresh:
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
                    _defer_node_status_refresh=True,
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
