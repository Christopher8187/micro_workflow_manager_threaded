from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from contextlib import ExitStack
from threading import Event
from typing import Callable

from ..errors import InvalidGraphError
from ..models import CANCELLED, FAILED, RUNNING, Job
from ..storage.job_sources import LiveRefreshableQueuedJobObjectSource, PrefetchingQueuedJobObjectSource
from .admission_sources import ClaimedJob, ClaimedQueuedJobSource, StoppingJobSource
from .component_scheduler import allocate_api_pumps
from .live_start import wait_for_live_component_release
from .execution_scope import programmatic_execution
from .node_execution_group import NodeExecutionGroup
from .dag_execution_operation import DagExecutionOperation
from ..runners.direct import DirectRunner
from ..runners.threaded import ThreadedRunner
from ..runners.process import ProcessPoolRunner


class DagSchedulerMixin:
    def run(self):
        nodes = list(self.graph_obj.nodes)
        if not nodes:
            return []
        with programmatic_execution(
            self, command='run', start_node=nodes[0], nodes=nodes, include_driver=True,
        ) as (context, driver):
            return self._run(execution_context=context, _session_driver=driver)

    def _run(self, *, execution_context, _session_driver=None):
        for node_name in self.graph_obj.nodes:
            self.execution_claim_context(node_name, context=execution_context)
        return self._run_concurrently(
            execution_context=execution_context, _session_driver=_session_driver,
            _sequential=self.runner not in {'threaded', 'api', 'process'},
        )

    def run_concurrently(
        self, nodes: list[str] | None = None,
        ready_check: Callable[[str], bool] | None = None, **kwargs,
    ) -> list[str]:
        selected = list(self.graph_obj.nodes) if nodes is None else list(nodes)
        if not selected:
            return []
        with programmatic_execution(
            self, command='run_concurrently', start_node=selected[0], nodes=selected, include_driver=True,
        ) as (context, driver):
            return self._run_concurrently(
                nodes, ready_check, execution_context=context, _session_driver=driver, **kwargs,
            )

    def _run_concurrently(
        self,
        nodes: list[str] | None = None,
        ready_check: Callable[[str], bool] | None = None,
        *,
        execution_context,
        _session_driver=None,
        _operation=None,
        _sequential=False,
        refuse_after_component: tuple[str, ...] | None = None,
        refuse_before_component: tuple[str, ...] | None = None,
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
        units = self.execution_components(nodes) if _operation is None else _operation.units
        if not units:
            return []
        for unit in units:
            for node_name in unit:
                self.execution_claim_context(node_name, context=execution_context)

        if _session_driver is not None:
            operation = DagExecutionOperation(self, units, execution_context, ready_check, {
                '_sequential': _sequential,
                'refuse_after_component': refuse_after_component,
                'refuse_before_component': refuse_before_component,
                'refusal_event': refusal_event,
                'wait_deadlock_resolver': wait_deadlock_resolver,
                'wait_deadlock_blocked_components': wait_deadlock_blocked_components,
            })
            return _session_driver.drive(operation)

        def default_ready_check(node_name: str) -> bool:
            return self.node_ready(node_name)

        check = ready_check or default_ready_check
        refuse_after = (
            tuple(refuse_after_component)
            if refuse_after_component is not None
            else None
        )
        refuse_before = (
            tuple(refuse_before_component)
            if refuse_before_component is not None
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

        def refusal_before_already_reached() -> bool:
            if refuse_before is None:
                return False
            return all(
                self.node_complete(node_name)
                or self.storage.get_node_status(node_name) in {FAILED, CANCELLED}
                for node_name in refuse_before
            )

        def unit_ready(unit: tuple[str, ...]) -> bool:
            if _operation is not None:
                if _operation.is_resuming(unit):
                    return True
                if _operation.stop_admission:
                    return False
            return any(self.storage.has_queued_jobs(node_name) for node_name in unit) and all(
                check(node_name) for node_name in unit
            )

        max_workers = max(1, len(units))
        ran: list[str] = [] if _operation is None else _operation.ran
        blocked_components = wait_deadlock_blocked_components if wait_deadlock_blocked_components is not None else set()
        in_flight: set[tuple[str, ...]] = set()
        futures = {} if _operation is None else _operation.futures
        future_api_pumps: dict[object, dict[str, int]] = {}
        admission_stopped = False if _operation is None else _operation.admission_stopped

        previous_active_api_nodes = self.active_api_admission_nodes()
        self.set_active_api_admission_nodes(set())
        with ExitStack() as cleanup:
            cleanup.callback(self.set_active_api_admission_nodes, previous_active_api_nodes)
            if _operation is not None:
                # Keep an executor cleanup error before restoring admission.
                cleanup.push(_operation.retain_exit_error)
            executor = None if _sequential else cleanup.enter_context(ThreadPoolExecutor(
                max_workers=max_workers,
                thread_name_prefix="mwf-unit",
            ))
            if _operation is not None:
                # Keep the body error before executor cleanup can replace it.
                cleanup.push(_operation.retain_exit_error)
            while True:
                self.finalize_ready_nodes(skip_components=(
                    in_flight if _operation is None else in_flight | _operation.suspended_units()
                ))

                if not admission_stopped and refusal_target_terminal():
                    admission_stopped = True
                    if refusal_event is not None:
                        refusal_event.set()
                if not admission_stopped and refusal_before_already_reached():
                    admission_stopped = True
                    if refusal_event is not None:
                        refusal_event.set()

                if _operation is not None:
                    _operation.admission_stopped = admission_stopped

                ready = [
                    unit
                    for unit in units
                    if (not admission_stopped or (_operation is not None and _operation.is_resuming(unit)))
                    and unit not in in_flight
                    and unit not in blocked_components
                    and unit_ready(unit)
                ]

                # ``refuse`` is a global exclusive boundary. The instant its
                # component becomes startable, stop all new component
                # admission before constructing this wave. Components already
                # submitted in an earlier wave remain non-preemptive and are
                # joined below.
                if refuse_before is not None and refuse_before in ready:
                    admission_stopped = True
                    ready = []
                    if _operation is not None:
                        _operation.admission_stopped = True
                    if refusal_event is not None:
                        refusal_event.set()

                # A later DAG branch may become ready while an independent
                # component from an earlier wave is still running. Retain those
                # pumps in the global accounting; otherwise every wave could
                # independently consume the whole host budget. Existing pumps
                # are non-preemptive, so newly ready components receive the
                # remaining budget. A component starts only when every one of
                # its API members can receive the guaranteed first pump.
                active_api_pumps = {
                    node_name: pumps
                    for allocation in future_api_pumps.values()
                    for node_name, pumps in allocation.items()
                }
                active_api_nodes = {
                    node_name: self.requested_max_threads(node_name)
                    for node_name in active_api_pumps
                }
                ready_api_by_unit = {
                    unit: {
                        node_name: self.requested_max_threads(node_name)
                        for node_name in unit
                        if (self.nodes[node_name].runner_override or self.runner) == "api"
                    }
                    for unit in ready
                }
                possible_api_nodes = dict(active_api_nodes)
                for limits in ready_api_by_unit.values():
                    possible_api_nodes.update(limits)
                target_pump_total = sum(allocate_api_pumps(possible_api_nodes).values())
                available_pumps = max(0, target_pump_total - sum(active_api_pumps.values()))

                launch: list[tuple[str, ...]] = []
                launch_api_nodes: dict[str, int] = {}
                skipped_api_unit = False
                for unit in ready:
                    unit_api_nodes = ready_api_by_unit[unit]
                    if len(unit_api_nodes) > available_pumps:
                        skipped_api_unit = True
                        continue
                    launch.append(unit)
                    launch_api_nodes.update(unit_api_nodes)
                    available_pumps -= len(unit_api_nodes)

                if launch_api_nodes:
                    # If every ready API component fits, use all remaining host
                    # capacity. If one must wait, keep unused pumps available
                    # rather than converting them into non-preemptive extras.
                    launch_budget = (
                        len(launch_api_nodes)
                        if skipped_api_unit
                        else target_pump_total - sum(active_api_pumps.values())
                    )
                    wave_api_pumps = allocate_api_pumps(
                        launch_api_nodes,
                        pump_budget=launch_budget,
                    )
                else:
                    wave_api_pumps = {}

                self.set_active_api_admission_nodes(
                    set(active_api_pumps) | set(launch_api_nodes)
                )

                for unit in launch:
                    unit_api_pumps = {
                        node_name: wave_api_pumps[node_name]
                        for node_name in ready_api_by_unit[unit]
                    }
                    if _sequential:
                        try:
                            if _operation is None:
                                ran.extend(self._run_component(
                                    set(unit), True, wait_deadlock_resolver, unit_api_pumps,
                                    execution_context=execution_context,
                                ))
                            else:
                                _operation.component_operation(unit, unit_api_pumps).run()
                                _operation.record_outcome(unit)
                        except BaseException as error:
                            if _operation is not None:
                                _operation.record_outcome(unit, error=error)
                            raise
                        if refuse_after is not None and unit == refuse_after:
                            admission_stopped = True
                            if _operation is not None:
                                _operation.admission_stopped = True
                            if refusal_event is not None:
                                refusal_event.set()
                            break
                        continue
                    if _operation is None:
                        future = executor.submit(
                            self._run_component, set(unit), True, wait_deadlock_resolver, unit_api_pumps,
                            execution_context=execution_context,
                        )
                    else:
                        future = executor.submit(_operation.component_operation(unit, unit_api_pumps).run)
                    futures[future] = unit
                    future_api_pumps[future] = unit_api_pumps
                    in_flight.add(unit)

                if _sequential:
                    if not launch:
                        break
                    continue
                if not futures:
                    break

                done, _ = wait(futures, return_when=FIRST_COMPLETED)

                for future in done:
                    unit = futures[future]
                    future_api_pumps.pop(future, None)
                    in_flight.remove(unit)
                    self.set_active_api_admission_nodes({
                        node_name
                        for allocation in future_api_pumps.values()
                        for node_name in allocation
                    })

                    if _operation is None and refuse_after is not None and unit == refuse_after:
                        admission_stopped = True
                        if refusal_event is not None:
                            refusal_event.set()

                    try:
                        unit_result = future.result()
                        if _operation is None:
                            ran.extend(unit_result)
                        else:
                            _operation.record_outcome(unit)
                            if refuse_after is not None and unit == refuse_after:
                                admission_stopped = True
                                _operation.admission_stopped = True
                                if refusal_event is not None:
                                    refusal_event.set()
                        futures.pop(future)
                    except BaseException as error:
                        if _operation is not None:
                            _operation.record_outcome(unit, error=error)
                        futures.pop(future)
                        for pending in futures:
                            pending.cancel()
                        wait(futures)
                        raise

        self.finalize_ready_nodes(
            skip_components=None if _operation is None else _operation.suspended_units(),
        )
        return ran

    def run_node(self, node_name: str, ignore_readiness: bool = False):
        with programmatic_execution(
            self, command='run_node', start_node=node_name, nodes=[node_name], include_driver=True,
        ) as (context, driver):
            return self._run_node(
                node_name, ignore_readiness, execution_context=context, _session_driver=driver,
            )

    def _run_node(
        self, node_name: str, ignore_readiness: bool = False, *, execution_context, _session_driver=None,
    ):
        self.execution_claim_context(node_name, context=execution_context)
        component = self.component_for(node_name)
        if not ignore_readiness and not self.component_ready(component):
            raise InvalidGraphError(f"Hoeflein component {sorted(component)} is not ready yet")

        # The programmatic API follows the same semantics as ``mwf run NODE``:
        # naming any member of a Hoeflein component pumps the whole component.
        if len(component) > 1 or self.component_is_cyclic(component):
            return self._run_component(
                component, ignore_readiness=True, execution_context=execution_context,
                _session_driver=_session_driver,
            )

        self.storage.set_node_status(node_name, RUNNING)
        return self._run_queued_node_jobs(
            node_name=node_name, ignore_readiness=True, execution_context=execution_context,
            _session_driver=_session_driver,
        )

    def run_queued_node_jobs(
        self, node_name: str, ignore_readiness: bool = False, **kwargs,
    ):
        with programmatic_execution(
            self, command='run_queued_node_jobs', start_node=node_name, nodes=[node_name], include_driver=True,
        ) as (context, driver):
            return self._run_queued_node_jobs(
                node_name, ignore_readiness, execution_context=context, _session_driver=driver, **kwargs,
            )

    def _run_queued_node_jobs(
        self,
        node_name: str,
        ignore_readiness: bool = False,
        *,
        execution_context,
        _session_driver=None,
        _stop_event: Event | None = None,
        _live_until_event: Event | None = None,
        _live_ready_event: Event | None = None,
        _live_start_event: Event | None = None,
        _defer_final_status_refresh: bool = False,
        _api_startup_lanes: int | None = None,
        _component_operation=None,
        _restart_job_ids=None,
    ):
        """Run all currently queued jobs for one node using a lazy job source."""
        self.execution_claim_context(node_name, context=execution_context)
        if not ignore_readiness and not self.node_ready(node_name):
            raise InvalidGraphError(f"Node {node_name} is not ready yet")

        node = self.nodes[node_name]

        if _restart_job_ids == []:
            return []

        if (_restart_job_ids is None and not self.storage.has_queued_jobs(node_name)
                and _live_until_event is None):
            self.refresh_node_status(node_name, allow_complete=True)
            return []

        self.storage.set_node_status(node_name, RUNNING)
        job_source = None
        try:
            runner = self.make_runner(
                node, execution_context=execution_context, api_startup_lanes=_api_startup_lanes,
            )

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
            if _restart_job_ids is not None:
                refreshable = False
                job_source = [self.storage.load_job(node_name, job_id) if preloaded else job_id
                              for job_id in _restart_job_ids]
            elif preloaded:
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
                    session_id, component = self.execution_claim_context(node_name, context=execution_context)
                    job_source = ClaimedQueuedJobSource(
                        self.storage,
                        node_name,
                        job_source,
                        session_id=session_id,
                        component=component,
                        task_started_data=task_started_data,
                        required_params=(main_task.required_params if main_task is not None else None),
                        allowed_params=(main_task.allowed_params if main_task is not None else None),
                        on_abandoned=(None if _component_operation is None else _component_operation.record_abandoned),
                    )
            elif refreshable:
                job_source = self.storage.queued_job_source(node_name)
            else:
                job_source = self.storage.iter_queued_job_ids(node_name)

            if _stop_event is not None:
                job_source = StoppingJobSource(job_source, _stop_event)

            wait_for_live_component_release(
                _live_ready_event, _live_start_event, _stop_event
            )

            def execute_source_item(item, *, _expected_restart=None):
                try:
                    if isinstance(item, ClaimedJob):
                        return self._run_job(
                            execution_context=execution_context,
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
                        return self._run_job(
                            execution_context=execution_context,
                            _expected_restart=_expected_restart,
                            node_name=item.node_name,
                            job_id=item.job_id,
                            ignore_readiness=True,
                            _preloaded_job=item,
                            _defer_node_status_refresh=True,
                        )
                    return self._run_job(
                        execution_context=execution_context,
                        _expected_restart=_expected_restart,
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

            def item_job_id(item):
                return item.job.job_id if isinstance(item, ClaimedJob) else (
                    item.job_id if isinstance(item, Job) else item
                )

            def record_outcome(item, *, value=None, error=None):
                if _component_operation is not None:
                    _component_operation.record_outcome(
                        node_name, item_job_id(item), value=value, error=error,
                    )

            def expected_restart(item):
                return (None if _component_operation is None else
                        _component_operation.expected_restart(node_name, item_job_id(item)))

            def run_source_item(item, *, _expected_restart=None):
                # Release an unstarted preclaim when a sibling stopped admission.
                if _stop_event is not None and _stop_event.is_set():
                    abandon = getattr(item, "abandon_unstarted", None)
                    if callable(abandon):
                        abandon()
                    return None
                if _component_operation is not None and _expected_restart is None:
                    _expected_restart = expected_restart(item)
                try:
                    value = execute_source_item(item, _expected_restart=_expected_restart)
                except BaseException as error:
                    record_outcome(item, error=error)
                    raise
                record_outcome(item, value=value)
                return value

            if isinstance(runner, ProcessPoolRunner) and _component_operation is not None:
                result = runner.run_job_source(
                    node_name=node_name, job_source=job_source, run_one=run_source_item,
                    on_outcome=record_outcome, get_restart_expectation=expected_restart,
                )
            elif not refreshable and _stop_event is None and isinstance(runner, (DirectRunner, ThreadedRunner, ProcessPoolRunner)):
                operation = NodeExecutionGroup(
                    self.storage, node_name, execution_context, job_source,
                )
                result = (_session_driver.drive(operation, runner, run_source_item)
                          if _session_driver is not None else operation.run(runner, run_source_item))
            else:
                result = runner.run_job_source(
                    node_name=node_name,
                    job_source=job_source,
                    run_one=run_source_item,
                )
        except BaseException:
            if _stop_event is not None:
                _stop_event.set()
            # Do not perform crash-recovery scans here. Already-started jobs are
            # joined by their runner and publish through the normal SQLite lane;
            # output-backed recovery is an explicit first step of mwf resume.
            if _component_operation is None and (_session_driver is None or not _session_driver.finished):
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
        self, node_name: str, jobs: list[Job], ignore_readiness: bool = False,
    ):
        jobs = list(jobs)
        if any(job.node_name != node_name for job in jobs):
            raise ValueError('All selected jobs must belong to the requested node')
        if not jobs:
            return []
        with programmatic_execution(
            self, command='run_node_jobs', start_node=node_name, nodes=[node_name],
            selected_jobs=[job.job_id for job in jobs], include_driver=True,
        ) as (context, driver):
            return self._run_node_jobs(
                node_name, jobs, ignore_readiness, execution_context=context, _session_driver=driver,
            )

    def _run_node_jobs(
        self,
        node_name: str,
        jobs: list[Job],
        ignore_readiness: bool = False,
        *, execution_context, _session_driver=None,
    ):
        """Run a specific list of jobs from one node.

        This is the shared implementation for normal node runs and the CLI's
        job-selection mode. The supplied jobs are the only jobs executed; other
        queued jobs on the same node are left untouched.
        """
        self.execution_claim_context(node_name, context=execution_context)
        if any(job.node_name != node_name for job in jobs):
            raise ValueError('All selected jobs must belong to the requested node')
        if not ignore_readiness and not self.node_ready(node_name):
            raise InvalidGraphError(f"Node {node_name} is not ready yet")

        node = self.nodes[node_name]

        if not jobs:
            self.refresh_node_status(node_name, allow_complete=True)
            return []

        self.storage.set_node_status(node_name, RUNNING)

        runner = self.make_runner(node, execution_context=execution_context)

        try:
            def run_selected_job(job, *, _expected_restart=None):
                return self._run_job(
                    execution_context=execution_context,
                    _expected_restart=_expected_restart,
                    node_name=job.node_name,
                    job_id=job.job_id,
                    ignore_readiness=True,
                    _preloaded_job=job,
                    _defer_node_status_refresh=True,
                )
            if isinstance(runner, (DirectRunner, ThreadedRunner, ProcessPoolRunner)):
                operation = NodeExecutionGroup(
                    self.storage, node_name, execution_context, jobs,
                )
                result = (_session_driver.drive(operation, runner, run_selected_job)
                          if _session_driver is not None else operation.run(runner, run_selected_job))
            else:
                result = runner.run_jobs(node_name=node_name, jobs=jobs, run_one=run_selected_job)

        except Exception:
            if _session_driver is None or not _session_driver.finished:
                self.storage.set_node_status(node_name, FAILED)
            raise

        self.refresh_node_status(node_name, allow_complete=True)

        return result

    def run_jobs(
        self, node_name: str, job_ids: list[int], ignore_readiness: bool = False,
    ):
        if not job_ids:
            return []
        with programmatic_execution(
            self, command='run_jobs', start_node=node_name, nodes=[node_name], selected_jobs=job_ids,
            include_driver=True,
        ) as (context, driver):
            return self._run_jobs(
                node_name, job_ids, ignore_readiness, execution_context=context, _session_driver=driver,
            )

    def _run_jobs(
        self,
        node_name: str,
        job_ids: list[int],
        ignore_readiness: bool = False,
        *, execution_context, _session_driver=None,
    ):
        """Run selected job IDs from one node.

        Unlike run_node(...), this does not gather every queued job. It loads the
        exact job IDs requested by the caller and runs only those jobs.
        """
        if not job_ids: return []
        jobs = [self.storage.load_job(node_name, job_id) for job_id in job_ids]
        return self._run_node_jobs(
            execution_context=execution_context,
            _session_driver=_session_driver,
            node_name=node_name,
            jobs=jobs,
            ignore_readiness=ignore_readiness,
        )
