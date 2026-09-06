from threading import Event, Lock

from ..errors import InvalidGraphError
from ..models import FAILED, RUNNING, Job
from ..storage.job_sources import LiveRefreshableQueuedJobObjectSource, PrefetchingQueuedJobObjectSource
from ..storage.component_states import ComponentExecutionIncomplete
from .admission_sources import ClaimedJob, ClaimedQueuedJobSource, StoppingJobSource
from .live_start import wait_for_live_component_release
from .execution_scope import programmatic_execution
from .node_execution_group import NodeExecutionGroup
from ..runners.direct import DirectRunner
from ..runners.threaded import ThreadedRunner
from ..runners.process import ProcessPoolRunner


class NodeSchedulerMixin:
    def run_node(self, node_name: str, ignore_readiness: bool = False):
        with programmatic_execution(
            self, command='run_node', start_node=node_name, nodes=[node_name], include_driver=True, include_parent=True,
            fresh=True,
        ) as (context, driver, parent):
            return self._run_node(
                node_name, ignore_readiness, execution_context=context, _session_driver=driver,
                _task_parent=parent,
            )

    def _run_node(
        self, node_name: str, ignore_readiness: bool = False, *, execution_context, _session_driver=None,
        _task_parent=None,
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
                _task_parent=_task_parent,
            )

        return self._run_queued_node_jobs(
            node_name=node_name, ignore_readiness=True, execution_context=execution_context,
            _session_driver=_session_driver, _full_component=True,
            _task_parent=_task_parent,
        )

    def run_queued_node_jobs(
        self, node_name: str, ignore_readiness: bool = False, **kwargs,
    ):
        with programmatic_execution(
            self, command='run_queued_node_jobs', start_node=node_name, nodes=[node_name],
            include_driver=True, include_parent=True,
        ) as (context, driver, parent):
            return self._run_queued_node_jobs(
                node_name, ignore_readiness, execution_context=context, _session_driver=driver, **kwargs,
                _full_component=len(context[1][node_name]) == 1,
                _task_parent=parent,
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
        _full_component=False,
        _task_parent=None,
    ):
        """Run all currently queued jobs for one node using a lazy job source."""
        self.execution_claim_context(node_name, context=execution_context)
        if not ignore_readiness and not self.node_ready(node_name):
            raise InvalidGraphError(f"Node {node_name} is not ready yet")

        node = self.nodes[node_name]
        completion = (self.begin_full_component_execution(
            (node_name,), execution_context=execution_context, task_parent=_task_parent,
        ) if _full_component else None)
        completed_values = {}
        admitted_order = {}
        values_lock = Lock()

        def finish_component():
            if completion is not None:
                if _session_driver is not None:
                    _session_driver.finish_component(completion, task_parent=_task_parent)
                else:
                    self.storage.finish_successful_component_execution(
                        execution_context[0], completion, task_parent=_task_parent,
                    )

        if (_restart_job_ids == [] or (
                _restart_job_ids is None and not self.storage.has_queued_jobs(node_name)
                and _live_until_event is None)):
            finish_component()
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
                if isinstance(error, ComponentExecutionIncomplete) and getattr(error, 'execution_attempt', None) is None:
                    # Admission refused before this job acquired an execution.
                    # Its queued state belongs to a later ordinary epoch.
                    return
                if completion is not None and error is None:
                    with values_lock:
                        completed_values[item_job_id(item)] = value
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
                if completion is not None:
                    with values_lock:
                        admitted_order.setdefault(item_job_id(item), None)
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
                    component_completion=completion,
                )
                result = (_session_driver.drive(operation, runner, run_source_item)
                          if _session_driver is not None else operation.run(runner, run_source_item))
            else:
                result = runner.run_job_source(
                    node_name=node_name,
                    job_source=job_source,
                    run_one=run_source_item,
                )
        except BaseException as error:
            if (completion is not None and _session_driver is not None
                    and isinstance(error, ComponentExecutionIncomplete)
                    and getattr(error, 'execution_attempt', None) is None):
                # A refreshable source can refuse a claim after earlier jobs
                # returned. Retain their source order before closing the source
                # and arbitrating the unfinished full component below.
                result = [completed_values[job_id] for job_id in admitted_order if job_id in completed_values]
            else:
                if _stop_event is not None:
                    _stop_event.set()
                # Joined workers publish through the normal SQLite lane.
                # Output-backed recovery belongs to explicit mwf resume.
                if _component_operation is None and (_session_driver is None or not _session_driver.finished):
                    self.storage.set_node_status(node_name, FAILED)
                raise
        finally:
            close_source = getattr(job_source, "close", None)
            if callable(close_source):
                close_source()

        if _live_until_event is None and not _defer_final_status_refresh:
            self.refresh_node_status(node_name, allow_complete=True)

        finish_component()
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
