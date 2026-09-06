from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from contextlib import ExitStack
from threading import Event
from typing import Callable

from ..errors import InvalidGraphError
from .component_scheduler import allocate_api_pumps
from .execution_scope import programmatic_execution
from .dag_execution_operation import DagExecutionOperation
from .node_scheduler import NodeSchedulerMixin


class DagSchedulerMixin(NodeSchedulerMixin):
    def run(self):
        nodes = list(self.graph_obj.nodes)
        if not nodes:
            return []
        with programmatic_execution(
            self, command='run', start_node=nodes[0], nodes=nodes, include_driver=True, include_parent=True,
        ) as (context, driver, parent):
            return self._run(execution_context=context, _session_driver=driver, _task_parent=parent)

    def _run(self, *, execution_context, _session_driver=None, _task_parent=None):
        for node_name in self.graph_obj.nodes:
            self.execution_claim_context(node_name, context=execution_context)
        return self._run_concurrently(
            execution_context=execution_context, _session_driver=_session_driver,
            _sequential=self.runner not in {'threaded', 'api', 'process'},
            _task_parent=_task_parent,
        )

    def run_concurrently(
        self, nodes: list[str] | None = None,
        ready_check: Callable[[str], bool] | None = None, **kwargs,
    ) -> list[str]:
        selected = list(self.graph_obj.nodes) if nodes is None else list(nodes)
        if not selected:
            return []
        with programmatic_execution(
            self, command='run_concurrently', start_node=selected[0], nodes=selected,
            include_driver=True, include_parent=True,
        ) as (context, driver, parent):
            return self._run_concurrently(
                nodes, ready_check, execution_context=context, _session_driver=driver, _task_parent=parent, **kwargs,
            )

    def _run_concurrently(
        self,
        nodes: list[str] | None = None,
        ready_check: Callable[[str], bool] | None = None,
        *,
        execution_context,
        _session_driver=None,
        _operation=None,
        _task_parent=None,
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

        if _operation is None:
            operation = DagExecutionOperation(self, units, execution_context, ready_check, {
                '_sequential': _sequential,
                'refuse_after_component': refuse_after_component,
                'refuse_before_component': refuse_before_component,
                'refusal_event': refusal_event,
                'wait_deadlock_resolver': wait_deadlock_resolver,
                'wait_deadlock_blocked_components': wait_deadlock_blocked_components,
            }, task_parent=_task_parent)
            return _session_driver.drive(operation) if _session_driver is not None else operation.run()

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

        def boundary_terminal(component) -> bool:
            if component is None:
                return False
            state = self.storage.read_component_states(
                [component], expected_shape=execution_context[2],
            )
            return state[component]['lifecycle'] in {'done', 'failed'}

        def unit_ready(unit: tuple[str, ...]) -> bool:
            if _operation is not None:
                if _operation.is_resuming(unit):
                    return True
                if _operation.stop_admission:
                    return False
            state = self.storage.get_component_state(unit)
            if state is None:
                raise RuntimeError('Selected component has no stored lifecycle')
            if state['lifecycle'] == 'running' and unit not in _operation.operations:
                parent = _operation.task_parent
                if parent is None or parent.component != unit:
                    # A nested call owns this pending execution. The session
                    # driver must consume its accepted restart before this DAG
                    # can continue from the published result.
                    return False
            needs_execution = state['lifecycle'] == 'queued' or any(
                self.storage.has_queued_jobs(node_name) for node_name in unit
            )
            return (needs_execution and self.component_ready(set(unit))
                    and (ready_check is None or all(ready_check(node_name) for node_name in unit)))

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
                if not admission_stopped and boundary_terminal(refuse_after):
                    admission_stopped = True
                    if refusal_event is not None:
                        refusal_event.set()
                if not admission_stopped and boundary_terminal(refuse_before):
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
                    if not unit_ready(unit):
                        continue
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

        return ran
