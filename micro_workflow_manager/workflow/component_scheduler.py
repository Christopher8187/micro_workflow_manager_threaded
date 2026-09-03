import os
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from threading import Event

import networkx as nx

from ..errors import InvalidGraphError
from ..models import FAILED, QUEUED, RUNNING, WAITING


WAIT_BLOCKING_JOB_STATUSES = {QUEUED, RUNNING, FAILED}


def allocate_api_pumps(
    limits: dict[str, int],
    *,
    logical_processors: int | None = None,
    pump_budget: int | None = None,
) -> dict[str, int]:
    """Allocate one shared controller-pump pool across active API nodes.

    Every node receives one pump. The shared budget is the smaller of (a) what
    every node would choose alone and (b) logical processors plus five, but is
    never smaller than the node count. On the 16-logical-CPU explode host this
    is 21 pumps, the measured simultaneous-node plateau.

    Remaining pumps are assigned by the marginal reduction in equally split
    controller load. For node concurrency ``n`` and current pump count ``p``,
    that benefit is ``n / (p * (p + 1))``. Greedy allocation is optimal for
    this separable diminishing-return objective. Each node is capped at its
    independently measured ``min(12, ceil(n/64))`` plateau.
    """
    names = sorted(limits)
    if not names:
        return {}
    if any(type(limits[name]) is not int or limits[name] < 1 for name in names):
        raise ValueError("API pump allocation limits must be integers >= 1")
    independent = {
        name: min(12, max(1, (limits[name] + 63) // 64))
        for name in names
    }
    independent_total = sum(independent.values())
    if pump_budget is None:
        processors = os.cpu_count() if logical_processors is None else logical_processors
        processors = 1 if processors is None else processors
        if type(processors) is not int or processors < 1:
            raise ValueError("logical processor count must be an integer >= 1")
        pump_budget = max(12, processors + 5)
    if type(pump_budget) is not int or pump_budget < 1:
        raise ValueError("API pump budget must be an integer >= 1")
    budget = max(len(names), min(independent_total, pump_budget))
    allocations = {name: 1 for name in names}
    while sum(allocations.values()) < budget:
        candidates = [name for name in names if allocations[name] < independent[name]]
        if not candidates:
            break
        # If work is evenly partitioned, controller load is w/p. The marginal
        # benefit of the next pump is w/(p*(p+1)); greedily selecting the largest
        # value solves the separable diminishing-return allocation.
        chosen = max(
            candidates,
            key=lambda name: (
                limits[name]
                / (allocations[name] * (allocations[name] + 1)),
            ),
        )
        allocations[chosen] += 1
    return allocations


class ComponentSchedulerMixin:
    # Durable lifecycle commits wake the scheduler immediately. The timeout is
    # only a defensive cross-process fallback if an external writer cannot use
    # the project event broker.
    component_queue_poll_seconds = 5.0

    def execution_components(self, nodes: list[str] | None = None) -> list[tuple[str, ...]]:
        """Return Hoeflein execution units in quotient-DAG topological order."""
        selected = set(self.graph_obj.nodes if nodes is None else nodes)
        if not selected:
            return []
        dag = self.component_dag()
        return [
            component
            for component in nx.topological_sort(dag)
            if set(component).intersection(selected)
        ]

    def component_has_queued_jobs(self, component: set[str]) -> bool:
        return any(self.storage.has_queued_jobs(node_name) for node_name in component)

    def _component_queued_nodes(self, component_nodes: list[str]) -> list[str]:
        queued = self.storage.queued_nodes(component_nodes)
        return [node_name for node_name in component_nodes if node_name in queued]

    def _component_wait_blockers(self, component_nodes: list[str]) -> set[str]:
        return self.storage.nodes_with_job_statuses(
            component_nodes,
            WAIT_BLOCKING_JOB_STATUSES,
        )

    def _finalize_failed_component(self, component: set[str], error: BaseException) -> None:
        """Publish one failed SCC only after no local runner can still mutate it.

        A hard resource failure can occur after a handler wrote its output but
        before SQLite accepted the terminal update. Recover those output-backed
        completions first. Any remaining RUNNING rows are abandoned executions:
        every node runner has already been joined at this point, so mark them
        failed with an explicit recovery reason rather than leaving a terminal
        component that still appears to own live work.
        """
        component = set(component)
        recovery_error = repr(error)

        # Normal handler failures already publish their terminal state before a
        # runner unwinds. Preserve the no-scan hot failure path in that common
        # case; recovery I/O is needed only when a joined component still has
        # stale RUNNING rows (for example after EMFILE interrupted publication).
        running_by_node = {}
        for node_name in sorted(component):
            try:
                running = self.storage.list_jobs(node_name, status=RUNNING)
            except BaseException:
                running = []
            if running:
                running_by_node[node_name] = running

        if running_by_node:
            try:
                self.storage.reconcile_terminal_outputs(component)
            except BaseException:
                # Preserve the original component error. Remaining RUNNING rows
                # are handled below once descriptor/database pressure subsides.
                pass

        for node_name in sorted(component):
            try:
                running = self.storage.list_jobs(node_name, status=RUNNING)
            except BaseException:
                continue
            for job in running:
                try:
                    self.storage.set_job_status(
                        node_name,
                        int(job["job_id"]),
                        FAILED,
                        error=recovery_error,
                        recovered_after_component_abort=True,
                    )
                except BaseException:
                    # Failure cleanup is best effort and must never replace the
                    # original error being raised to the caller.
                    pass
        self.mark_component_failed(component)

    def _waiting_startable_nodes(
        self,
        component_nodes: list[str],
        *,
        active_nodes: set[str] | None = None,
        queued_nodes: set[str] | None = None,
        blocking_nodes: set[str] | None = None,
    ) -> list[str]:
        active = active_nodes or set()
        queued = queued_nodes
        if queued is None:
            queued = self.storage.queued_nodes(component_nodes)
        blocked = blocking_nodes
        if blocked is None:
            blocked = self._component_wait_blockers(component_nodes)

        result: list[str] = []
        waiting_statuses: dict[str, str] = {}
        for node_name in component_nodes:
            if node_name in active or node_name not in queued:
                continue
            blockers = self.waiting_dependencies(node_name).intersection(blocked)
            if not blockers:
                result.append(node_name)
            else:
                waiting_statuses[node_name] = WAITING
        self.storage.set_node_statuses(waiting_statuses)
        return result

    def run_component(
        self,
        component: set[str] | tuple[str, ...] | list[str],
        ignore_readiness: bool = False,
        wait_deadlock_resolver=None,
        api_pump_allocations: dict[str, int] | None = None,
    ) -> list[str]:
        """Pump one Hoeflein component until it is quiescent.

        A waiting node is admitted only after every selected peer has no queued,
        running, or failed jobs. A pump that has already started is allowed to
        finish its current jobs; waiting is checked again before later admission.
        """
        component_set = set(component)
        if not component_set:
            return []
        if not ignore_readiness and not self.component_ready(component_set):
            raise InvalidGraphError(f"Hoeflein component {sorted(component_set)} is not ready yet")

        ran: list[str] = []
        component_nodes = list(self.component_key(component_set))

        api_nodes = {
            node_name: self.effective_max_threads(node_name)
            for node_name in component_nodes
            if (self.nodes[node_name].runner_override or self.runner) == "api"
        }
        if api_pump_allocations is None:
            api_pump_allocations = allocate_api_pumps(api_nodes)

        def resolve_wait_deadlock(queued_nodes, blocking_nodes):
            if wait_deadlock_resolver is None:
                return None
            queued = tuple(node for node in component_nodes if node in set(queued_nodes))
            blockers = {
                node: tuple(sorted(self.waiting_dependencies(node).intersection(blocking_nodes)))
                for node in queued
            }
            choice = wait_deadlock_resolver(tuple(component_nodes), queued, blockers)
            return choice if choice in queued else None

        while True:
            queued_nodes = self._component_queued_nodes(component_nodes)
            if not queued_nodes:
                self.refresh_component_status(component_set, allow_complete=True)
                return ran

            if self.runner == "direct":
                blocking_nodes = self._component_wait_blockers(component_nodes)
                startable = self._waiting_startable_nodes(
                    component_nodes,
                    queued_nodes=set(queued_nodes),
                    blocking_nodes=blocking_nodes,
                )
                if not startable:
                    override = resolve_wait_deadlock(queued_nodes, blocking_nodes)
                    if override is None:
                        self.refresh_component_status(component_set)
                        return ran
                    startable = [override]
                try:
                    for node_name in startable:
                        self.run_queued_node_jobs(
                            node_name,
                            ignore_readiness=True,
                            _defer_final_status_refresh=True,
                        )
                        ran.append(node_name)
                except Exception:
                    self.mark_component_failed(component_set)
                    raise
                continue

            # Hoeflein components are one live execution unit, not a tiny DAG.
            # Ordinary threaded/API members therefore own one resident queue pump
            # for the lifetime of the component, even while their queue is
            # temporarily empty. Only explicit waiting nodes are admitted in
            # phases. This prevents feedback cycles from accumulating durable
            # Q>0/R=0 backlogs merely because a member's previous queue slice
            # happened to drain.
            executor = ThreadPoolExecutor(
                max_workers=max(1, len(component_nodes)),
                thread_name_prefix="mwf-hoeflein-node",
            )
            stop_event = Event()
            wake_event = Event()
            live_start_event = Event()

            is_live_hoeflein = (
                len(component_nodes) > 1 or self.component_is_cyclic(component_set)
            )
            live_nodes = {
                node_name
                for node_name in component_nodes
                if is_live_hoeflein
                and not self.nodes[node_name].waiting
                and (self.nodes[node_name].runner_override or self.runner)
                in {"threaded", "api"}
            }
            live_ready_events = {
                node_name: Event()
                for node_name in live_nodes
            }

            def wake_live_sources() -> None:
                # Live sources subscribe to the same local state broker. A wake
                # after stop_event closes idle waits immediately instead of
                # making component shutdown depend on a defensive timeout.
                wake_event.set()
                self.storage.notify_queue_changes(component_nodes)

            def run_node_worker(node_name: str):
                try:
                    return self.run_queued_node_jobs(
                        node_name,
                        True,
                        _stop_event=stop_event,
                        _live_until_event=(stop_event if node_name in live_nodes else None),
                        _live_ready_event=live_ready_events.get(node_name),
                        _live_start_event=(live_start_event if node_name in live_nodes else None),
                        _defer_final_status_refresh=True,
                        _api_startup_lanes=api_pump_allocations.get(node_name),
                    )
                finally:
                    self.storage.close_thread_connection()

            futures = {}
            active_nodes: set[str] = set()
            work_nodes: set[str] = set(queued_nodes)
            first_error = None

            def submit_node(node_name: str) -> None:
                if node_name in active_nodes or stop_event.is_set():
                    return
                future = executor.submit(run_node_worker, node_name)
                futures[future] = node_name
                active_nodes.add(node_name)
                future.add_done_callback(lambda _future: wake_event.set())

            # Keep every ordinary live-capable member resident from component
            # startup. It may initially have no work; the source sleeps on the
            # state broker until a sibling publishes feedback.
            for node_name in component_nodes:
                if node_name in live_nodes:
                    submit_node(node_name)

            # Do not let a fast producer outrun a consumer that has been
            # submitted but has not yet installed its node-scoped queue
            # subscription. If source construction fails, release every peer;
            # the ordinary Future error path below publishes component failure.
            while live_ready_events and not all(
                ready.is_set() for ready in live_ready_events.values()
            ):
                if any(future.done() for future in futures):
                    break
                wake_event.wait(0.01)
                wake_event.clear()
            live_start_event.set()

            try:
                while True:
                    done = [future for future in futures if future.done()]
                    for future in done:
                        node_name = futures.pop(future)
                        active_nodes.discard(node_name)
                        try:
                            future.result()
                        except BaseException as error:
                            if first_error is None:
                                first_error = error
                            stop_event.set()
                            wake_live_sources()
                            break
                        ran.append(node_name)
                        # A resident ordinary pump should return only after the
                        # component stop event. If an implementation returns
                        # early, immediately restore the invariant rather than
                        # allowing internal queueing until the coordinator next
                        # happens to notice that node.
                        if node_name in live_nodes and not stop_event.is_set():
                            submit_node(node_name)
                    if first_error is not None:
                        break

                    queued_set = self.storage.queued_nodes(component_nodes)
                    work_nodes.update(queued_set)
                    running_job_nodes = self.storage.nodes_with_job_statuses(
                        component_nodes, {RUNNING}
                    )
                    blocking_nodes = self._component_wait_blockers(component_nodes)

                    # Explicit waiting nodes remain phase-gated. Non-live runner
                    # types also use the legacy finite pump semantics. Ordinary
                    # threaded/API nodes are already resident and are excluded.
                    startable = self._waiting_startable_nodes(
                        component_nodes,
                        active_nodes=active_nodes,
                        queued_nodes=queued_set,
                        blocking_nodes=blocking_nodes,
                    )
                    for node_name in startable:
                        submit_node(node_name)

                    # No durable work remains anywhere in the SCC. Tell resident
                    # pumps to leave their event waits, join them, and only then
                    # publish completion. A job pulled but not yet claimed still
                    # remains QUEUED in SQLite, so it prevents this branch.
                    if not queued_set and not running_job_nodes:
                        stop_event.set()
                        wake_live_sources()
                        break

                    # The only legal internal queue is a real waiting deadlock:
                    # every queued node is explicitly waiting on a blocking peer
                    # and no job is currently running that could clear it.
                    if queued_set and not running_job_nodes:
                        blocked_queued = {
                            node_name
                            for node_name in queued_set
                            if self.waiting_dependencies(node_name).intersection(blocking_nodes)
                        }
                        if blocked_queued == queued_set:
                            override = resolve_wait_deadlock(queued_set, blocking_nodes)
                            if override is None:
                                stop_event.set()
                                wake_live_sources()
                                break
                            submit_node(override)

                    # Durable per-job transitions can arrive thousands of times a
                    # second. Resident pumps consume ordinary feedback directly,
                    # so the coordinator only needs a bounded 50 ms housekeeping
                    # cadence (or an immediate node-Future wake) to gate waiting
                    # nodes and detect global quiescence. This avoids turning the
                    # state-event stream into an SQLite rescan storm.
                    wake_event.clear()
                    if any(future.done() for future in futures):
                        continue
                    wake_event.wait(0.05)
            except BaseException as error:
                if first_error is None:
                    first_error = error
                stop_event.set()
                wake_live_sources()
            finally:
                stop_event.set()
                wake_live_sources()
                if first_error is not None:
                    for future in futures:
                        future.cancel()
                    # Running node controllers join all already-started work
                    # before failure is published. This is the 0.5.3 invariant
                    # that prevents a terminal component with hundreds of live
                    # jobs still owned by one member.
                    executor.shutdown(wait=True, cancel_futures=True)
                    self._finalize_failed_component(component_set, first_error)
                else:
                    executor.shutdown(wait=True)
                    # Quiescence can become visible just before a node worker
                    # publishes its exception to its Future. Joining alone does
                    # not surface that exception, so inspect every worker before
                    # treating the component as successful.
                    for future in futures:
                        try:
                            future.result()
                        except BaseException as error:
                            first_error = error
                            break
                    if first_error is not None:
                        self._finalize_failed_component(component_set, first_error)
                    else:
                        # Resident live pumps normally finish only after the
                        # coordinator has already observed global quiescence, so
                        # their Future may not pass through the per-iteration
                        # ``done`` loop. Preserve the public run()/runfrom return
                        # behavior by reporting live members that actually owned
                        # at least one job during this component execution.
                        for node_name in component_nodes:
                            if node_name in work_nodes and node_name not in ran:
                                ran.append(node_name)
                        self.refresh_component_status(
                            component_set,
                            allow_complete=not self.component_has_queued_jobs(component_set),
                        )
            if first_error is not None:
                raise first_error
            return ran
