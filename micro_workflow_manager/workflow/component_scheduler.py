from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from threading import Event

import networkx as nx

from ..errors import InvalidGraphError
from ..models import FAILED, QUEUED, RUNNING, WAITING


WAIT_BLOCKING_JOB_STATUSES = {QUEUED, RUNNING, FAILED}
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
                    self.refresh_component_status(component_set)
                    return ran
                try:
                    for node_name in startable:
                        self.storage.set_node_status(node_name, RUNNING)
                        self.run_queued_node_jobs(node_name, ignore_readiness=True)
                        ran.append(node_name)
                except Exception:
                    self.mark_component_failed(component_set)
                    raise
                continue

            # Keep at most one node pump active per component member. A failure
            # stops new admission, but already-started node pumps and jobs are
            # joined before the component is marked failed.
            executor = ThreadPoolExecutor(
                max_workers=max(1, len(component_nodes)),
                thread_name_prefix="mwf-hoeflein-node",
            )
            stop_event = Event()

            def run_node_worker(node_name: str):
                try:
                    return self.run_queued_node_jobs(
                        node_name,
                        True,
                        _stop_event=stop_event,
                    )
                finally:
                    self.storage.close_thread_connection()

            futures = {}
            active_nodes: set[str] = set()
            first_error = None
            wake_event = Event()
            unsubscribe = self.storage.subscribe_state_changes(wake_event.set)
            try:
                while True:
                    done = [future for future in futures if future.done()]
                    for future in done:
                        node_name = futures.pop(future)
                        active_nodes.discard(node_name)
                        try:
                            future.result()
                        except Exception as error:
                            if first_error is None:
                                first_error = error
                            stop_event.set()
                            break
                        ran.append(node_name)
                    if first_error is not None:
                        break

                    queued_set = self.storage.queued_nodes(component_nodes)
                    blocking_nodes = self._component_wait_blockers(component_nodes)
                    startable = self._waiting_startable_nodes(
                        component_nodes,
                        active_nodes=active_nodes,
                        queued_nodes=queued_set,
                        blocking_nodes=blocking_nodes,
                    )
                    if startable:
                        self.storage.set_node_statuses({
                            node_name: RUNNING for node_name in startable
                        })
                    for node_name in startable:
                        future = executor.submit(run_node_worker, node_name)
                        futures[future] = node_name
                        active_nodes.add(node_name)
                        future.add_done_callback(lambda _future: wake_event.set())

                    if not futures:
                        queued_nodes = [
                            node_name
                            for node_name in component_nodes
                            if node_name in queued_set
                        ]
                        if not queued_nodes:
                            self.refresh_component_status(
                                component_set,
                                allow_complete=True,
                            )
                            return ran
                        self.refresh_component_status(component_set)
                        return ran

                    # Once every component member already owns a live pump, no
                    # durable job transition can make another node startable. Do
                    # not turn thousands of per-job state events into redundant
                    # component-wide queue scans; the next scheduler decision is
                    # needed only when one pump exits. This is still event-driven
                    # (the worker Future is the event) and removes the largest
                    # high-concurrency startup reader storm.
                    if len(active_nodes) == len(component_nodes):
                        wait(tuple(futures), return_when=FIRST_COMPLETED)
                        continue

                    wake_event.clear()
                    # Close the clear/wait race: a completion or a durable queue
                    # change observed after clear must loop without sleeping.
                    if any(future.done() for future in futures):
                        continue
                    latest_queued = self.storage.queued_nodes(component_nodes)
                    if latest_queued - active_nodes != queued_set - active_nodes:
                        continue
                    wake_event.wait(self.component_queue_poll_seconds)
            finally:
                unsubscribe()
                if first_error is not None:
                    stop_event.set()
                    for future in futures:
                        future.cancel()
                    executor.shutdown(wait=True, cancel_futures=True)
                    self.mark_component_failed(component_set)
                else:
                    executor.shutdown(wait=True)
            if first_error is not None:
                raise first_error
