from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait

import networkx as nx

from ..errors import InvalidGraphError
from ..models import QUEUED, RUNNING, WAITING


class ComponentSchedulerMixin:
    component_queue_poll_seconds = 0.10

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
        return [
            node_name
            for node_name in component_nodes
            if self.storage.has_queued_jobs(node_name)
        ]

    def _waiting_startable_nodes(
        self,
        component_nodes: list[str],
        *,
        active_nodes: set[str] | None = None,
        forced_ready: set[str] | None = None,
    ) -> list[str]:
        active = active_nodes or set()
        forced = forced_ready or set()
        result: list[str] = []
        for node_name in component_nodes:
            if node_name in active or not self.storage.has_queued_jobs(node_name):
                continue
            if node_name in forced or self.node_waiting_ready(node_name):
                result.append(node_name)
            else:
                self.storage.set_node_status(node_name, WAITING)
        return result

    def _waiting_cycle_breaker(
        self,
        component_nodes: list[str],
        queued_nodes: list[str],
    ) -> str:
        """Choose a deterministic phase leader when waiting gates form a cycle.

        Mutual waiting is intentionally useful for router/worker phase barriers.
        A resumed project may contain queued work on both sides before any pump
        is active. In that state every gate can be true simultaneously. Release
        the first queued vertex in stable component order for one pump; once it
        starts, normal waiting rules resume. This avoids an inert component while
        keeping at most one side of the waiting cycle newly admitted.
        """
        queued = set(queued_nodes)
        chosen = next(node for node in component_nodes if node in queued)
        blockers = sorted(self.waiting_blockers(chosen))
        self.storage.write_debug(
            chosen,
            "waiting-cycle bootstrap: released one pump while blocked by "
            + (", ".join(blockers) if blockers else "an empty waiting cycle"),
        )
        return chosen

    def run_component(
        self,
        component: set[str] | tuple[str, ...] | list[str],
        ignore_readiness: bool = False,
    ) -> list[str]:
        """Pump one Hoeflein component until it is quiescent.

        A waiting node is admitted only after its selected peers have no queued
        jobs left. The gate is checked before a node pump starts; a pump that is
        already active continues normally until it exhausts its own live queue.
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

            self.refresh_component_status(component_set, allow_complete=False)

            if self.runner == "direct":
                startable = self._waiting_startable_nodes(component_nodes)
                if not startable:
                    startable = [self._waiting_cycle_breaker(component_nodes, queued_nodes)]
                try:
                    for node_name in startable:
                        self.storage.set_node_status(node_name, RUNNING)
                        self.run_queued_node_jobs(node_name, ignore_readiness=True)
                        ran.append(node_name)
                except Exception:
                    self.mark_component_failed(component_set)
                    raise
                continue

            # A Hoeflein component is a live work graph. Keep at most one pump
            # active per node and discover newly queued sibling work while other
            # pumps remain active. Waiting gates affect only admission of a new
            # pump, never a pump that is already running.
            executor = ThreadPoolExecutor(
                max_workers=max(1, len(component_nodes)),
                thread_name_prefix="mwf-hoeflein-node",
            )

            def run_node_worker(node_name: str):
                try:
                    return self.run_queued_node_jobs(node_name, True)
                finally:
                    self.storage.close_thread_connection()

            futures = {}
            active_nodes: set[str] = set()
            forced_ready: set[str] = set()
            first_error = None
            try:
                while True:
                    startable = self._waiting_startable_nodes(
                        component_nodes,
                        active_nodes=active_nodes,
                        forced_ready=forced_ready,
                    )
                    for node_name in startable:
                        future = executor.submit(run_node_worker, node_name)
                        futures[future] = node_name
                        active_nodes.add(node_name)
                        forced_ready.discard(node_name)
                        self.storage.set_node_status(node_name, RUNNING)

                    if not futures:
                        queued_nodes = self._component_queued_nodes(component_nodes)
                        if not queued_nodes:
                            self.refresh_component_status(
                                component_set,
                                allow_complete=True,
                            )
                            return ran
                        # No active pump can change queue state, so all queued
                        # nodes are mutually waiting. Bootstrap one stable phase.
                        forced_ready.add(
                            self._waiting_cycle_breaker(component_nodes, queued_nodes)
                        )
                        continue

                    done, _ = wait(
                        futures,
                        timeout=self.component_queue_poll_seconds,
                        return_when=FIRST_COMPLETED,
                    )
                    if not done:
                        continue
                    for future in done:
                        node_name = futures.pop(future)
                        active_nodes.discard(node_name)
                        try:
                            future.result()
                        except Exception as error:
                            first_error = error
                            self.mark_component_failed(component_set)
                            break
                        ran.append(node_name)
                    if first_error is not None:
                        break
            finally:
                if first_error is not None:
                    for future in futures:
                        future.cancel()
                    executor.shutdown(wait=True, cancel_futures=True)
                    self.mark_component_failed(component_set)
                else:
                    executor.shutdown(wait=True)
            if first_error is not None:
                raise first_error
