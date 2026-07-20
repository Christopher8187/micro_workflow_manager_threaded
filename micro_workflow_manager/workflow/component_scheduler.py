from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait

import networkx as nx

from ..errors import InvalidGraphError
from ..models import RUNNING


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

    def run_component(
        self,
        component: set[str] | tuple[str, ...] | list[str],
        ignore_readiness: bool = False,
    ) -> list[str]:
        """Pump one Hoeflein component until it is quiescent.

        Every original edge inside the component behaves as autostart, whether or
        not the individual ``add`` call repeats ``autostart=True``. A failure in
        any node marks the entire component failed and prevents another batch.
        """
        component_set = set(component)
        if not component_set:
            return []
        if not ignore_readiness and not self.component_ready(component_set):
            raise InvalidGraphError(f"Hoeflein component {sorted(component_set)} is not ready yet")

        ran: list[str] = []
        component_nodes = list(self.component_key(component_set))

        while True:
            queued_nodes = [
                node_name for node_name in component_nodes
                if self.storage.has_queued_jobs(node_name)
            ]
            if not queued_nodes:
                self.refresh_component_status(component_set, allow_complete=True)
                return ran

            for node_name in component_nodes:
                self.storage.set_node_status(node_name, RUNNING)

            if self.runner == "direct":
                try:
                    for node_name in queued_nodes:
                        self.run_queued_node_jobs(node_name, ignore_readiness=True)
                        ran.append(node_name)
                except Exception:
                    self.mark_component_failed(component_set)
                    raise
                continue

            # A cyclic component is a live work graph, not a sequence of static
            # batches. A node pump may exhaust the queue snapshot it saw while
            # another still-running node is creating more jobs for it. Keep at
            # most one pump active per node, and immediately resubmit a node as
            # soon as its previous pump exits and new queued work is visible.
            # This lets explode handlers continue alongside the explode router
            # instead of waiting for the router's entire queue to drain.
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
            first_error = None
            try:
                while True:
                    for node_name in component_nodes:
                        if (
                            node_name not in active_nodes
                            and self.storage.has_queued_jobs(node_name)
                        ):
                            future = executor.submit(run_node_worker, node_name)
                            futures[future] = node_name
                            active_nodes.add(node_name)

                    if not futures:
                        # Rescan after all completed workers have published their
                        # downstream jobs. If no work remains, the component is
                        # genuinely quiescent.
                        if self.component_has_queued_jobs(component_set):
                            continue
                        self.refresh_component_status(
                            component_set,
                            allow_complete=True,
                        )
                        return ran

                    done, _ = wait(
                        futures,
                        timeout=self.component_queue_poll_seconds,
                        return_when=FIRST_COMPLETED,
                    )
                    if not done:
                        # A still-running node may have just created work for an
                        # idle sibling. Poll the component queues while pumps are
                        # active rather than waiting for one whole node pump to
                        # finish before discovering that work.
                        continue
                    for future in done:
                        node_name = futures.pop(future)
                        active_nodes.discard(node_name)
                        try:
                            future.result()
                        except Exception as error:
                            first_error = error
                            # Publish failure before waiting for already-running
                            # sibling node pumps to wind down, so monitor cannot
                            # keep presenting a dead component as running.
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
                    # A sibling pump may have refreshed component state while
                    # it was winding down. Reassert the terminal component
                    # failure after every active pump has stopped.
                    self.mark_component_failed(component_set)
                else:
                    executor.shutdown(wait=True)
            if first_error is not None:
                raise first_error
