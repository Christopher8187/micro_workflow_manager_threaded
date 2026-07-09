from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait

import networkx as nx

from ..errors import InvalidGraphError
from ..models import RUNNING


class ComponentSchedulerMixin:
    def component_key(self, component: set[str]) -> tuple[str, ...]:
        node_order = {node_name: index for index, node_name in enumerate(self.graph_obj.nodes)}
        return tuple(sorted(component, key=lambda item: node_order.get(item, 10**9)))

    def execution_components(self, nodes: list[str] | None = None) -> list[tuple[str, ...]]:
        """Return unique SCC execution units in component-topological order.

        A strongly connected component is one scheduler-owned unit. This is the
        game-engine/entity-spawn rule for cyclic autostart graphs: jobs may
        create more jobs in any node of the component, and the component is
        pumped until it is quiescent.
        """
        selected = set(self.graph_obj.nodes if nodes is None else nodes)
        if not selected:
            return []

        components = [set(component) for component in nx.strongly_connected_components(self.graph_obj)]
        component_by_node = {
            node_name: index
            for index, component in enumerate(components)
            for node_name in component
        }

        component_graph = nx.DiGraph()
        component_graph.add_nodes_from(range(len(components)))

        for start, end in self.graph_obj.edges:
            start_component = component_by_node[start]
            end_component = component_by_node[end]
            if start_component != end_component:
                component_graph.add_edge(start_component, end_component)

        units: list[tuple[str, ...]] = []
        seen: set[int] = set()

        for component_index in nx.topological_sort(component_graph):
            component = components[component_index]
            if not component.intersection(selected):
                continue
            if component_index in seen:
                continue
            seen.add(component_index)
            units.append(self.component_key(component))

        return units

    def component_has_queued_jobs(self, component: set[str]) -> bool:
        return any(self.storage.has_queued_jobs(node_name) for node_name in component)

    def run_component(
        self,
        component: set[str] | tuple[str, ...] | list[str],
        ignore_readiness: bool = False,
    ) -> list[str]:
        """Pump one SCC/component until it has no queued jobs left.

        This is deliberately not recursive autostart. Child jobs spawned by a
        running job are queued as new entities, then picked up by this component
        pump after the current worker returns. The component is marked complete
        only when all jobs across all nodes in the SCC are terminal.
        """
        component_set = set(component)
        if not component_set:
            return []

        if not ignore_readiness and not self.component_ready(component_set):
            raise InvalidGraphError(
                f"Component {sorted(component_set)} is not ready yet"
            )

        ran: list[str] = []
        component_nodes = list(self.component_key(component_set))

        while True:
            queued_nodes = [
                node_name
                for node_name in component_nodes
                if self.storage.has_queued_jobs(node_name)
            ]

            if not queued_nodes:
                self.refresh_component_status(component_set, allow_complete=True)
                return ran

            for node_name in queued_nodes:
                self.storage.set_node_status(node_name, RUNNING)

            if self.runner == "direct":
                for node_name in queued_nodes:
                    self.run_queued_node_jobs(node_name, ignore_readiness=True)
                    ran.append(node_name)
                continue

            max_workers = max(1, len(queued_nodes))
            futures = {}
            with ThreadPoolExecutor(
                max_workers=max_workers,
                thread_name_prefix="mwf-component-node",
            ) as executor:
                for node_name in queued_nodes:
                    futures[executor.submit(
                        self.run_queued_node_jobs,
                        node_name,
                        True,
                    )] = node_name

                done, not_done = wait(futures, return_when=FIRST_COMPLETED)

                first_error = None
                while done:
                    for future in done:
                        node_name = futures.pop(future)
                        try:
                            future.result()
                        except Exception as error:
                            first_error = error
                            break
                        ran.append(node_name)

                    if first_error is not None:
                        for pending in not_done:
                            pending.cancel()
                        wait(not_done)
                        raise first_error

                    if not futures:
                        break

                    done, not_done = wait(futures, return_when=FIRST_COMPLETED)
