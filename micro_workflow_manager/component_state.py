import networkx as nx

from .models import (
    CANCELLED,
    DONE,
    FAILED,
    NODE_COMPLETE_STATUSES,
    QUEUED,
    RUNNING,
    SKIPPED,
)


class ComponentStateMixin:
    def node_complete(self, node_name: str) -> bool:
        return self.storage.get_node_status(node_name) in NODE_COMPLETE_STATUSES

    def strongly_connected_components(self) -> list[set[str]]:
        return [set(component) for component in nx.strongly_connected_components(self.graph_obj)]

    def component_for(self, node_name: str) -> set[str]:
        for component in self.strongly_connected_components():
            if node_name in component:
                return component
        return {node_name}

    def component_is_cyclic(self, component: set[str]) -> bool:
        return len(component) > 1 or any(
            self.graph_obj.has_edge(node_name, node_name)
            for node_name in component
        )

    def component_predecessors(self, component: set[str]) -> set[str]:
        predecessors: set[str] = set()

        for node_name in component:
            predecessors.update(self.graph_obj.predecessors(node_name))

        return predecessors - component

    def component_ready(self, component: set[str]) -> bool:
        return all(self.node_complete(node) for node in self.component_predecessors(component))

    def component_has_any_jobs(self, component: set[str]) -> bool:
        return any(self.storage.list_jobs(node_name) for node_name in component)

    def refresh_component_status(self, component: set[str], allow_complete: bool = False):
        """Refresh a strongly connected component as one communicating class.

        This uses the per-node job index instead of list_jobs(), so refreshing a
        cyclic autostart component is O(number of nodes) rather than repeatedly
        scanning thousands of job folders after every small routing update.
        """
        component = set(component)
        counts_by_node = {
            node_name: self.storage.job_status_counts(node_name)
            for node_name in component
        }
        totals_by_node = {
            node_name: sum(counts.values())
            for node_name, counts in counts_by_node.items()
        }
        total_jobs = sum(totals_by_node.values())

        if total_jobs == 0:
            for node_name in component:
                current_status = self.storage.get_node_status(node_name)
                if current_status in {DONE, FAILED, CANCELLED, SKIPPED}:
                    continue
                self.storage.set_node_status(node_name, QUEUED)
            return

        if any(counts.get(FAILED, 0) for counts in counts_by_node.values()):
            for node_name in component:
                self.storage.set_node_status(node_name, FAILED)
            return

        has_running_or_queued = any(
            counts.get(RUNNING, 0) or counts.get(QUEUED, 0)
            for counts in counts_by_node.values()
        )
        if has_running_or_queued:
            for node_name, counts in counts_by_node.items():
                if counts.get(RUNNING, 0):
                    self.storage.set_node_status(node_name, RUNNING)
                else:
                    self.storage.set_node_status(node_name, QUEUED)
            return

        successful_terminal = {DONE, SKIPPED}
        all_terminal_success = all(
            totals_by_node[node_name] > 0
            and sum(counts_by_node[node_name].get(status, 0) for status in successful_terminal) == totals_by_node[node_name]
            for node_name in component
        )

        if all_terminal_success:
            if allow_complete and self.component_ready(component):
                for node_name in component:
                    self.storage.set_node_status(node_name, DONE)
            else:
                for node_name in component:
                    self.storage.set_node_status(node_name, QUEUED)
            return

        for node_name in component:
            self.storage.set_node_status(node_name, QUEUED)

    def node_ready(self, node_name: str) -> bool:
        return self.component_ready(self.component_for(node_name))

    def refresh_node_status(self, node_name: str, allow_complete: bool = False):
        """Refresh a node or cyclic component without unsafe early completion.

        Uses the per-node job index rather than list_jobs(), avoiding a full
        status-file scan every time a node or component is considered for
        readiness/finalization.
        """
        component = self.component_for(node_name)

        if self.component_is_cyclic(component):
            self.refresh_component_status(component, allow_complete=allow_complete)
            return

        counts = self.storage.job_status_counts(node_name)
        total = sum(counts.values())

        if total == 0:
            current_status = self.storage.get_node_status(node_name)
            if current_status in {DONE, FAILED, CANCELLED, SKIPPED}:
                return
            self.storage.set_node_status(node_name, QUEUED)
            return

        if counts.get(FAILED, 0):
            self.storage.set_node_status(node_name, FAILED)
            return

        if counts.get(RUNNING, 0):
            self.storage.set_node_status(node_name, RUNNING)
            return

        if counts.get(QUEUED, 0):
            self.storage.set_node_status(node_name, QUEUED)
            return

        successful = counts.get(DONE, 0) + counts.get(SKIPPED, 0)
        if successful == total:
            if allow_complete and self.node_ready(node_name):
                self.storage.set_node_status(node_name, DONE)
            else:
                self.storage.set_node_status(node_name, QUEUED)
            return

        self.storage.set_node_status(node_name, QUEUED)

    def finalize_ready_nodes(self):
        for node_name in self.graph_obj.nodes:
            if self.node_ready(node_name):
                self.refresh_node_status(node_name, allow_complete=True)

    def ready_nodes(self) -> list[str]:
        self.finalize_ready_nodes()
        ready = []

        for node_name in self.graph_obj.nodes:
            if self.storage.has_queued_jobs(node_name) and self.node_ready(node_name):
                ready.append(node_name)

        return ready
