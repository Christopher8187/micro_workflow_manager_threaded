"""Remove current raw-node rows after an authorized graph update."""

from __future__ import annotations

from .preparation_guards import refuse_receiver_mutation


class NodeStateDeletionStorageMixin:
    def delete_node_state(self, node_name: str) -> None:
        node_name = self.validate_node_name(node_name)
        with self.db_transaction() as connection:
            self._delete_node_state(connection, node_name)

    @staticmethod
    def _delete_node_state(connection, node_name):
        refuse_receiver_mutation(connection, node_name)
        connection.execute("DELETE FROM idempotency WHERE node_name=?", (node_name,))
        connection.execute("DELETE FROM default_job_specs WHERE node_name=?", (node_name,))
        connection.execute("DELETE FROM job_events WHERE node_name=?", (node_name,))
        connection.execute("DELETE FROM jobs WHERE node_name=?", (node_name,))
        connection.execute("DELETE FROM job_sequences WHERE node_name=?", (node_name,))
        connection.execute("DELETE FROM nodes WHERE node_name=?", (node_name,))
