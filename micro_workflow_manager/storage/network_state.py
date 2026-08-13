from __future__ import annotations
from typing import Any

class NetworkStateStorageMixin:
    """Batched observability snapshots from the process-wide NetworkManager."""

    def publish_network_manager_snapshot(self, rows: list[dict[str, Any]], updated_at: float) -> None:
        manager = next(
            (row.get("_manager") for row in rows if isinstance(row.get("_manager"), dict)),
            None,
        )
        if manager is not None:
            self.atomic_write_json(
                self.project_dir / ".mwf" / "network_manager.json",
                {"updated_at": float(updated_at), **manager},
            )
        normalized = []
        for row in rows:
            node = self.validate_node_name(str(row.get("node_name") or ""))
            normalized.append((node, int(row.get("submitted", 0)), int(row.get("dispatched", 0)),
                int(row.get("completed", 0)), int(row.get("failed", 0)), int(row.get("bytes_received", 0)),
                int(row.get("in_flight", 0)), int(row.get("peak_in_flight", 0)),
                float(row.get("max_ingress_delay_seconds", 0.0)), float(row.get("max_request_seconds", 0.0)),
                float(row.get("average_request_seconds", 0.0)), row.get("last_error"), float(updated_at)))
        if not normalized: return
        def operation(connection):
            connection.executemany(
                "INSERT INTO network_state(node_name,submitted,dispatched,completed,failed,bytes_received,in_flight,peak_in_flight,max_ingress_delay_seconds,max_request_seconds,average_request_seconds,last_error,updated_at) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(node_name) DO UPDATE SET "
                "submitted=excluded.submitted,dispatched=excluded.dispatched,completed=excluded.completed,failed=excluded.failed,bytes_received=excluded.bytes_received,in_flight=excluded.in_flight,"
                "peak_in_flight=excluded.peak_in_flight,max_ingress_delay_seconds=excluded.max_ingress_delay_seconds,"
                "max_request_seconds=excluded.max_request_seconds,average_request_seconds=excluded.average_request_seconds,last_error=excluded.last_error,updated_at=excluded.updated_at",
                normalized)
        self.submit_db_mutation(operation, wait=False, priority=30)

    def network_manager_state(self):
        rows = self.db_connection().execute(
            "SELECT node_name,submitted,dispatched,completed,failed,bytes_received,in_flight,peak_in_flight,max_ingress_delay_seconds,max_request_seconds,average_request_seconds,last_error,updated_at FROM network_state ORDER BY node_name"
        ).fetchall()
        return {str(row["node_name"]): dict(row) for row in rows}
