from __future__ import annotations

import asyncio
import time
from collections import Counter
from typing import Any


class NetworkDiagnosticsMixin:
    async def _state_flush_loop(self) -> None:
        while True:
            await asyncio.sleep(self._state_flush_interval)
            self._flush_state()

    def _flush_state(self) -> None:
        grouped: dict[str, list[dict[str, Any]]] = {}
        for (project, node), counter in self._stats.items():
            grouped.setdefault(project, []).append(counter.row(node))
        updated = time.time()
        dead = []
        for project, rows in grouped.items():
            reference = self._sinks.get(project)
            callback = reference() if reference is not None else None
            if callback is None:
                dead.append(project)
                continue
            try:
                diagnostic_rows = [dict(row) for row in rows]
                if diagnostic_rows:
                    diagnostic_rows[0]["_manager"] = self._manager_diagnostics(project)
                callback(diagnostic_rows, updated)
            except Exception:
                pass
        for project in dead:
            self._sinks.pop(project, None)

    async def _snapshot(self) -> dict[str, Any]:
        return {
            **self._configuration_snapshot(),
            "client_count": len(self._clients),
            "in_flight": sum(x.in_flight for x in self._clients),
            "peak_in_flight_per_client": [x.peak_in_flight for x in self._clients],
            "requests_enqueued": self._requests_enqueued,
            "ingress_wakeups": self._ingress_wakeups,
            "wakeups_per_request": (
                self._ingress_wakeups / self._requests_enqueued
                if self._requests_enqueued else 0.0
            ),
            "state_flush_interval": self._state_flush_interval,
            **self._manager_diagnostics(),
        }

    def _configuration_snapshot(self) -> dict[str, Any]:
        return {
            "architecture": self._architecture,
            "http2": self._http2,
            "requested_streams_per_connection": self._requested_streams_per_connection,
            "streams_per_connection": self._streams_per_connection,
            "http2_stream_safety_cap": self._http2_stream_safety_cap,
            "shard_capacity": self._shard_capacity(),
            "http1_connections_per_shard": self._http1_connections_per_shard,
            "tcp_keepalive": self._tcp_keepalive,
            "tcp_keepalive_idle_seconds": self._tcp_keepalive_idle_seconds,
            "tcp_keepalive_interval_seconds": self._tcp_keepalive_interval_seconds,
            "tcp_keepalive_probes": self._tcp_keepalive_probes,
            "json_terminal_grace_seconds": self._json_terminal_grace_seconds,
            "retired_shards": self._retired_shards,
            "json_stream_recoveries": self._json_stream_recoveries,
            "cohort_stall_seconds": self._cohort_stall_seconds,
            "cohort_terminal_evidence": self._cohort_terminal_evidence,
            "cohort_retry_limit": self._cohort_retry_limit,
            "cohort_stream_retries": self._cohort_stream_retries,
            "transport_error_retry_limit": self._transport_error_retry_limit,
            "transport_error_retries": self._transport_error_retries,
            "recovery_shard_reuses": self._recovery_shard_reuses,
            "recovery_shards_created": self._recovery_shards_created,
        }

    def _manager_diagnostics(self, project_key: str | None = None) -> dict[str, Any]:
        now_value = time.monotonic()
        active = [
            item for item in self._active_requests.values()
            if project_key is None or item.get("project_key") == project_key
        ]
        active_by_shard: dict[int, list[dict[str, Any]]] = {}
        for item in active:
            shard_id = item.get("shard_id")
            if isinstance(shard_id, int):
                active_by_shard.setdefault(shard_id, []).append(item)
        shards = []
        for shard in self._clients:
            # Group once above.  The previous nested scan was O(clients ×
            # requests), which magnified the CPU cost precisely when a recovery
            # defect had allowed the client population to grow.
            shard_active = active_by_shard.get(shard.shard_id, [])
            oldest = min(shard_active, key=lambda item: item["started_at"], default=None)
            shards.append({
                "shard_id": shard.shard_id,
                "age_seconds": max(0.0, now_value - shard.created_at),
                "in_flight": len(shard_active),
                "peak_in_flight": shard.peak_in_flight,
                "requests_started": shard.requests_started,
                "requests_completed": shard.requests_completed,
                "requests_failed": shard.requests_failed,
                "last_error": shard.last_error,
                "retiring": shard.retiring,
                "retired_reason": shard.retired_reason,
                "cohort_stalls": shard.cohort_stalls,
                "seconds_since_terminal": (
                    max(0.0, now_value - shard.last_terminal_at)
                    if shard.last_terminal_at else None
                ),
                "active_phase_counts": dict(Counter(
                    str(item["phase"]) for item in shard_active
                )),
                "oldest_active_seconds": (
                    max(0.0, now_value - oldest["started_at"]) if oldest else None
                ),
                "oldest_phase": oldest["phase"] if oldest else None,
                "oldest_node": oldest.get("node_name") if oldest else None,
                "oldest_job_id": oldest.get("job_id") if oldest else None,
                "oldest_stream_id": oldest.get("stream_id") if oldest else None,
                "oldest_phase_seconds": (
                    max(0.0, now_value - oldest["phase_at"]) if oldest else None
                ),
                "active_nodes": dict(Counter(
                    str(item.get("node_name") or "unknown") for item in shard_active
                )),
                "active_stream_ids": sorted(
                    int(item["stream_id"])
                    for item in shard_active
                    if isinstance(item.get("stream_id"), int)
                ),
                "active_response_bytes": sum(
                    int(item.get("response_bytes", 0)) for item in shard_active
                ),
                "active_with_response_bytes": sum(
                    int(item.get("response_bytes", 0)) > 0 for item in shard_active
                ),
                "oldest_seconds_since_response_progress": max(
                    (
                        now_value - item["last_response_progress_at"]
                        for item in shard_active
                        if item.get("last_response_progress_at") is not None
                    ),
                    default=None,
                ),
            })
        return {
            **self._configuration_snapshot(),
            "client_count": len(self._clients),
            "retiring_client_count": sum(shard.retiring for shard in self._clients),
            "idle_client_count": sum(shard.in_flight == 0 for shard in self._clients),
            "in_flight": len(active),
            "active_phase_counts": dict(Counter(str(item["phase"]) for item in active)),
            "oldest_active_seconds": max(
                (now_value - item["started_at"] for item in active), default=0.0
            ),
            "shards": shards,
        }

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            loop, thread = self._loop, self._thread
        if loop is None or thread is None or not thread.is_alive():
            return {
                **self._configuration_snapshot(),
                "client_count": 0,
                "requests_enqueued": self._requests_enqueued,
                "ingress_wakeups": self._ingress_wakeups,
                "wakeups_per_request": 0.0,
                "state_flush_interval": self._state_flush_interval,
                "active_phase_counts": {},
                "oldest_active_seconds": 0.0,
                "shards": [],
            }
        return asyncio.run_coroutine_threadsafe(self._snapshot(), loop).result(timeout=5)
