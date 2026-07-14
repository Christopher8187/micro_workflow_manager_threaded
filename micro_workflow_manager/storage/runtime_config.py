from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

from micro_workflow_manager.paths import threads_file
from micro_workflow_manager.node import validate_positive_int
from micro_workflow_manager.schema import CURRENT_STATE_SCHEMA_VERSION


class RuntimeConfigStorageMixin:
    """Small mutable project settings used by second-terminal controls.

    Runtime thread overrides are deliberately run-scoped. A command issued
    outside a run creates a pending override for the next run only. When a run
    claims the project, pending overrides are bound to that run ID; when the run
    finishes they are removed. A stale override from a crashed/older run is
    ignored and cleared when the next run starts.
    """

    def thread_overrides_file(self) -> Path:
        return threads_file(self.project_dir)

    def read_thread_override_state(self) -> dict[str, Any]:
        data = self.read_json(self.thread_overrides_file(), default={})
        if not isinstance(data, dict):
            raise ValueError(".mwf/threads.json must contain a JSON object")

        raw = data.get("overrides", {})
        if raw is None:
            raw = {}
        if not isinstance(raw, dict):
            raise ValueError(".mwf/threads.json overrides must be a JSON object")

        result: dict[str, int] = {}
        for node_name, value in raw.items():
            self.validate_node_name(node_name)
            result[node_name] = validate_positive_int(
                f"thread override for {node_name}", value
            )

        run_id = data.get("run_id")
        if run_id is not None and not isinstance(run_id, str):
            raise ValueError(".mwf/threads.json run_id must be a string or null")
        return {"run_id": run_id or None, "overrides": result}

    def _thread_override_state_is_current(self, state: dict[str, Any]) -> bool:
        bound_run_id = state.get("run_id")
        if bound_run_id is None:
            # Pending overrides are valid until the next run claims them.
            return True
        run_state = self.get_run_state()
        return (
            isinstance(run_state, dict)
            and run_state.get("status") == "running"
            and run_state.get("run_id") == bound_run_id
        )

    def read_thread_overrides(self) -> dict[str, int]:
        state = self.read_thread_override_state()
        if not self._thread_override_state_is_current(state):
            return {}
        return dict(state["overrides"])

    def write_thread_override_state(
        self,
        overrides: dict[str, int],
        *,
        run_id: str | None,
    ) -> None:
        checked: dict[str, int] = {}
        for node_name, value in overrides.items():
            self.validate_node_name(node_name)
            checked[node_name] = validate_positive_int(
                f"thread override for {node_name}", value
            )

        path = self.thread_overrides_file()
        if not checked:
            self.remove_if_exists(path)
            return

        self.atomic_write_json(
            path,
            {
                "schema_version": CURRENT_STATE_SCHEMA_VERSION,
                "updated_at": datetime.now().astimezone().isoformat(
                    timespec="milliseconds"
                ),
                "run_id": run_id,
                "overrides": dict(sorted(checked.items())),
            },
        )

    def write_thread_overrides(self, overrides: dict[str, int]) -> None:
        # Backward-compatible helper: keep the current scope if it is usable;
        # otherwise create a pending next-run override.
        state = self.read_thread_override_state()
        run_id = state.get("run_id") if self._thread_override_state_is_current(state) else None
        self.write_thread_override_state(overrides, run_id=run_id)

    def set_thread_override(
        self,
        node_name: str,
        max_threads: int,
        *,
        run_id: str | None = None,
    ) -> int:
        node_name = self.validate_node_name(node_name)
        max_threads = validate_positive_int("max_threads", max_threads)
        with self.interprocess_lock("thread-overrides"):
            state = self.read_thread_override_state()
            if state.get("run_id") not in {None, run_id}:
                overrides: dict[str, int] = {}
            else:
                overrides = dict(state["overrides"])
            overrides[node_name] = max_threads
            self.write_thread_override_state(overrides, run_id=run_id)
        return max_threads

    def clear_thread_override(
        self,
        node_name: str,
        *,
        run_id: str | None = None,
    ) -> bool:
        node_name = self.validate_node_name(node_name)
        with self.interprocess_lock("thread-overrides"):
            state = self.read_thread_override_state()
            if run_id is not None and state.get("run_id") not in {None, run_id}:
                return False
            overrides = dict(state["overrides"])
            existed = node_name in overrides
            overrides.pop(node_name, None)
            self.write_thread_override_state(
                overrides,
                run_id=run_id if run_id is not None else state.get("run_id"),
            )
        return existed

    def bind_thread_overrides_to_run(self, run_id: str) -> dict[str, int]:
        """Claim pending overrides for one run and discard stale-run overrides."""
        if not isinstance(run_id, str) or not run_id:
            raise ValueError("run_id must be a non-empty string")
        with self.interprocess_lock("thread-overrides"):
            state = self.read_thread_override_state()
            if state.get("run_id") not in {None, run_id}:
                overrides: dict[str, int] = {}
            else:
                overrides = dict(state["overrides"])
            self.write_thread_override_state(overrides, run_id=run_id)
            return overrides

    def clear_thread_overrides_for_run(self, run_id: str) -> bool:
        """Remove overrides only when they belong to the finishing run."""
        with self.interprocess_lock("thread-overrides"):
            state = self.read_thread_override_state()
            if state.get("run_id") != run_id:
                return False
            self.write_thread_override_state({}, run_id=None)
            return True
