from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

from micro_workflow_manager.paths import threads_file
from micro_workflow_manager.node import validate_positive_int
from micro_workflow_manager.schema import CURRENT_STATE_SCHEMA_VERSION


class RuntimeConfigStorageMixin:
    """Small mutable project settings used by second-terminal controls.

    Per-node concurrency overrides are deliberately run-scoped. The legacy
    api_total_limit field is accepted only for state-schema compatibility and is
    ignored by MWF 0.3.15. A command issued outside a run creates a pending
    setting for the next run only. When a run claims the project, pending
    settings are bound to that run ID; when the run finishes they are removed.
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

        raw_api_total = data.get("api_total_limit")
        api_total_limit = (
            None
            if raw_api_total is None
            else validate_positive_int("api_total_limit", raw_api_total)
        )

        run_id = data.get("run_id")
        if run_id is not None and not isinstance(run_id, str):
            raise ValueError(".mwf/threads.json run_id must be a string or null")
        return {
            "run_id": run_id or None,
            "overrides": result,
            "api_total_limit": api_total_limit,
        }

    def _thread_override_state_is_current(self, state: dict[str, Any]) -> bool:
        bound_run_id = state.get("run_id")
        if bound_run_id is None:
            return True
        run_state = self.get_run_state()
        return (
            isinstance(run_state, dict)
            and run_state.get("status") == "running"
            and run_state.get("run_id") == bound_run_id
        )

    def read_runtime_limit_state(self) -> dict[str, Any]:
        state = self.read_thread_override_state()
        if not self._thread_override_state_is_current(state):
            return {"run_id": None, "overrides": {}, "api_total_limit": None}
        return state

    def read_thread_overrides(self) -> dict[str, int]:
        return dict(self.read_runtime_limit_state()["overrides"])

    def read_api_total_limit(self) -> int | None:
        return self.read_runtime_limit_state().get("api_total_limit")

    def write_thread_override_state(
        self,
        overrides: dict[str, int],
        *,
        run_id: str | None,
        api_total_limit: int | None = None,
    ) -> None:
        checked: dict[str, int] = {}
        for node_name, value in overrides.items():
            self.validate_node_name(node_name)
            checked[node_name] = validate_positive_int(
                f"thread override for {node_name}", value
            )
        checked_api_total = (
            None
            if api_total_limit is None
            else validate_positive_int("api_total_limit", api_total_limit)
        )

        path = self.thread_overrides_file()
        if not checked and checked_api_total is None:
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
                "api_total_limit": checked_api_total,
            },
        )

    def write_thread_overrides(self, overrides: dict[str, int]) -> None:
        state = self.read_thread_override_state()
        current = self._thread_override_state_is_current(state)
        self.write_thread_override_state(
            overrides,
            run_id=state.get("run_id") if current else None,
            api_total_limit=state.get("api_total_limit") if current else None,
        )

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
                api_total_limit = None
            else:
                overrides = dict(state["overrides"])
                api_total_limit = state.get("api_total_limit")
            overrides[node_name] = max_threads
            self.write_thread_override_state(
                overrides,
                run_id=run_id,
                api_total_limit=api_total_limit,
            )
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
                api_total_limit=state.get("api_total_limit"),
            )
        return existed

    def set_api_total_limit(
        self,
        value: int,
        *,
        run_id: str | None = None,
    ) -> int:
        value = validate_positive_int("api_total_limit", value)
        with self.interprocess_lock("thread-overrides"):
            state = self.read_thread_override_state()
            if state.get("run_id") not in {None, run_id}:
                overrides: dict[str, int] = {}
            else:
                overrides = dict(state["overrides"])
            self.write_thread_override_state(
                overrides,
                run_id=run_id,
                api_total_limit=value,
            )
        return value

    def clear_api_total_limit(self, *, run_id: str | None = None) -> bool:
        with self.interprocess_lock("thread-overrides"):
            state = self.read_thread_override_state()
            if run_id is not None and state.get("run_id") not in {None, run_id}:
                return False
            existed = state.get("api_total_limit") is not None
            self.write_thread_override_state(
                dict(state["overrides"]),
                run_id=run_id if run_id is not None else state.get("run_id"),
                api_total_limit=None,
            )
            return existed

    def bind_thread_overrides_to_run(self, run_id: str) -> dict[str, int]:
        """Claim pending runtime limits for one run; discard stale-run values."""
        if not isinstance(run_id, str) or not run_id:
            raise ValueError("run_id must be a non-empty string")
        with self.interprocess_lock("thread-overrides"):
            state = self.read_thread_override_state()
            if state.get("run_id") not in {None, run_id}:
                overrides: dict[str, int] = {}
                api_total_limit = None
            else:
                overrides = dict(state["overrides"])
                api_total_limit = state.get("api_total_limit")
            self.write_thread_override_state(
                overrides,
                run_id=run_id,
                api_total_limit=api_total_limit,
            )
            return overrides

    def clear_thread_overrides_for_run(self, run_id: str) -> bool:
        """Remove all runtime limits only when they belong to the finishing run."""
        with self.interprocess_lock("thread-overrides"):
            state = self.read_thread_override_state()
            if state.get("run_id") != run_id:
                return False
            self.write_thread_override_state({}, run_id=None, api_total_limit=None)
            return True
