from __future__ import annotations

from datetime import datetime
from pathlib import Path

from micro_workflow_manager.paths import threads_file

from micro_workflow_manager.node import validate_positive_int
from micro_workflow_manager.schema import CURRENT_STATE_SCHEMA_VERSION


class RuntimeConfigStorageMixin:
    """Small mutable project settings used by second-terminal controls.

    Runtime thread overrides are intentionally stored separately from ``.mwf``.
    The graph/router declaration remains the durable default; this file is only
    a local testing/runtime override and can be deleted or reset at any time.
    """

    def thread_overrides_file(self) -> Path:
        return threads_file(self.project_dir)

    def read_thread_overrides(self) -> dict[str, int]:
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
        return result

    def write_thread_overrides(self, overrides: dict[str, int]) -> None:
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
                "overrides": dict(sorted(checked.items())),
            },
        )

    def set_thread_override(self, node_name: str, max_threads: int) -> int:
        node_name = self.validate_node_name(node_name)
        max_threads = validate_positive_int("max_threads", max_threads)
        with self.interprocess_lock("thread-overrides"):
            overrides = self.read_thread_overrides()
            overrides[node_name] = max_threads
            self.write_thread_overrides(overrides)
        return max_threads

    def clear_thread_override(self, node_name: str) -> bool:
        node_name = self.validate_node_name(node_name)
        with self.interprocess_lock("thread-overrides"):
            overrides = self.read_thread_overrides()
            existed = node_name in overrides
            overrides.pop(node_name, None)
            self.write_thread_overrides(overrides)
        return existed
