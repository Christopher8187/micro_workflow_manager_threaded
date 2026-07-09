from __future__ import annotations

from pathlib import Path
from shutil import copy2

from micro_workflow_manager.models import QUEUED


class NodeFileStorageMixin:
    """Project, node-folder, and node-level file operations."""

    def node_dir(self, node_name: str) -> Path:
        node_name = self.validate_node_name(node_name)
        path = self.safe_join(self.project_dir / "node", node_name)
        path.mkdir(parents=True, exist_ok=True)
        return path

    def node_input_dir(self, node_name: str) -> Path:
        path = self.node_dir(node_name) / "input"
        path.mkdir(parents=True, exist_ok=True)
        return path

    def node_output_dir(self, node_name: str) -> Path:
        path = self.node_dir(node_name) / "output"
        path.mkdir(parents=True, exist_ok=True)
        return path

    def jobs_dir(self, node_name: str) -> Path:
        path = self.node_dir(node_name) / "jobs"
        path.mkdir(parents=True, exist_ok=True)
        return path

    def job_dir(self, node_name: str, job_id: int) -> Path:
        job_id = self.validate_job_id(job_id)
        path = self.safe_join(self.jobs_dir(node_name), str(job_id))
        path.mkdir(parents=True, exist_ok=True)
        self.files_dir(node_name, job_id)
        return path

    def files_dir(self, node_name: str, job_id: int) -> Path:
        job_id = self.validate_job_id(job_id)
        path = self.safe_join(self.jobs_dir(node_name), str(job_id), "files")
        path.mkdir(parents=True, exist_ok=True)
        return path

    def debug_file(self, node_name: str) -> Path:
        return self.node_output_dir(node_name) / "debug.txt"

    def node_state_file(self, node_name: str) -> Path:
        return self.node_dir(node_name) / "node_state.json"

    def node_schema_file(self, node_name: str) -> Path:
        return self.node_dir(node_name) / "schema.json"

    def default_jobs_file(self, node_name: str) -> Path:
        return self.node_dir(node_name) / "default_jobs.json"

    def job_index_file(self, node_name: str) -> Path:
        return self.node_dir(node_name) / "job_index.json"

    def queued_dir(self, node_name: str) -> Path:
        path = self.node_dir(node_name) / "queued"
        path.mkdir(parents=True, exist_ok=True)
        return path

    def queued_marker_file(self, node_name: str, job_id: int) -> Path:
        job_id = self.validate_job_id(job_id)
        return self.queued_dir(node_name) / f"{job_id}.queued"

    def job_index_dirty_file(self, node_name: str) -> Path:
        return self.node_dir(node_name) / "job_index.dirty"

    def job_base_dir(self, node_name: str, job_id: int) -> Path:
        job_id = self.validate_job_id(job_id)
        return self.safe_join(self.jobs_dir(node_name), str(job_id))

    def job_file(self, node_name: str, job_id: int) -> Path:
        return self.job_base_dir(node_name, job_id) / "job.json"

    def input_file(self, node_name: str, job_id: int) -> Path:
        return self.job_base_dir(node_name, job_id) / "input.json"

    def status_file(self, node_name: str, job_id: int) -> Path:
        return self.job_base_dir(node_name, job_id) / "status.json"

    def output_file(self, node_name: str, job_id: int) -> Path:
        return self.job_base_dir(node_name, job_id) / "output.json"

    def write_graph(self, edges: list[tuple[str, str]]):
        data = self.read_json(self.workflow_file(), default={})

        if not isinstance(data, dict):
            data = {}

        data["edges"] = edges
        self.atomic_write_json(self.workflow_file(), data)

    def init_node_folders(self, node_name: str):
        self.node_dir(node_name)
        self.node_input_dir(node_name)
        self.node_output_dir(node_name)
        self.jobs_dir(node_name)
        self.queued_dir(node_name)

    def input_path(self, node_name: str, *parts: str) -> Path:
        return self.safe_join(self.node_input_dir(node_name), *parts)

    def output_path(self, node_name: str, *parts: str) -> Path:
        return self.safe_join(self.node_output_dir(node_name), *parts)

    def input_files(
        self,
        node_name: str,
        pattern: str = "*",
        recursive: bool = False,
        files_only: bool = True,
    ) -> list[Path]:
        self.validate_relative_pattern(pattern)
        root = self.node_input_dir(node_name)

        paths = root.rglob(pattern) if recursive else root.glob(pattern)

        result = sorted(
            path for path in paths
            if path.resolve().is_relative_to(root.resolve())
        )

        if files_only:
            result = [path for path in result if path.is_file()]

        return result

    def output_files(
        self,
        node_name: str,
        pattern: str = "*",
        recursive: bool = False,
        files_only: bool = True,
    ) -> list[Path]:
        self.validate_relative_pattern(pattern)
        root = self.node_output_dir(node_name)

        paths = root.rglob(pattern) if recursive else root.glob(pattern)

        result = sorted(
            path for path in paths
            if path.resolve().is_relative_to(root.resolve())
        )

        if files_only:
            result = [path for path in result if path.is_file()]

        return result

    def write_node_output_text(
        self,
        node_name: str,
        filename: str,
        content: str,
    ) -> Path:
        path = self.safe_join(self.node_output_dir(node_name), filename)
        self.atomic_write_text(path, content)
        return path

    def write_node_output_bytes(
        self,
        node_name: str,
        filename: str,
        content: bytes,
    ) -> Path:
        path = self.safe_join(self.node_output_dir(node_name), filename)
        self.atomic_write_bytes(path, content)
        return path

    def write_node_input_text(
        self,
        node_name: str,
        filename: str,
        content: str,
        *,
        overwrite: bool = False,
    ) -> Path:
        with self.interprocess_lock(f"node-{node_name}-input"):
            directory = self.node_input_dir(node_name)
            path = self.safe_join(directory, filename)
            if path.exists() and not overwrite:
                path = self.unique_target(path.parent, path.name)
            self.atomic_write_text(path, content)
            return path

    def write_node_input_bytes(
        self,
        node_name: str,
        filename: str,
        content: bytes,
        *,
        overwrite: bool = False,
    ) -> Path:
        with self.interprocess_lock(f"node-{node_name}-input"):
            directory = self.node_input_dir(node_name)
            path = self.safe_join(directory, filename)
            if path.exists() and not overwrite:
                path = self.unique_target(path.parent, path.name)
            self.atomic_write_bytes(path, content)
            return path

    def copy_to_node_input(
        self,
        node_name: str,
        source: str | Path,
        filename: str | None = None,
        *,
        overwrite: bool = False,
    ) -> Path:
        source_path = Path(source)
        if not source_path.exists():
            raise FileNotFoundError(f"Input source file does not exist: {source_path}")
        if not source_path.is_file():
            raise ValueError(f"Input source path is not a file: {source_path}")

        with self.interprocess_lock(f"node-{node_name}-input"):
            target_name = filename or source_path.name
            target = self.safe_join(self.node_input_dir(node_name), target_name)
            if target.exists() and not overwrite:
                target = self.unique_target(target.parent, target.name)
            self.atomic_copy_file(source_path, target)
            return target

    def write_node_schema(
        self,
        node_name: str,
        allowed_params: set[str],
        required_params: set[str],
        retries: int,
        repeats: int,
        fallbacks: list[str],
        runner_override: str | None = None,
        max_threads: int | None = None,
    ):
        self.atomic_write_json(
            self.node_schema_file(node_name),
            {
                "node": node_name,
                "allowed_params": sorted(allowed_params),
                "required_params": sorted(required_params),
                "retries": retries,
                "repeats": repeats,
                "fallbacks": fallbacks,
                "runner_override": runner_override,
                "sequential": runner_override == "direct",
                "max_threads": max_threads,
                "input_dir": str(self.node_input_dir(node_name)),
                "output_dir": str(self.node_output_dir(node_name)),
                "jobs_dir": str(self.jobs_dir(node_name)),
            },
        )

    def set_node_status(self, node_name: str, status: str):
        status = self.validate_status(status)
        path = self.node_state_file(node_name)
        current = self.read_json(path, default=None)
        if isinstance(current, dict) and current.get("status") == status:
            return

        with self.lock:
            current = self.read_json(path, default=None)
            if isinstance(current, dict) and current.get("status") == status:
                return
            self.atomic_write_json(
                path,
                {
                    "node": node_name,
                    "status": status,
                },
            )

    def get_node_status(self, node_name: str) -> str | None:
        data = self.read_json(self.node_state_file(node_name), default=None)

        if data is None:
            return None

        return data.get("status")

    def write_debug(self, node_name: str, message: str):
        from datetime import datetime

        timestamp = datetime.now().isoformat(timespec="seconds")

        with self.interprocess_lock(f"node-{node_name}-debug"):
            self.append_text(self.debug_file(node_name), f"[{timestamp}] {message}\n")
