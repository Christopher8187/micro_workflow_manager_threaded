from __future__ import annotations

from pathlib import Path
from shutil import copy2

from micro_workflow_manager.models import QUEUED
from micro_workflow_manager.schema import CURRENT_STATE_SCHEMA_VERSION


class NodeFileStorageMixin:
    """Project, node-folder, and node-level file operations."""

    def node_dir(self, node_name: str) -> Path:
        node_name = self.validate_node_name(node_name)
        with self.lock:
            path = self._node_dir_cache.get(node_name)
            if path is None:
                # Validation rejects separators and traversal, so a direct child
                # join is safe and avoids a realpath walk on every job operation.
                path = self.project_dir / "node" / node_name
                self._node_dir_cache[node_name] = path
        # Graph updates may remove a previously cached directory while
        # the FileStorage object remains alive. Recreate the cached location
        # without repeating canonical path resolution.
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
        node_name = self.validate_node_name(node_name)
        with self.lock:
            path = self._jobs_dir_cache.get(node_name)
            if path is None:
                path = self.node_dir(node_name) / "jobs"
                self._jobs_dir_cache[node_name] = path
        path.mkdir(parents=True, exist_ok=True)
        return path

    def job_dir(self, node_name: str, job_id: int) -> Path:
        job_id = self.validate_job_id(job_id)
        path = self.jobs_dir(node_name) / str(job_id)
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
        """Legacy compatibility path; queue membership now lives in SQLite."""
        return self.node_dir(node_name) / "queued"

    def queued_marker_file(self, node_name: str, job_id: int) -> Path:
        job_id = self.validate_job_id(job_id)
        return self.queued_dir(node_name) / f"{job_id}.queued"

    def job_index_dirty_file(self, node_name: str) -> Path:
        return self.node_dir(node_name) / "job_index.dirty"

    def idempotency_dir(self, node_name: str) -> Path:
        """Legacy compatibility path; idempotency keys now live in SQLite."""
        return self.node_dir(node_name) / "idempotency"

    def idempotency_file(self, node_name: str, key_hash: str) -> Path:
        return self.idempotency_dir(node_name) / f"{key_hash}.json"

    def job_base_dir(self, node_name: str, job_id: int) -> Path:
        job_id = self.validate_job_id(job_id)
        return self.jobs_dir(node_name) / str(job_id)

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

        data["schema_version"] = CURRENT_STATE_SCHEMA_VERSION
        data["edges"] = edges
        self.atomic_write_json(self.workflow_file(), data)

    def init_node_folders(self, node_name: str):
        self.node_dir(node_name)
        self.node_input_dir(node_name)
        self.node_output_dir(node_name)
        self.jobs_dir(node_name)

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

    def write_node_input_texts(
        self,
        node_name: str,
        entries: list[tuple[str, str]],
        *,
        overwrite: bool = False,
        encoding: str = "utf-8",
    ) -> list[Path]:
        """Write many node-input files with atomic per-file publication.

        High-fanout producers should use this instead of calling
        ``write_node_input_text`` once per record. Deterministic overwrite batches
        do not need a global node lock; unique-name allocation for
        ``overwrite=False`` acquires it once for the batch.
        """
        if not isinstance(entries, list):
            raise TypeError("entries must be a list of (filename, content) pairs")
        if not entries:
            return []

        normalized: list[tuple[str, str]] = []
        seen: set[str] = set()
        for entry in entries:
            if not isinstance(entry, tuple) or len(entry) != 2:
                raise TypeError("each entry must be a (filename, content) tuple")
            filename, content = entry
            if not isinstance(filename, str) or not filename:
                raise ValueError("batch input filenames must be non-empty strings")
            if not isinstance(content, str):
                raise TypeError("batch input content must be text")
            relative = filename.replace("\\", "/")
            if relative in seen:
                raise ValueError(f"duplicate batch input filename: {relative}")
            seen.add(relative)
            normalized.append((relative, content))

        written: list[Path] = []

        def write_all() -> None:
            directory = self.node_input_dir(node_name)
            for filename, content in normalized:
                path = self.safe_join(directory, filename)
                if path.exists() and not overwrite:
                    path = self.unique_target(path.parent, path.name)
                self.atomic_write_text(path, content, encoding=encoding)
                written.append(path)

        # overwrite=True is used for deterministic, disjoint record paths. Atomic
        # replacement already makes same-path races safe, so a global node-input
        # lock would only serialize unrelated sections. Keep the lock for the
        # unique-name allocation required by overwrite=False.
        if overwrite:
            write_all()
        else:
            with self.interprocess_lock(f"node-{node_name}-input"):
                write_all()
        return written

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
        timeout: float | None = None,
        checkpoint_timeout: float | None = None,
        waiting: bool = False,
        wait_for: tuple[str, ...] | list[str] | None = None,
        resolved_wait_for: list[str] | tuple[str, ...] | None = None,
    ):
        self.atomic_write_json(
            self.node_schema_file(node_name),
            {
                "schema_version": CURRENT_STATE_SCHEMA_VERSION,
                "node": node_name,
                "allowed_params": sorted(allowed_params),
                "required_params": sorted(required_params),
                "retries": retries,
                "repeats": repeats,
                "fallbacks": fallbacks,
                "runner_override": runner_override,
                "sequential": runner_override == "direct",
                "max_threads": max_threads,
                "timeout": timeout,
                "checkpoint_timeout": checkpoint_timeout,
                "waiting": bool(waiting),
                "wait_for": None if wait_for is None else list(wait_for),
                "resolved_wait_for": list(resolved_wait_for or ()),
                "input_dir": str(self.node_input_dir(node_name)),
                "output_dir": str(self.node_output_dir(node_name)),
                "jobs_dir": str(self.jobs_dir(node_name)),
            },
        )

    def set_node_status(self, node_name: str, status: str):
        status = self.validate_node_status(status)
        node_name = self.validate_node_name(node_name)

        def update(connection):
            connection.execute(
                "INSERT INTO nodes(node_name, status) VALUES(?, ?) "
                "ON CONFLICT(node_name) DO UPDATE SET status=excluded.status, "
                "updated_at=CURRENT_TIMESTAMP WHERE nodes.status IS NOT excluded.status",
                (node_name, status),
            )

        # All project-local SQLite writes share the same mutation lane. A direct
        # worker-thread node-status transaction can otherwise hold the write
        # lock while that worker waits for a queued job-status mutation, leaving
        # the single mutation writer waiting for the same lock. Resident
        # Hoeflein pumps make that inversion reproducible under heavy feedback.
        self.submit_db_mutation(update, priority=5)

    def set_node_statuses(self, statuses: dict[str, str]) -> None:
        """Persist a component status snapshot through the mutation writer."""
        if not statuses:
            return
        rows = [
            (self.validate_node_name(node_name), self.validate_node_status(status))
            for node_name, status in statuses.items()
        ]

        def update(connection):
            connection.executemany(
                "INSERT INTO nodes(node_name, status) VALUES(?, ?) "
                "ON CONFLICT(node_name) DO UPDATE SET status=excluded.status, "
                "updated_at=CURRENT_TIMESTAMP WHERE nodes.status IS NOT excluded.status",
                rows,
            )

        self.submit_db_mutation(update, priority=5)

    def get_node_status(self, node_name: str) -> str | None:
        row = self.db_connection().execute(
            "SELECT status FROM nodes WHERE node_name=?",
            (self.validate_node_name(node_name),),
        ).fetchone()
        return None if row is None else row["status"]

    def get_node_statuses(self, node_names) -> dict[str, str]:
        """Read many node lifecycle states with one bounded SQLite query set."""
        normalized = sorted({self.validate_node_name(name) for name in node_names})
        if not normalized:
            return {}
        result: dict[str, str] = {}
        connection = self.db_connection()
        for offset in range(0, len(normalized), 500):
            chunk = normalized[offset:offset + 500]
            placeholders = ",".join("?" for _ in chunk)
            rows = connection.execute(
                f"SELECT node_name, status FROM nodes WHERE node_name IN ({placeholders})",
                chunk,
            ).fetchall()
            result.update({str(row["node_name"]): str(row["status"]) for row in rows})
        return result

    def write_debug(self, node_name: str, message: str):
        from datetime import datetime

        timestamp = datetime.now().isoformat(timespec="seconds")

        with self.interprocess_lock(f"node-{node_name}-debug"):
            self.append_text(self.debug_file(node_name), f"[{timestamp}] {message}\n")
