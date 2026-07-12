from __future__ import annotations

import hashlib
import os
from pathlib import Path
from typing import Any

from micro_workflow_manager.models import Job, QUEUED
from micro_workflow_manager.schema import CURRENT_STATE_SCHEMA_VERSION


class JobFileStorageMixin:
    """Job lifecycle, job status, and returned-file storage operations."""

    def idempotency_key_hash(self, key: str) -> str:
        if not isinstance(key, str) or not key.strip():
            raise ValueError("idempotency_key must be a non-empty string")
        return hashlib.sha256(key.encode("utf-8")).hexdigest()

    def lookup_idempotent_job(self, node_name: str, key: str) -> Job | None:
        key_hash = self.idempotency_key_hash(key)
        data = self.read_json(self.idempotency_file(node_name, key_hash), default=None)
        if not isinstance(data, dict):
            return None
        job_id = data.get("job_id")
        if type(job_id) is not int or not self.job_exists(node_name, job_id):
            return None
        return self.load_job(node_name, job_id)

    def record_idempotent_job(self, node_name: str, key: str, job_id: int):
        key_hash = self.idempotency_key_hash(key)
        self.atomic_write_json(
            self.idempotency_file(node_name, key_hash),
            {"schema_version": CURRENT_STATE_SCHEMA_VERSION, "key": key, "job_id": self.validate_job_id(job_id)},
        )


    def next_job_id(self, node_name: str) -> int:
        # Fast path: use the per-node index instead of scanning every job folder.
        # add_job/create_job already hold the node jobs lock while allocating.
        try:
            return int(self.read_job_index(node_name).get("last_job_id") or 0) + 1
        except OSError as error:
            if not self.retryable_errno(error):
                raise
            # If the index file itself is temporarily inaccessible, job-id
            # allocation must still work. The jobs folder is authoritative and
            # this call is made while the node jobs lock is held.
            self.mark_job_index_dirty(node_name, f"next_job_id: {error}")
            return max(self.iter_job_ids(node_name), default=0) + 1

    def job_exists(self, node_name: str, job_id: int) -> bool:
        job_id = self.validate_job_id(job_id)
        return self.job_file(node_name, job_id).exists()

    def default_job_spec_key(self, start_job_id: int, number: int) -> str:
        return f"{start_job_id}:{number}"

    def default_job_spec_current(
        self,
        node_name: str,
        *,
        start_job_id: int,
        number: int,
        params: dict[str, Any],
    ) -> bool:
        manifest = self.read_json(self.default_jobs_file(node_name), default={})
        if not isinstance(manifest, dict):
            return False

        key = self.default_job_spec_key(start_job_id, number)
        expected = {
            "start_job_id": start_job_id,
            "number": number,
            "params_signature": self.json_signature(params),
        }

        if manifest.get(key) != expected:
            return False

        jobs_root = self.jobs_dir(node_name)
        return all(
            (jobs_root / str(job_id) / "job.json").is_file()
            for job_id in range(start_job_id, start_job_id + number)
        )

    def write_default_job_spec(
        self,
        node_name: str,
        *,
        start_job_id: int,
        number: int,
        params: dict[str, Any],
    ):
        with self.lock:
            path = self.default_jobs_file(node_name)
            manifest = self.read_json(path, default={})
            if not isinstance(manifest, dict):
                manifest = {}

            key = self.default_job_spec_key(start_job_id, number)
            manifest["schema_version"] = CURRENT_STATE_SCHEMA_VERSION
            manifest[key] = {
                "start_job_id": start_job_id,
                "number": number,
                "params_signature": self.json_signature(params),
            }
            self.atomic_write_json(path, manifest)

    def create_job(self, job: Job):
        self.validate_job_id(job.job_id)
        # Validate before creating folders so bad params cannot leave partial jobs.
        self.json_text(Path("input.json"), job.params)

        with self.lock:
            if self.job_exists(job.node_name, job.job_id):
                raise ValueError(
                    f"Job {job.node_name}/{job.job_id} already exists"
                )

            self.job_dir(job.node_name, job.job_id)

            self.atomic_write_json(
                self.job_file(job.node_name, job.job_id),
                {
                    "schema_version": CURRENT_STATE_SCHEMA_VERSION,
                    "job_id": job.job_id,
                    "node_name": job.node_name,
                    "parent": job.parent,
                    "created_at": job.created_at,
                },
            )

            self.atomic_write_json(
                self.input_file(job.node_name, job.job_id),
                job.params,
            )

            self.remove_if_exists(self.status_file(job.node_name, job.job_id))
            self.register_job_created(job.node_name, job.job_id, QUEUED)
            self.append_job_event(
                job.node_name,
                job.job_id,
                "created",
                status=QUEUED,
                parent=job.parent,
            )

    def ensure_job(self, job: Job) -> Job:
        """Create or refresh a deterministic default job.

        Unlike create_job, this method is idempotent. It is used for jobs
        declared in node_behavior files, because those files are imported every
        time the CLI loads the workflow.
        """
        self.validate_job_id(job.job_id)
        self.json_text(Path("input.json"), job.params)

        with self.lock:
            if not self.job_exists(job.node_name, job.job_id):
                self.create_job(job)
                return job

            existing_params = self.read_json(
                self.input_file(job.node_name, job.job_id),
                default={},
            )

            if existing_params != job.params:
                self.atomic_write_json(
                    self.input_file(job.node_name, job.job_id),
                    job.params,
                )
                self.set_job_status(job.node_name, job.job_id, QUEUED)

            job_data = self.read_json(
                self.job_file(job.node_name, job.job_id),
                default={},
            )

            if job_data.get("parent") is not None:
                self.atomic_write_json(
                    self.job_file(job.node_name, job.job_id),
                    {
                        "schema_version": CURRENT_STATE_SCHEMA_VERSION,
                        "job_id": job.job_id,
                        "node_name": job.node_name,
                        "parent": None,
                        "created_at": job_data.get("created_at", job.created_at),
                    },
                )

            return job

    def load_job(self, node_name: str, job_id: int) -> Job:
        self.validate_job_id(job_id)
        job_path = self.job_file(node_name, job_id)
        job_data = self.read_json(job_path)
        if job_data is None:
            raise FileNotFoundError(f"Job does not exist: {node_name}/{job_id}")

        params = self.read_json(self.input_file(node_name, job_id), default={})

        return Job(
            job_id=job_data["job_id"],
            node_name=job_data["node_name"],
            params=params,
            parent=job_data.get("parent"),
            created_at=job_data["created_at"],
        )

    def set_job_status(self, node_name: str, job_id: int, status: str, **extra):
        job_id = self.validate_job_id(job_id)
        status = self.validate_status(status)
        status_path = self.status_file(node_name, job_id)
        old_data = self.read_json(status_path, default=None)

        if isinstance(old_data, dict):
            old_status = old_data.get("status") or QUEUED
        elif self.job_exists(node_name, job_id):
            old_status = QUEUED
        else:
            old_status = None

        # QUEUED is the default state for an existing job. Keeping it implicit
        # avoids thousands of tiny JSON writes when a large node is reset before
        # a run. Non-queued states still get an explicit status.json so existing
        # tooling can inspect running/done/failed jobs on disk.
        if status == QUEUED and not extra:
            self.remove_if_exists(status_path)
            new_data = None
        else:
            new_data = {
                "schema_version": CURRENT_STATE_SCHEMA_VERSION,
                "job_id": job_id,
                "node_name": node_name,
                "status": status,
                **extra,
            }
            self.atomic_write_json(status_path, new_data)

        self.update_job_index_status(
            node_name=node_name,
            job_id=job_id,
            old_status=old_status,
            new_status=status,
            old_data=old_data if isinstance(old_data, dict) else None,
            new_data=new_data,
        )

        if old_status != status or extra:
            event_name = {
                "queued": "queued",
                "running": "started",
                "done": "done",
                "failed": "failed",
                "cancelled": "cancelled",
                "skipped": "skipped",
            }.get(status, "status_changed")
            self.append_job_event(
                node_name,
                job_id,
                event_name,
                previous_status=old_status,
                status=status,
                **extra,
            )

    def get_job_status(self, node_name: str, job_id: int) -> str | None:
        self.validate_job_id(job_id)

        if not self.job_exists(node_name, job_id):
            return None

        data = self.read_json(self.status_file(node_name, job_id), default=None)

        if data is None:
            return QUEUED

        return data.get("status")

    def list_job_ids(self, node_name: str) -> list[int]:
        ids = []

        for path in self.jobs_dir(node_name).iterdir():
            if path.is_dir() and path.name.isdigit() and (path / "job.json").is_file():
                ids.append(int(path.name))

        return sorted(ids)

    def list_jobs(self, node_name: str, status: str | None = None) -> list[dict]:
        rows = []

        for job_id in self.list_job_ids(node_name):
            job_data = self.read_json(self.job_file(node_name, job_id), default={})
            status_data = self.read_json(
                self.status_file(node_name, job_id),
                default={
                    "job_id": job_id,
                    "node_name": node_name,
                    "status": QUEUED,
                },
            )

            row = {
                **job_data,
                **status_data,
            }

            if status is None or row.get("status") == status:
                rows.append(row)

        return rows

    def job_is_queued(self, node_name: str, job_id: int) -> bool:
        self.validate_job_id(job_id)
        status_path = self.status_file(node_name, job_id)

        if not status_path.exists():
            return self.job_exists(node_name, job_id)

        return self.read_json(status_path, default={}).get("status") == QUEUED

    def iter_job_ids(self, node_name: str):
        """Yield existing job IDs without building a full in-memory list first."""
        jobs_root = self.jobs_dir(node_name)

        with os.scandir(jobs_root) as entries:
            for entry in entries:
                if not entry.is_dir() or not entry.name.isdigit():
                    continue

                job_path = Path(entry.path) / "job.json"
                if job_path.is_file():
                    yield int(entry.name)

    def iter_queued_job_ids(self, node_name: str):
        """Yield queued job IDs from the queue marker directory.

        This avoids scanning every job/status file whenever the scheduler wants
        to know what can run next. The marker directory is maintained
        incrementally by set_job_status/create_job and rebuilt once for older
        projects that do not yet have job_index.json.
        """
        self.read_job_index(node_name)
        queued = self.queued_dir(node_name)
        ids = []
        with os.scandir(queued) as entries:
            for entry in entries:
                if entry.is_file() and entry.name.endswith(".queued"):
                    raw = entry.name[:-7]
                    if raw.isdigit():
                        ids.append(int(raw))
        yield from sorted(ids)

    def queued_job_ids(self, node_name: str) -> list[int]:
        return list(self.iter_queued_job_ids(node_name))

    def has_queued_jobs(self, node_name: str) -> bool:
        # Queue markers are the cheap scheduler source of truth. This avoids
        # missing newly-spawned jobs if the summary index is temporarily dirty,
        # and avoids reading the hot job_index.json file just to answer yes/no.
        queued = self.queued_dir(node_name)

        def action():
            with os.scandir(queued) as entries:
                return any(entry.is_file() and entry.name.endswith(".queued") for entry in entries)

        return bool(self.retry_fs(action, attempts=20, base_delay=0.01))

    def queued_jobs(self, node_name: str) -> list[Job]:
        return [
            self.load_job(node_name, job_id)
            for job_id in self.queued_job_ids(node_name)
        ]

    def write_output(self, node_name: str, job_id: int, data: dict):
        self.validate_job_id(job_id)
        self.atomic_write_json(
            self.output_file(node_name, job_id),
            data,
        )

    def write_text(self, node_name: str, job_id: int, filename: str, content: str) -> Path:
        path = self.safe_join(self.files_dir(node_name, job_id), filename)
        self.atomic_write_text(path, content)
        return path

    def write_bytes(self, node_name: str, job_id: int, filename: str, content: bytes) -> Path:
        path = self.safe_join(self.files_dir(node_name, job_id), filename)
        self.atomic_write_bytes(path, content)
        return path

    def unique_target(self, directory: Path, filename: str) -> Path:
        target = self.safe_join(directory, Path(filename).name)

        if not target.exists():
            return target

        stem = target.stem
        suffix = target.suffix

        index = 2
        while True:
            candidate = self.safe_join(directory, f"{stem}_{index}{suffix}")

            if not candidate.exists():
                return candidate

            index += 1

    def extract_files(self, result: Any, explicit: bool = False) -> list[Path]:
        files: list[Path] = []

        if result is None:
            return files

        if isinstance(result, Path):
            return [result]

        if isinstance(result, str):
            return [Path(result)] if explicit else []

        if isinstance(result, list | tuple):
            for item in result:
                files.extend(self.extract_files(item, explicit=explicit))
            return files

        if isinstance(result, dict):
            if "file" in result:
                files.extend(self.extract_files(result["file"], explicit=True))

            if "files" in result:
                files.extend(self.extract_files(result["files"], explicit=True))

            return files

        return files

    def store_returned_files(self, node_name: str, job_id: int, result: Any) -> list[str]:
        files = self.extract_files(result)
        stored: list[str] = []

        if not files:
            return stored

        destination = self.files_dir(node_name, job_id)

        for file in files:
            source = Path(file)

            if not source.exists():
                raise FileNotFoundError(f"Returned file does not exist: {source}")

            if not source.is_file():
                raise ValueError(f"Returned path is not a file: {source}")

            if source.parent.resolve() == destination.resolve():
                stored.append(str(source))
                continue

            target = self.unique_target(destination, source.name)
            self.atomic_copy_file(source, target)
            stored.append(str(target))

        return stored
