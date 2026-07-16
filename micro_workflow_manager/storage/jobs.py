from __future__ import annotations

import hashlib
import json
import shutil
from pathlib import Path
from typing import Any

from micro_workflow_manager.models import Job, QUEUED


class JobFileStorageMixin:
    """Job metadata in SQLite; job inputs/outputs and returned files on disk."""

    def idempotency_key_hash(self, key: str) -> str:
        if not isinstance(key, str) or not key.strip():
            raise ValueError("idempotency_key must be a non-empty string")
        return hashlib.sha256(key.encode("utf-8")).hexdigest()

    def lookup_idempotent_job(self, node_name: str, key: str) -> Job | None:
        key_hash = self.idempotency_key_hash(key)
        row = self.db_connection().execute(
            "SELECT job_id FROM idempotency WHERE node_name=? AND key_hash=?",
            (node_name, key_hash),
        ).fetchone()
        if row is None or not self.job_exists(node_name, int(row["job_id"])):
            return None
        return self.load_job(node_name, int(row["job_id"]))

    def record_idempotent_job(self, node_name: str, key: str, job_id: int):
        job_id = self.validate_job_id(job_id)
        key_hash = self.idempotency_key_hash(key)
        with self.db_transaction() as connection:
            connection.execute(
                "INSERT INTO idempotency(node_name, key_hash, key_text, job_id) "
                "VALUES(?, ?, ?, ?) "
                "ON CONFLICT(node_name, key_hash) DO UPDATE SET "
                "key_text=excluded.key_text, job_id=excluded.job_id",
                (node_name, key_hash, key, job_id),
            )

    def next_job_id(self, node_name: str) -> int:
        row = self.db_connection().execute(
            "SELECT COALESCE(MAX(job_id), 0) + 1 AS next_id FROM jobs WHERE node_name=?",
            (node_name,),
        ).fetchone()
        return int(row["next_id"])

    def job_exists(self, node_name: str, job_id: int) -> bool:
        job_id = self.validate_job_id(job_id)
        row = self.db_connection().execute(
            "SELECT 1 FROM jobs WHERE node_name=? AND job_id=?",
            (node_name, job_id),
        ).fetchone()
        return row is not None

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
        key = self.default_job_spec_key(start_job_id, number)
        row = self.db_connection().execute(
            "SELECT start_job_id, number, params_signature FROM default_job_specs "
            "WHERE node_name=? AND spec_key=?",
            (node_name, key),
        ).fetchone()
        if row is None:
            return False
        if (
            int(row["start_job_id"]) != start_job_id
            or int(row["number"]) != number
            or row["params_signature"] != self.json_signature(params)
        ):
            return False
        count = self.db_connection().execute(
            "SELECT COUNT(*) FROM jobs WHERE node_name=? AND job_id>=? AND job_id<?",
            (node_name, start_job_id, start_job_id + number),
        ).fetchone()[0]
        return int(count) == number

    def write_default_job_spec(
        self,
        node_name: str,
        *,
        start_job_id: int,
        number: int,
        params: dict[str, Any],
    ):
        key = self.default_job_spec_key(start_job_id, number)
        with self.db_transaction() as connection:
            connection.execute(
                "INSERT INTO default_job_specs"
                "(node_name, spec_key, start_job_id, number, params_signature) "
                "VALUES(?, ?, ?, ?, ?) "
                "ON CONFLICT(node_name, spec_key) DO UPDATE SET "
                "start_job_id=excluded.start_job_id, number=excluded.number, "
                "params_signature=excluded.params_signature",
                (node_name, key, start_job_id, number, self.json_signature(params)),
            )

    def read_job_metadata(self, node_name: str, job_id: int) -> dict[str, Any]:
        job_id = self.validate_job_id(job_id)
        row = self.db_connection().execute(
            "SELECT * FROM jobs WHERE node_name=? AND job_id=?",
            (node_name, job_id),
        ).fetchone()
        if row is None:
            return {}
        parent = json.loads(row["parent_json"]) if row["parent_json"] else None
        return {
            "job_id": int(row["job_id"]),
            "node_name": str(row["node_name"]),
            "parent": parent,
            "created_at": str(row["created_at"]),
        }

    def create_job(self, job: Job):
        self.validate_job_id(job.job_id)
        self.json_text(Path("input.json"), job.params)
        if self.job_exists(job.node_name, job.job_id):
            raise ValueError(f"Job {job.node_name}/{job.job_id} already exists")

        job_dir = self.job_dir(job.node_name, job.job_id)
        input_path = self.input_file(job.node_name, job.job_id)
        self.atomic_write_json(input_path, job.params)
        parent_json = json.dumps(job.parent, ensure_ascii=False) if job.parent is not None else None
        try:
            with self.db_transaction() as connection:
                connection.execute(
                    "INSERT INTO jobs(node_name, job_id, parent_json, created_at, status, status_json) "
                    "VALUES(?, ?, ?, ?, ?, '{}')",
                    (job.node_name, job.job_id, parent_json, job.created_at, QUEUED),
                )
        except BaseException:
            shutil.rmtree(job_dir, ignore_errors=True)
            raise
        self.append_job_event(
            job.node_name,
            job.job_id,
            "created",
            status=QUEUED,
            parent=job.parent,
        )

    def ensure_job(self, job: Job) -> Job:
        self.validate_job_id(job.job_id)
        self.json_text(Path("input.json"), job.params)
        if not self.job_exists(job.node_name, job.job_id):
            self.create_job(job)
            return job

        existing_params = self.read_json(self.input_file(job.node_name, job.job_id), default={})
        if existing_params != job.params:
            self.atomic_write_json(self.input_file(job.node_name, job.job_id), job.params)
            self.set_job_status(job.node_name, job.job_id, QUEUED)

        metadata = self.read_job_metadata(job.node_name, job.job_id)
        if metadata.get("parent") is not None:
            with self.db_transaction() as connection:
                connection.execute(
                    "UPDATE jobs SET parent_json=NULL WHERE node_name=? AND job_id=?",
                    (job.node_name, job.job_id),
                )
        return job

    def load_job(self, node_name: str, job_id: int) -> Job:
        metadata = self.read_job_metadata(node_name, job_id)
        if not metadata:
            raise FileNotFoundError(f"Job does not exist: {node_name}/{job_id}")
        params = self.read_json(self.input_file(node_name, job_id), default={})
        return Job(
            job_id=metadata["job_id"],
            node_name=metadata["node_name"],
            params=params,
            parent=metadata.get("parent"),
            created_at=metadata["created_at"],
        )

    def read_job_status_data(self, node_name: str, job_id: int) -> dict[str, Any]:
        row = self.db_connection().execute(
            "SELECT status, status_json FROM jobs WHERE node_name=? AND job_id=?",
            (node_name, self.validate_job_id(job_id)),
        ).fetchone()
        if row is None:
            return {}
        try:
            extra = json.loads(row["status_json"] or "{}")
        except json.JSONDecodeError:
            extra = {}
        if not isinstance(extra, dict):
            extra = {}
        return {
            "job_id": job_id,
            "node_name": node_name,
            "status": row["status"],
            **extra,
        }

    def set_job_status(self, node_name: str, job_id: int, status: str, **extra):
        job_id = self.validate_job_id(job_id)
        status = self.validate_status(status)
        old_data = self.read_job_status_data(node_name, job_id)
        if not old_data:
            raise FileNotFoundError(f"Job does not exist: {node_name}/{job_id}")
        old_status = old_data.get("status", QUEUED)
        terminal = status in {"done", "failed", "cancelled", "skipped"}
        with self.db_transaction() as connection:
            if terminal:
                connection.execute(
                    "UPDATE jobs SET status=?, status_json=?, active_execution_id=NULL, "
                    "active_pid=NULL, active_thread_id=NULL, active_started_at=NULL "
                    "WHERE node_name=? AND job_id=?",
                    (
                        status,
                        json.dumps(extra, ensure_ascii=False, separators=(",", ":")),
                        node_name,
                        job_id,
                    ),
                )
            else:
                connection.execute(
                    "UPDATE jobs SET status=?, status_json=? WHERE node_name=? AND job_id=?",
                    (
                        status,
                        json.dumps(extra, ensure_ascii=False, separators=(",", ":")),
                        node_name,
                        job_id,
                    ),
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
        row = self.db_connection().execute(
            "SELECT status FROM jobs WHERE node_name=? AND job_id=?",
            (node_name, self.validate_job_id(job_id)),
        ).fetchone()
        return None if row is None else str(row["status"])

    def list_job_ids(self, node_name: str) -> list[int]:
        rows = self.db_connection().execute(
            "SELECT job_id FROM jobs WHERE node_name=? ORDER BY job_id",
            (node_name,),
        ).fetchall()
        return [int(row["job_id"]) for row in rows]

    def list_jobs(self, node_name: str, status: str | None = None) -> list[dict]:
        sql = "SELECT job_id FROM jobs WHERE node_name=?"
        args: list[Any] = [node_name]
        if status is not None:
            sql += " AND status=?"
            args.append(status)
        sql += " ORDER BY job_id"
        rows = self.db_connection().execute(sql, args).fetchall()
        result = []
        for row in rows:
            job_id = int(row["job_id"])
            result.append({
                **self.read_job_metadata(node_name, job_id),
                **self.read_job_status_data(node_name, job_id),
            })
        return result

    def job_is_queued(self, node_name: str, job_id: int) -> bool:
        return self.get_job_status(node_name, job_id) == QUEUED

    def iter_job_ids(self, node_name: str):
        yield from self.list_job_ids(node_name)

    def iter_queued_job_ids(self, node_name: str):
        rows = self.db_connection().execute(
            "SELECT job_id FROM jobs WHERE node_name=? AND status=? ORDER BY job_id",
            (node_name, QUEUED),
        ).fetchall()
        for row in rows:
            yield int(row["job_id"])

    def queued_job_ids(self, node_name: str) -> list[int]:
        return list(self.iter_queued_job_ids(node_name))

    def has_queued_jobs(self, node_name: str) -> bool:
        row = self.db_connection().execute(
            "SELECT 1 FROM jobs WHERE node_name=? AND status=? LIMIT 1",
            (node_name, QUEUED),
        ).fetchone()
        return row is not None

    def queued_jobs(self, node_name: str) -> list[Job]:
        return [self.load_job(node_name, job_id) for job_id in self.iter_queued_job_ids(node_name)]

    def delete_job(self, node_name: str, job_id: int, *, remove_payload: bool = True) -> bool:
        job_id = self.validate_job_id(job_id)
        with self.db_transaction() as connection:
            existed = connection.execute(
                "SELECT 1 FROM jobs WHERE node_name=? AND job_id=?",
                (node_name, job_id),
            ).fetchone() is not None
            connection.execute(
                "DELETE FROM idempotency WHERE node_name=? AND job_id=?",
                (node_name, job_id),
            )
            connection.execute(
                "DELETE FROM job_events WHERE node_name=? AND job_id=?",
                (node_name, job_id),
            )
            connection.execute(
                "DELETE FROM jobs WHERE node_name=? AND job_id=?",
                (node_name, job_id),
            )
        if remove_payload:
            shutil.rmtree(self.job_base_dir(node_name, job_id), ignore_errors=True)
        return existed

    def delete_node_jobs(self, node_name: str, *, remove_payload: bool = True) -> int:
        ids = self.list_job_ids(node_name)
        with self.db_transaction() as connection:
            connection.execute("DELETE FROM idempotency WHERE node_name=?", (node_name,))
            connection.execute("DELETE FROM job_events WHERE node_name=?", (node_name,))
            connection.execute("DELETE FROM default_job_specs WHERE node_name=?", (node_name,))
            connection.execute("DELETE FROM jobs WHERE node_name=?", (node_name,))
        if remove_payload:
            shutil.rmtree(self.jobs_dir(node_name), ignore_errors=True)
            self.jobs_dir(node_name)
        return len(ids)

    def write_output(self, node_name: str, job_id: int, data: dict):
        self.validate_job_id(job_id)
        self.atomic_write_json(self.output_file(node_name, job_id), data)

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
