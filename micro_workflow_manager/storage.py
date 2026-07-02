import errno
import json
import os
import re
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from shutil import copy2
from threading import Lock, RLock
from typing import Any
from uuid import uuid4

from .models import Job, QUEUED, VALID_STATUSES


class FileStorage:
    _thread_locks: dict[Path, RLock] = {}
    _thread_locks_guard = Lock()

    def __init__(self, project_dir: str | Path):
        self.project_dir = Path(project_dir).resolve()
        self.lock = RLock()
        self.project_dir.mkdir(parents=True, exist_ok=True)

    @classmethod
    def thread_lock_for(cls, path: Path) -> RLock:
        path = Path(path).resolve()
        with cls._thread_locks_guard:
            lock = cls._thread_locks.get(path)
            if lock is None:
                lock = RLock()
                cls._thread_locks[path] = lock
            return lock

    def retryable_errno(self, error: OSError) -> bool:
        retryable = {
            getattr(errno, "EACCES", 13),
            getattr(errno, "EAGAIN", 11),
            getattr(errno, "EBUSY", 16),
            getattr(errno, "EDEADLK", 35),
            getattr(errno, "ENOLCK", 37),
            getattr(errno, "EPERM", 1),
            36,  # Some platforms report "Resource deadlock avoided" as errno 36.
        }
        return getattr(error, "errno", None) in retryable

    def retry_fs(self, action, *, attempts: int = 60, base_delay: float = 0.02):
        last_error = None

        for attempt in range(attempts):
            try:
                return action()
            except OSError as error:
                if not self.retryable_errno(error):
                    raise
                last_error = error
                time.sleep(min(1.0, base_delay * (attempt + 1)))

        raise last_error

    def remove_if_exists(self, path: Path):
        def action():
            try:
                path.unlink()
            except FileNotFoundError:
                pass

        self.retry_fs(action)

    def atomic_write_text(self, path: Path, content: str, encoding: str = "utf-8") -> Path:
        path.parent.mkdir(parents=True, exist_ok=True)
        temp = path.with_name(
            f".{path.name}.{os.getpid()}.{threading.get_ident()}.{uuid4().hex}.tmp"
        )

        try:
            def action():
                temp.write_text(content, encoding=encoding)
                os.replace(temp, path)

            self.retry_fs(action)
            return path
        finally:
            if temp.exists():
                self.remove_if_exists(temp)

    def atomic_write_bytes(self, path: Path, content: bytes) -> Path:
        path.parent.mkdir(parents=True, exist_ok=True)
        temp = path.with_name(
            f".{path.name}.{os.getpid()}.{threading.get_ident()}.{uuid4().hex}.tmp"
        )

        try:
            def action():
                temp.write_bytes(content)
                os.replace(temp, path)

            self.retry_fs(action)
            return path
        finally:
            if temp.exists():
                self.remove_if_exists(temp)

    def atomic_copy_file(self, source: Path, target: Path) -> Path:
        target.parent.mkdir(parents=True, exist_ok=True)
        temp = target.with_name(
            f".{target.name}.{os.getpid()}.{threading.get_ident()}.{uuid4().hex}.tmp"
        )

        try:
            def action():
                copy2(source, temp)
                os.replace(temp, target)

            self.retry_fs(action)
            return target
        finally:
            if temp.exists():
                self.remove_if_exists(temp)

    def append_text(self, path: Path, content: str, encoding: str = "utf-8") -> Path:
        path.parent.mkdir(parents=True, exist_ok=True)

        def action():
            with path.open("a", encoding=encoding) as file:
                file.write(content)

        self.retry_fs(action)
        return path

    def validate_node_name(self, node_name: str) -> str:
        if not isinstance(node_name, str) or not node_name or node_name in {".", ".."}:
            raise ValueError("Invalid node name")

        if any(part in node_name for part in ["/", "\\", ".."]):
            raise ValueError(f"Unsafe node name: {node_name}")

        return node_name

    def safe_join(self, base: Path, *parts: str | Path) -> Path:
        base = base.resolve()
        path = base.joinpath(*parts).resolve()

        try:
            path.relative_to(base)
        except ValueError as error:
            raise ValueError(f"Unsafe path outside base directory: {path}") from error

        return path

    def validate_relative_pattern(self, pattern: str):
        path = Path(pattern)

        if path.is_absolute() or ".." in path.parts:
            raise ValueError(f"Unsafe glob pattern: {pattern}")

    def workflow_file(self) -> Path:
        return self.project_dir / ".mwf"

    def run_state_file(self) -> Path:
        return self.project_dir / ".mwf_run.json"

    def write_run_state(self, data: dict):
        self.atomic_write_json(self.run_state_file(), data)

    def get_run_state(self) -> dict:
        data = self.read_json(self.run_state_file(), default={})
        return data if isinstance(data, dict) else {}

    def update_run_state(self, **updates):
        data = self.get_run_state()
        data.update(updates)
        self.write_run_state(data)

    def lock_dir(self) -> Path:
        path = self.project_dir / ".mwf_locks"
        path.mkdir(parents=True, exist_ok=True)
        return path

    def lock_file(self, name: str) -> Path:
        safe = re.sub(r"[^A-Za-z0-9_.-]+", "_", name).strip("._") or "lock"
        return self.lock_dir() / f"{safe}.lock"

    @contextmanager
    def interprocess_lock(self, name: str):
        """Cross-process lock with same-process thread serialization.

        On Linux/macOS, fcntl/flock locks are process-level and can behave badly
        when many threads in the same process try to acquire the same lock file
        at once. The per-lock RLock serializes threads before taking the OS lock.
        The OS lock still protects process-pool workers and separate CLI commands.
        """
        path = self.lock_file(name)
        thread_lock = self.thread_lock_for(path)

        with thread_lock:
            file = self.retry_fs(lambda: path.open("a+b"), attempts=60)
            try:
                if os.name == "nt":
                    import msvcrt

                    if file.tell() == 0 and file.read(1) == b"":
                        file.write(b"0")
                        file.flush()
                    file.seek(0)
                    self.retry_fs(lambda: msvcrt.locking(file.fileno(), msvcrt.LK_LOCK, 1), attempts=120)
                    try:
                        yield
                    finally:
                        file.seek(0)
                        self.retry_fs(lambda: msvcrt.locking(file.fileno(), msvcrt.LK_UNLCK, 1), attempts=20)
                else:
                    import fcntl

                    self.retry_fs(lambda: fcntl.flock(file.fileno(), fcntl.LOCK_EX), attempts=120)
                    try:
                        yield
                    finally:
                        self.retry_fs(lambda: fcntl.flock(file.fileno(), fcntl.LOCK_UN), attempts=20)
            finally:
                file.close()

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

    def validate_job_id(self, job_id: int) -> int:
        if type(job_id) is not int or job_id < 1:
            raise ValueError("job_id must be an integer >= 1")
        return job_id

    def validate_status(self, status: str) -> str:
        if status not in VALID_STATUSES:
            raise ValueError(f"Invalid status: {status}")
        return status

    def json_text(self, path: Path, data: Any) -> str:
        try:
            return json.dumps(data, indent=2, ensure_ascii=False)
        except TypeError as error:
            raise TypeError(
                f"Data written to {path} must be JSON serializable: {error}"
            ) from error

    def json_signature(self, data: Any) -> str:
        try:
            return json.dumps(
                data,
                sort_keys=True,
                ensure_ascii=False,
                separators=(",", ":"),
            )
        except TypeError as error:
            raise TypeError(f"Data must be JSON serializable: {error}") from error

    def atomic_write_json(self, path: Path, data: Any):
        with self.lock:
            self.atomic_write_text(path, self.json_text(path, data))

    def read_json(self, path: Path, default: Any = None) -> Any:
        if not path.exists():
            return default

        return json.loads(path.read_text(encoding="utf-8"))

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
        with self.lock:
            self.atomic_write_json(
                self.node_state_file(node_name),
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

    def next_job_id(self, node_name: str) -> int:
        with self.lock:
            existing = []

            for path in self.jobs_dir(node_name).iterdir():
                if path.is_dir() and path.name.isdigit() and (path / "job.json").is_file():
                    existing.append(int(path.name))

            if not existing:
                return 1

            return max(existing) + 1

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

            self.set_job_status(job.node_name, job.job_id, QUEUED)

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

        # QUEUED is the default state for an existing job. Keeping it implicit
        # avoids thousands of tiny JSON writes when a large node is reset before
        # a run. Non-queued states still get an explicit status.json so existing
        # tooling can inspect running/done/failed jobs on disk.
        if status == QUEUED and not extra:
            with self.lock:
                path = self.status_file(node_name, job_id)
                self.remove_if_exists(path)
            return

        data = {
            "job_id": job_id,
            "node_name": node_name,
            "status": status,
            **extra,
        }

        self.atomic_write_json(
            self.status_file(node_name, job_id),
            data,
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
        """Yield queued job IDs lazily.

        This lets runners start executing the first queued jobs while the rest of
        a huge node is still being discovered. The older queued_job_ids() helper
        still returns a sorted list for callers that need a stable display order.
        """
        for job_id in self.iter_job_ids(node_name):
            if self.job_is_queued(node_name, job_id):
                yield job_id

    def queued_job_ids(self, node_name: str) -> list[int]:
        return sorted(self.iter_queued_job_ids(node_name))

    def has_queued_jobs(self, node_name: str) -> bool:
        return next(self.iter_queued_job_ids(node_name), None) is not None

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
