from __future__ import annotations

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

from micro_workflow_manager.models import VALID_STATUSES
from micro_workflow_manager.schema import CURRENT_STATE_SCHEMA_VERSION


class FileStorageBase:
    """Low-level file-backed primitives shared by storage implementations.

    Keep these methods backend-neutral where possible. A future database-backed
    storage can provide the same public storage API while reusing validation or
    replacing this base entirely.
    """

    _thread_locks: dict[Path, RLock] = {}
    _thread_locks_guard = Lock()

    def __init__(self, project_dir: str | Path):
        self.project_dir = Path(project_dir).resolve()
        self.lock = RLock()
        self.project_dir.mkdir(parents=True, exist_ok=True)

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
        thread_lock = self.thread_lock_for(path)

        try:
            def action():
                temp.write_text(content, encoding=encoding)
                os.replace(temp, path)

            with thread_lock:
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
        thread_lock = self.thread_lock_for(path)

        try:
            def action():
                temp.write_bytes(content)
                os.replace(temp, path)

            with thread_lock:
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
        self.atomic_write_json(
            self.run_state_file(),
            {**data, "schema_version": CURRENT_STATE_SCHEMA_VERSION},
        )

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
        # Atomic writes are serialized per target path by atomic_write_text.
        # Do not take the global storage lock here: high-fan-in workflows such
        # as many typed explode nodes writing to attachfragment should not make every
        # unrelated JSON write wait behind one global lock. Multi-file updates
        # still take their own node/job interprocess locks at the call site.
        self.atomic_write_text(path, self.json_text(path, data))

    def read_json(self, path: Path, default: Any = None) -> Any:
        path = Path(path)

        def read_text_or_none():
            try:
                return path.read_text(encoding="utf-8")
            except FileNotFoundError:
                return None

        # Windows can briefly deny reading a file that another process is
        # replacing. Treat that like a transient filesystem condition. Also
        # retry JSON decoding a few times in case a third-party tool has the
        # file open while it is being swapped.
        last_decode_error = None
        thread_lock = self.thread_lock_for(path)
        for attempt in range(20):
            with thread_lock:
                text = self.retry_fs(read_text_or_none, attempts=8, base_delay=0.01)
            if text is None:
                return default
            try:
                return json.loads(text)
            except json.JSONDecodeError as error:
                last_decode_error = error
                time.sleep(min(0.25, 0.01 * (attempt + 1)))

        raise last_decode_error
