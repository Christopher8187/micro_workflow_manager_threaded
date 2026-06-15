import json
import os
import threading
from pathlib import Path
from shutil import copy2
from threading import RLock
from typing import Any

from .models import Job, QUEUED, VALID_STATUSES


class FileStorage:
    def __init__(self, project_dir: str | Path):
        self.project_dir = Path(project_dir).resolve()
        self.lock = RLock()
        self.project_dir.mkdir(parents=True, exist_ok=True)

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

    def atomic_write_json(self, path: Path, data: Any):
        path.parent.mkdir(parents=True, exist_ok=True)
        text = self.json_text(path, data)
        temp = path.with_name(
            f".{path.name}.{os.getpid()}.{threading.get_ident()}.tmp"
        )

        try:
            temp.write_text(text, encoding="utf-8")
            temp.replace(path)
        finally:
            if temp.exists():
                temp.unlink()

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

    def write_node_output_text(
        self,
        node_name: str,
        filename: str,
        content: str,
    ) -> Path:
        path = self.safe_join(self.node_output_dir(node_name), filename)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
        return path

    def write_node_output_bytes(
        self,
        node_name: str,
        filename: str,
        content: bytes,
    ) -> Path:
        path = self.safe_join(self.node_output_dir(node_name), filename)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
        return path

    def write_node_schema(
        self,
        node_name: str,
        allowed_params: set[str],
        required_params: set[str],
        retries: int,
        repeats: int,
        fallbacks: list[str],
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

        with self.lock:
            with self.debug_file(node_name).open("a", encoding="utf-8") as file:
                file.write(f"[{timestamp}] {message}\n")

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
        data = self.read_json(self.status_file(node_name, job_id), default=None)

        if data is None:
            return None

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
            status_data = self.read_json(self.status_file(node_name, job_id), default={})

            row = {
                **job_data,
                **status_data,
            }

            if status is None or row.get("status") == status:
                rows.append(row)

        return rows

    def queued_jobs(self, node_name: str) -> list[Job]:
        jobs = []

        for row in self.list_jobs(node_name, status=QUEUED):
            jobs.append(self.load_job(node_name, row["job_id"]))

        return jobs

    def write_output(self, node_name: str, job_id: int, data: dict):
        self.validate_job_id(job_id)
        self.atomic_write_json(
            self.output_file(node_name, job_id),
            data,
        )

    def write_text(self, node_name: str, job_id: int, filename: str, content: str) -> Path:
        path = self.safe_join(self.files_dir(node_name, job_id), filename)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
        return path

    def write_bytes(self, node_name: str, job_id: int, filename: str, content: bytes) -> Path:
        path = self.safe_join(self.files_dir(node_name, job_id), filename)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
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
            copy2(source, target)
            stored.append(str(target))

        return stored
