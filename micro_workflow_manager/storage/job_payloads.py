from __future__ import annotations

import hashlib
import json
import os
import shutil
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable
from uuid import uuid4

from micro_workflow_manager.models import Job, QUEUED


class JobPayloadStorageMixin:
    """Filesystem output and returned-file handling."""

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
