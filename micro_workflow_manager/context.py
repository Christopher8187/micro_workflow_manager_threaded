from pathlib import Path
from typing import Any

from .models import Job


class NodeHandle:
    def __init__(self, system, from_node: str, from_job_id: int, to_node: str):
        self.system = system
        self.from_node = from_node
        self.from_job_id = from_job_id
        self.to_node = to_node

    def add(
        self,
        job_id: int | None = None,
        autostart: bool = False,
        **params,
    ):
        return self.system.add_job(
            from_node=self.from_node,
            to_node=self.to_node,
            job_id=job_id,
            autostart=autostart,
            _parent_job_id=self.from_job_id,
            **params,
        )

    def add_from_output_files(
        self,
        pattern: str = "*",
        *,
        file_param: str = "output_file",
        autostart: bool = False,
        recursive: bool = False,
        files_only: bool = True,
        path_mode: str = "absolute",
        dedupe: bool = True,
        **params,
    ):
        """Create downstream jobs from this node's output files after the node finishes.

        The jobs are intentionally deferred because the node-level output folder
        may not contain every file until all jobs in the current node have
        finished. Each matched file becomes one downstream job, with the file
        path placed in ``file_param``.
        """
        return self.system.defer_output_file_jobs(
            from_node=self.from_node,
            to_node=self.to_node,
            pattern=pattern,
            file_param=file_param,
            autostart=autostart,
            recursive=recursive,
            files_only=files_only,
            path_mode=path_mode,
            dedupe=dedupe,
            _parent_job_id=self.from_job_id,
            **params,
        )

    # Short alias for code that reads naturally in node behavior files.
    add_from_outputs = add_from_output_files


class JobContext:
    def __init__(
        self,
        system,
        current_node: str,
        current_job: Job,
        current_task: str,
        attempt: int,
        repeat_index: int,
        error: Exception | None = None,
    ):
        self.system = system
        self.current_node = current_node
        self.current_job = current_job
        self.current_task = current_task
        self.attempt = attempt
        self.repeat_index = repeat_index
        self.error = error

    @property
    def job_id(self) -> int:
        return self.current_job.job_id

    @property
    def params(self) -> dict[str, Any]:
        return self.current_job.params

    @property
    def input_dir(self) -> Path:
        return self.system.storage.node_input_dir(self.current_node)

    @property
    def output_dir(self) -> Path:
        return self.system.storage.node_output_dir(self.current_node)

    @property
    def storage_dir(self) -> Path:
        return self.system.storage.job_dir(self.current_node, self.job_id)

    @property
    def files_dir(self) -> Path:
        return self.system.storage.files_dir(self.current_node, self.job_id)

    def input_path(self, *parts: str) -> Path:
        return self.system.storage.input_path(self.current_node, *parts)

    def output_path(self, *parts: str) -> Path:
        return self.system.storage.output_path(self.current_node, *parts)

    def input_files(
        self,
        pattern: str = "*",
        recursive: bool = False,
        files_only: bool = True,
    ) -> list[Path]:
        return self.system.storage.input_files(
            self.current_node,
            pattern=pattern,
            recursive=recursive,
            files_only=files_only,
        )

    def output_files(
        self,
        pattern: str = "*",
        recursive: bool = False,
        files_only: bool = True,
    ) -> list[Path]:
        return self.system.storage.output_files(
            self.current_node,
            pattern=pattern,
            recursive=recursive,
            files_only=files_only,
        )

    def write(self, filename: str, content: str) -> Path:
        return self.system.storage.write_text(
            self.current_node,
            self.job_id,
            filename,
            content,
        )

    def write_bytes(self, filename: str, content: bytes) -> Path:
        return self.system.storage.write_bytes(
            self.current_node,
            self.job_id,
            filename,
            content,
        )

    def write_output(self, filename: str, content: str) -> Path:
        return self.system.storage.write_node_output_text(
            self.current_node,
            filename,
            content,
        )

    def write_output_bytes(self, filename: str, content: bytes) -> Path:
        return self.system.storage.write_node_output_bytes(
            self.current_node,
            filename,
            content,
        )

    def debug(self, message: str):
        self.system.storage.write_debug(self.current_node, message)

    def node(self, node_name: str) -> NodeHandle:
        self.system.validate_edge(self.current_node, node_name)

        return NodeHandle(
            system=self.system,
            from_node=self.current_node,
            from_job_id=self.job_id,
            to_node=node_name,
        )
