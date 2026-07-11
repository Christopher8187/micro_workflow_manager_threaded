from pathlib import Path
from typing import Any, Callable, TypeVar

from .models import Job


T = TypeVar("T")


class NodeHandle:
    def __init__(
        self,
        system,
        from_node: str,
        from_job_id: int,
        to_node: str,
        execution_generation: int,
        execution_id: str | None,
    ):
        self.system = system
        self.from_node = from_node
        self.from_job_id = from_job_id
        self.to_node = to_node
        self.execution_generation = execution_generation
        self.execution_id = execution_id

    def _guarded(self, action: Callable[[], T]) -> T:
        if self.execution_id is None:
            return action()
        return self.system.run_job_side_effect(
            self.from_node,
            self.from_job_id,
            self.execution_generation,
            self.execution_id,
            action,
        )

    def checkpoint(self):
        """Raise immediately if the parent job has been manually restarted."""
        if self.execution_id is not None:
            self.system.check_job_execution(
                self.from_node,
                self.from_job_id,
                self.execution_generation,
                self.execution_id,
            )

    def add(
        self,
        job_id: int | None = None,
        autostart: bool = False,
        **params,
    ):
        return self._guarded(
            lambda: self.system.add_job(
                from_node=self.from_node,
                to_node=self.to_node,
                job_id=job_id,
                autostart=autostart,
                _parent_job_id=self.from_job_id,
                **params,
            )
        )

    @property
    def input_dir(self) -> Path:
        """The input folder for the downstream node."""
        return self._guarded(lambda: self.system.storage.node_input_dir(self.to_node))

    def input_path(self, *parts: str) -> Path:
        """Build a safe path inside the downstream node's input folder."""
        return self._guarded(lambda: self.system.storage.input_path(self.to_node, *parts))

    def write_input(
        self,
        filename: str,
        content: str,
        *,
        overwrite: bool = False,
    ) -> Path:
        """Write text into the downstream node's input folder."""
        return self._guarded(
            lambda: self.system.storage.write_node_input_text(
                self.to_node,
                filename,
                content,
                overwrite=overwrite,
            )
        )

    def write_input_bytes(
        self,
        filename: str,
        content: bytes,
        *,
        overwrite: bool = False,
    ) -> Path:
        """Write bytes into the downstream node's input folder."""
        return self._guarded(
            lambda: self.system.storage.write_node_input_bytes(
                self.to_node,
                filename,
                content,
                overwrite=overwrite,
            )
        )

    def add_input_file(
        self,
        source: str | Path,
        filename: str | None = None,
        *,
        overwrite: bool = False,
    ) -> Path:
        """Copy one file into the downstream node's input folder.

        This is the replacement for output-folder-triggered job creation. The
        current job can place concrete files where a later node can read them
        through ``ctx.input_files(...)`` or ``ctx.input_path(...)``.
        """
        return self._guarded(
            lambda: self.system.storage.copy_to_node_input(
                self.to_node,
                source,
                filename=filename,
                overwrite=overwrite,
            )
        )

    def add_input_files(
        self,
        sources,
        *,
        overwrite: bool = False,
    ) -> list[Path]:
        """Copy several files into the downstream node's input folder."""
        return [
            self.add_input_file(source, overwrite=overwrite)
            for source in sources
        ]

    # Short aliases for code that reads naturally in node behavior files.
    add_file = add_input_file
    add_files = add_input_files


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
        *,
        execution_generation: int,
        execution_id: str | None,
    ):
        self.system = system
        self.current_node = current_node
        self.current_job = current_job
        self.current_task = current_task
        self.attempt = attempt
        self.repeat_index = repeat_index
        self.error = error
        self.execution_generation = execution_generation
        self.execution_id = execution_id

    def _guarded(self, action: Callable[[], T]) -> T:
        if self.execution_id is None:
            return action()
        return self.system.run_job_side_effect(
            self.current_node,
            self.job_id,
            self.execution_generation,
            self.execution_id,
            action,
        )

    def checkpoint(self):
        """Raise immediately if this job has been manually restarted.

        Context mutation helpers use a stronger cross-process guard. Long custom
        loops may call this lightweight check between expensive operations.
        """
        if self.execution_id is not None:
            self.system.check_job_execution(
                self.current_node,
                self.job_id,
                self.execution_generation,
                self.execution_id,
            )

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
        return self._guarded(lambda: self.system.storage.node_output_dir(self.current_node))

    @property
    def storage_dir(self) -> Path:
        return self._guarded(lambda: self.system.storage.job_dir(self.current_node, self.job_id))

    @property
    def files_dir(self) -> Path:
        return self._guarded(lambda: self.system.storage.files_dir(self.current_node, self.job_id))

    def input_path(self, *parts: str) -> Path:
        return self.system.storage.input_path(self.current_node, *parts)

    def output_path(self, *parts: str) -> Path:
        return self._guarded(lambda: self.system.storage.output_path(self.current_node, *parts))

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
        return self._guarded(
            lambda: self.system.storage.output_files(
                self.current_node,
                pattern=pattern,
                recursive=recursive,
                files_only=files_only,
            )
        )

    def write(self, filename: str, content: str) -> Path:
        return self._guarded(
            lambda: self.system.storage.write_text(
                self.current_node,
                self.job_id,
                filename,
                content,
            )
        )

    def write_bytes(self, filename: str, content: bytes) -> Path:
        return self._guarded(
            lambda: self.system.storage.write_bytes(
                self.current_node,
                self.job_id,
                filename,
                content,
            )
        )

    def write_output(self, filename: str, content: str) -> Path:
        return self._guarded(
            lambda: self.system.storage.write_node_output_text(
                self.current_node,
                filename,
                content,
            )
        )

    def write_output_bytes(self, filename: str, content: bytes) -> Path:
        return self._guarded(
            lambda: self.system.storage.write_node_output_bytes(
                self.current_node,
                filename,
                content,
            )
        )

    def debug(self, message: str):
        self._guarded(lambda: self.system.storage.write_debug(self.current_node, message))

    def node(self, node_name: str) -> NodeHandle:
        self.system.validate_edge(self.current_node, node_name)

        return NodeHandle(
            system=self.system,
            from_node=self.current_node,
            from_job_id=self.job_id,
            to_node=node_name,
            execution_generation=self.execution_generation,
            execution_id=self.execution_id,
        )
