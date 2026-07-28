from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .file_entry import FileSystemEntry
from .paths import relative_posix
from .file_helpers import (
    _copy_file,
    _format_template,
    _relative_parts,
    _write_bytes_file,
    _write_text_file,
)


@dataclass(frozen=True, slots=True)
class FileSystem:
    """Declarative description of one workflow filesystem scope.

    A filesystem object is normally defined once beside a ``NodeRouter`` and
    reused by the task. ``base`` may be a readable ``str.format`` template.

    The base class describes the shared API. Use ``InputFileSystem``,
    ``OutputFileSystem``, ``JobFileSystem``, or ``NodeInputFileSystem`` in node
    behavior files.
    """

    label: str = "filesystem"
    base: str = ""
    encoding: str = "utf-8"

    scope: str = "filesystem"
    node_name: str | None = None
    writable: bool = False

    def __post_init__(self) -> None:
        for field_name in ("label", "base", "encoding", "scope"):
            value = getattr(self, field_name)
            if not isinstance(value, str):
                raise TypeError(f"{field_name} must be a string")
        if not self.label.strip():
            raise ValueError("label must not be empty")

    def bind(self, ctx, /, **values: Any) -> "FileSystemEntry":
        return FileSystemEntry(self, ctx, _format_template(self.base, values))

    def file(
        self,
        ctx,
        *parts: str | os.PathLike[str],
        **values: Any,
    ) -> "FileSystemEntry":
        return self.bind(ctx, **values).file(*parts)

    def directory(
        self,
        ctx,
        *parts: str | os.PathLike[str],
        **values: Any,
    ) -> "FileSystemEntry":
        return self.bind(ctx, **values).directory(*parts)

    def files(
        self,
        ctx,
        pattern: str = "*",
        *,
        recursive: bool = False,
        files_only: bool = True,
        **values: Any,
    ) -> list["FileSystemEntry"]:
        """List entries below this filesystem's bound base directory."""
        root = self.bind(ctx, **values)
        entries = root.rglob(pattern) if recursive else root.glob(pattern)
        if files_only:
            entries = [entry for entry in entries if entry.is_file()]
        return entries

    def describe(self) -> dict[str, Any]:
        """Return the small human-readable declaration behind this object."""
        return {
            "kind": type(self).__name__,
            "label": self.label,
            "scope": self.scope,
            "node": self.node_name,
            "base": self.base,
            "encoding": self.encoding,
            "writable": self.writable,
        }

    def _resolve(self, ctx, parts: tuple[str, ...]) -> Path:
        raise NotImplementedError

    def _write_text(
        self,
        ctx,
        relative: str,
        content: str,
        *,
        overwrite: bool,
        encoding: str,
    ) -> Path:
        raise PermissionError(f"{self.label} is read-only")

    def _write_bytes(
        self,
        ctx,
        relative: str,
        content: bytes,
        *,
        overwrite: bool,
    ) -> Path:
        raise PermissionError(f"{self.label} is read-only")

    def _copy_from(
        self,
        ctx,
        relative: str,
        source: Path,
        *,
        overwrite: bool,
    ) -> Path:
        raise PermissionError(f"{self.label} is read-only")

    def _append_text(self, ctx, relative: str, content: str, *, encoding: str) -> Path:
        raise PermissionError(f"{self.label} is read-only")

    def _delete(self, ctx, relative: str, *, missing_ok: bool) -> None:
        raise PermissionError(f"{self.label} is read-only")

    def __repr__(self) -> str:
        fields = [f"label={self.label!r}"]
        if self.node_name is not None:
            fields.append(f"node={self.node_name!r}")
        if self.base:
            fields.append(f"base={self.base!r}")
        return f"{type(self).__name__}({', '.join(fields)})"

class InputFileSystem(FileSystem):
    """Read-only input folder of the current node."""

    def __init__(
        self,
        label: str = "input",
        *,
        base: str = "",
        encoding: str = "utf-8",
    ):
        super().__init__(
            label=label,
            base=base,
            encoding=encoding,
            scope="input",
            writable=False,
        )

    def _resolve(self, ctx, parts: tuple[str, ...]) -> Path:
        return ctx.input_path(*parts)

class OutputFileSystem(FileSystem):
    """Persistent output folder of the current node."""

    def __init__(
        self,
        label: str = "output",
        *,
        base: str = "",
        encoding: str = "utf-8",
    ):
        super().__init__(
            label=label,
            base=base,
            encoding=encoding,
            scope="output",
            writable=True,
        )

    def _resolve(self, ctx, parts: tuple[str, ...]) -> Path:
        return ctx.output_path(*parts)

    def _write_text(
        self,
        ctx,
        relative: str,
        content: str,
        *,
        overwrite: bool,
        encoding: str,
    ) -> Path:
        target = ctx.system.storage.output_path(ctx.current_node, relative)
        path = ctx._guarded(
            lambda: _write_text_file(
                ctx.system.storage, target, content, encoding=encoding, overwrite=overwrite
            )
        )
        ctx._record_output(path, content, scope=self.scope)
        return path

    def _write_bytes(self, ctx, relative: str, content: bytes, *, overwrite: bool) -> Path:
        target = ctx.system.storage.output_path(ctx.current_node, relative)
        path = ctx._guarded(
            lambda: _write_bytes_file(
                ctx.system.storage, target, content, overwrite=overwrite
            )
        )
        ctx._record_output(path, content, scope=self.scope)
        return path

    def _copy_from(self, ctx, relative: str, source: Path, *, overwrite: bool) -> Path:
        target = ctx.system.storage.output_path(ctx.current_node, relative)
        path = ctx._guarded(
            lambda: _copy_file(
                ctx.system.storage, source, target, overwrite=overwrite
            )
        )
        ctx._record_event(
            "output_written",
            path=(
                f"output/jobs/{ctx.job_id}/files/{relative_posix(path, ctx.files_dir)}"
                if self.scope == "job_files"
                else f"output/{relative_posix(path, ctx.output_dir)}"
            ),
            content_type="file", source=str(source),
            size=path.stat().st_size if path.exists() else None,
        )
        return path

    def _append_text(self, ctx, relative: str, content: str, *, encoding: str) -> Path:
        target = ctx.system.storage.output_path(ctx.current_node, relative)
        path = ctx._guarded(
            lambda: ctx.system.storage.append_text(target, content, encoding=encoding)
        )
        ctx._record_output(path, content, scope=self.scope)
        return path

    def _delete(self, ctx, relative: str, *, missing_ok: bool) -> None:
        target = ctx.system.storage.output_path(ctx.current_node, relative)

        def remove():
            if missing_ok:
                ctx.system.storage.remove_if_exists(target)
            else:
                target.unlink()

        ctx._guarded(remove)

class JobFileSystem(FileSystem):
    """Returned files folder of the current job (``jobs/<id>/files``)."""

    def __init__(
        self,
        label: str = "job files",
        *,
        base: str = "",
        encoding: str = "utf-8",
    ):
        super().__init__(
            label=label,
            base=base,
            encoding=encoding,
            scope="job_files",
            writable=True,
        )

    def _resolve(self, ctx, parts: tuple[str, ...]) -> Path:
        return ctx.system.storage.safe_join(ctx.files_dir, *parts)

    def _write_text(
        self,
        ctx,
        relative: str,
        content: str,
        *,
        overwrite: bool,
        encoding: str,
    ) -> Path:
        target = ctx.system.storage.safe_join(
            ctx.system.storage.files_dir(ctx.current_node, ctx.job_id), relative
        )
        path = ctx._guarded(
            lambda: _write_text_file(
                ctx.system.storage, target, content, encoding=encoding, overwrite=overwrite
            )
        )
        ctx._record_output(path, content, scope=self.scope)
        return path

    def _write_bytes(self, ctx, relative: str, content: bytes, *, overwrite: bool) -> Path:
        target = ctx.system.storage.safe_join(
            ctx.system.storage.files_dir(ctx.current_node, ctx.job_id), relative
        )
        path = ctx._guarded(
            lambda: _write_bytes_file(
                ctx.system.storage, target, content, overwrite=overwrite
            )
        )
        ctx._record_output(path, content, scope=self.scope)
        return path

    def _copy_from(self, ctx, relative: str, source: Path, *, overwrite: bool) -> Path:
        target = ctx.system.storage.safe_join(
            ctx.system.storage.files_dir(ctx.current_node, ctx.job_id), relative
        )
        path = ctx._guarded(
            lambda: _copy_file(
                ctx.system.storage, source, target, overwrite=overwrite
            )
        )
        ctx._record_event(
            "output_written",
            path=(
                f"output/jobs/{ctx.job_id}/files/{relative_posix(path, ctx.files_dir)}"
                if self.scope == "job_files"
                else f"output/{relative_posix(path, ctx.output_dir)}"
            ),
            content_type="file", source=str(source),
            size=path.stat().st_size if path.exists() else None,
        )
        return path

    def _append_text(self, ctx, relative: str, content: str, *, encoding: str) -> Path:
        target = ctx.system.storage.safe_join(
            ctx.system.storage.files_dir(ctx.current_node, ctx.job_id), relative
        )
        path = ctx._guarded(
            lambda: ctx.system.storage.append_text(target, content, encoding=encoding)
        )
        ctx._record_output(path, content, scope=self.scope)
        return path

    def _delete(self, ctx, relative: str, *, missing_ok: bool) -> None:
        target = ctx.system.storage.safe_join(
            ctx.system.storage.files_dir(ctx.current_node, ctx.job_id), relative
        )

        def remove():
            if missing_ok:
                ctx.system.storage.remove_if_exists(target)
            else:
                target.unlink()

        ctx._guarded(remove)

class NodeInputFileSystem(FileSystem):
    """Input folder and job route for another graph node."""

    def __init__(
        self,
        node_name: str,
        label: str | None = None,
        *,
        base: str = "",
        encoding: str = "utf-8",
    ):
        if not isinstance(node_name, str) or not node_name:
            raise ValueError("node_name must be a non-empty string")
        super().__init__(
            label=label or f"{node_name} input",
            base=base,
            encoding=encoding,
            scope="node_input",
            node_name=node_name,
            writable=True,
        )

    def handle(self, ctx):
        return ctx.node(self.node_name)

    def add_job(self, ctx, **params):
        return self.handle(ctx).add(**params)

    def add_jobs(
        self,
        ctx,
        params_list: list[dict[str, Any]],
        *,
        autostart: bool = False,
        idempotency_keys: list[str | None] | None = None,
    ):
        return self.handle(ctx).add_many(
            params_list,
            autostart=autostart,
            idempotency_keys=idempotency_keys,
        )

    def write_jsons(
        self,
        ctx,
        entries: list[tuple[str, Any]],
        *,
        overwrite: bool = False,
    ) -> list[Path]:
        texts = [
            (
                filename,
                json.dumps(value, ensure_ascii=False, indent=2) + "\n",
            )
            for filename, value in entries
        ]
        return self.handle(ctx).write_inputs(
            texts,
            overwrite=overwrite,
            encoding=self.encoding,
        )

    add = add_job

    def _resolve(self, ctx, parts: tuple[str, ...]) -> Path:
        return self.handle(ctx).input_path(*parts)

    def _write_text(
        self,
        ctx,
        relative: str,
        content: str,
        *,
        overwrite: bool,
        encoding: str,
    ) -> Path:
        if encoding.lower().replace("_", "-") == "utf-8":
            return self.handle(ctx).write_input(relative, content, overwrite=overwrite)
        return self.handle(ctx).write_input_bytes(
            relative, content.encode(encoding), overwrite=overwrite
        )

    def _write_bytes(self, ctx, relative: str, content: bytes, *, overwrite: bool) -> Path:
        return self.handle(ctx).write_input_bytes(relative, content, overwrite=overwrite)

    def _copy_from(self, ctx, relative: str, source: Path, *, overwrite: bool) -> Path:
        return self.handle(ctx).add_input_file(
            source,
            filename=relative,
            overwrite=overwrite,
        )

    def _append_text(self, ctx, relative: str, content: str, *, encoding: str) -> Path:
        handle = self.handle(ctx)
        target = ctx.system.storage.input_path(self.node_name, relative)
        return handle._guarded(
            lambda: ctx.system.storage.append_text(target, content, encoding=encoding)
        )

    def _delete(self, ctx, relative: str, *, missing_ok: bool) -> None:
        handle = self.handle(ctx)
        target = ctx.system.storage.input_path(self.node_name, relative)

        def remove():
            if missing_ok:
                ctx.system.storage.remove_if_exists(target)
            else:
                target.unlink()

        handle._guarded(remove)
