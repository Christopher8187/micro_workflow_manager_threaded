from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Iterator


_WINDOWS_ABSOLUTE = re.compile(r"^[A-Za-z]:[\\/]")


def _relative_parts(*parts: str | os.PathLike[str]) -> tuple[str, ...]:
    """Normalize user-facing paths to portable, safe relative parts."""
    normalized: list[str] = []
    for raw in parts:
        text = os.fspath(raw)
        if not isinstance(text, str):
            raise TypeError("filesystem path parts must be strings or path-like objects")
        if not text or text == ".":
            continue
        if text.startswith(("/", "\\")) or _WINDOWS_ABSOLUTE.match(text):
            raise ValueError(f"filesystem paths must be relative: {text}")
        for part in text.replace("\\", "/").split("/"):
            if not part or part == ".":
                continue
            if part == "..":
                raise ValueError("filesystem paths cannot contain '..'")
            normalized.append(part)
    return tuple(normalized)


def _format_template(template: str, values: dict[str, Any]) -> tuple[str, ...]:
    if not template:
        return ()
    try:
        rendered = template.format_map(values)
    except KeyError as error:
        missing = error.args[0]
        raise ValueError(
            f"Missing filesystem template value {missing!r} for {template!r}"
        ) from error
    return _relative_parts(rendered)


def _source_path(source: "FileSystemEntry | str | os.PathLike[str]") -> Path:
    if isinstance(source, FileSystemEntry):
        return source.path
    return Path(source)


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
        return ctx._guarded(
            lambda: _write_text_file(
                ctx.system.storage, target, content, encoding=encoding, overwrite=overwrite
            )
        )

    def _write_bytes(self, ctx, relative: str, content: bytes, *, overwrite: bool) -> Path:
        target = ctx.system.storage.output_path(ctx.current_node, relative)
        return ctx._guarded(
            lambda: _write_bytes_file(
                ctx.system.storage, target, content, overwrite=overwrite
            )
        )

    def _copy_from(self, ctx, relative: str, source: Path, *, overwrite: bool) -> Path:
        target = ctx.system.storage.output_path(ctx.current_node, relative)
        return ctx._guarded(
            lambda: _copy_file(
                ctx.system.storage, source, target, overwrite=overwrite
            )
        )

    def _append_text(self, ctx, relative: str, content: str, *, encoding: str) -> Path:
        target = ctx.system.storage.output_path(ctx.current_node, relative)
        return ctx._guarded(
            lambda: ctx.system.storage.append_text(target, content, encoding=encoding)
        )

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
        return ctx._guarded(
            lambda: _write_text_file(
                ctx.system.storage, target, content, encoding=encoding, overwrite=overwrite
            )
        )

    def _write_bytes(self, ctx, relative: str, content: bytes, *, overwrite: bool) -> Path:
        target = ctx.system.storage.safe_join(
            ctx.system.storage.files_dir(ctx.current_node, ctx.job_id), relative
        )
        return ctx._guarded(
            lambda: _write_bytes_file(
                ctx.system.storage, target, content, overwrite=overwrite
            )
        )

    def _copy_from(self, ctx, relative: str, source: Path, *, overwrite: bool) -> Path:
        target = ctx.system.storage.safe_join(
            ctx.system.storage.files_dir(ctx.current_node, ctx.job_id), relative
        )
        return ctx._guarded(
            lambda: _copy_file(
                ctx.system.storage, source, target, overwrite=overwrite
            )
        )

    def _append_text(self, ctx, relative: str, content: str, *, encoding: str) -> Path:
        target = ctx.system.storage.safe_join(
            ctx.system.storage.files_dir(ctx.current_node, ctx.job_id), relative
        )
        return ctx._guarded(
            lambda: ctx.system.storage.append_text(target, content, encoding=encoding)
        )

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


@dataclass(frozen=True, slots=True)
class FileSystemEntry(os.PathLike[str]):
    """A filesystem object bound to one job context and relative path."""

    filesystem: FileSystem
    ctx: Any
    parts: tuple[str, ...] = ()

    @property
    def relative_path(self) -> str:
        return PurePosixPath(*self.parts).as_posix() if self.parts else ""

    @property
    def label(self) -> str:
        return self.filesystem.label

    @property
    def scope(self) -> str:
        return self.filesystem.scope

    @property
    def path(self) -> Path:
        return self.filesystem._resolve(self.ctx, self.parts)

    def __fspath__(self) -> str:
        return os.fspath(self.path)

    def __str__(self) -> str:
        return os.fspath(self.path)

    def __repr__(self) -> str:
        return (
            f"FileSystemEntry({self.filesystem.label!r}, "
            f"relative={self.relative_path!r})"
        )

    def __truediv__(self, part: str | os.PathLike[str]) -> "FileSystemEntry":
        return self.file(part)

    @property
    def name(self) -> str:
        return self.path.name

    @property
    def stem(self) -> str:
        return self.path.stem

    @property
    def suffix(self) -> str:
        return self.path.suffix

    @property
    def parent(self) -> "FileSystemEntry":
        if not self.parts:
            return self
        return FileSystemEntry(self.filesystem, self.ctx, self.parts[:-1])

    def file(self, *parts: str | os.PathLike[str]) -> "FileSystemEntry":
        return FileSystemEntry(
            self.filesystem,
            self.ctx,
            (*self.parts, *_relative_parts(*parts)),
        )

    directory = file
    child = file

    def exists(self) -> bool:
        return self.path.exists()

    def is_file(self) -> bool:
        return self.path.is_file()

    def is_dir(self) -> bool:
        return self.path.is_dir()

    def mkdir(self, *, parents: bool = True, exist_ok: bool = True) -> Path:
        if not self.filesystem.writable:
            raise PermissionError(f"{self.filesystem.label} is read-only")
        path = self.path
        if isinstance(self.filesystem, NodeInputFileSystem):
            return self.filesystem.handle(self.ctx)._guarded(
                lambda: _mkdir(path, parents=parents, exist_ok=exist_ok)
            )
        return self.ctx._guarded(
            lambda: _mkdir(path, parents=parents, exist_ok=exist_ok)
        )

    def read_text(self, *, encoding: str | None = None) -> str:
        return self.path.read_text(encoding=encoding or self.filesystem.encoding)

    def open(self, mode: str = "r", *args, **kwargs):
        writing = any(flag in mode for flag in "wax+")
        if writing and not self.filesystem.writable:
            raise PermissionError(f"{self.filesystem.label} is read-only")
        # Opening a writable handle cannot be transactionally fenced for the
        # lifetime of that handle. Prefer write_text/write_bytes/copy_from; this
        # method exists for Path-compatible libraries and performs a generation
        # check before opening.
        if writing:
            if isinstance(self.filesystem, NodeInputFileSystem):
                self.filesystem.handle(self.ctx).checkpoint()
            else:
                self.ctx.raise_if_cancelled()
            self.path.parent.mkdir(parents=True, exist_ok=True)
        return self.path.open(mode, *args, **kwargs)

    def resolve(self) -> Path:
        return self.path.resolve()

    def iterdir(self) -> list["FileSystemEntry"]:
        root = self.path
        return [self.file(path.name) for path in sorted(root.iterdir())]

    def read_bytes(self) -> bytes:
        return self.path.read_bytes()

    def read_json(self, *, encoding: str | None = None) -> Any:
        return json.loads(self.read_text(encoding=encoding))

    def write_text(
        self,
        content: str,
        *,
        overwrite: bool = True,
        encoding: str | None = None,
    ) -> Path:
        # Text is already decoded; ``encoding`` is accepted for Path-like
        # ergonomics and documents the intended client encoding.
        return self.filesystem._write_text(
            self.ctx,
            self.relative_path,
            content,
            overwrite=overwrite,
            encoding=encoding or self.filesystem.encoding,
        )

    def write_bytes(self, content: bytes, *, overwrite: bool = True) -> Path:
        return self.filesystem._write_bytes(
            self.ctx,
            self.relative_path,
            content,
            overwrite=overwrite,
        )

    def write_json(
        self,
        data: Any,
        *,
        indent: int | None = 2,
        ensure_ascii: bool = False,
        trailing_newline: bool = True,
        overwrite: bool = True,
    ) -> Path:
        text = json.dumps(data, indent=indent, ensure_ascii=ensure_ascii)
        if trailing_newline:
            text += "\n"
        return self.write_text(text, overwrite=overwrite)

    def append_text(self, content: str, *, encoding: str | None = None) -> Path:
        return self.filesystem._append_text(
            self.ctx,
            self.relative_path,
            content,
            encoding=encoding or self.filesystem.encoding,
        )

    def copy_from(
        self,
        source: "FileSystemEntry | str | os.PathLike[str]",
        *,
        overwrite: bool = False,
    ) -> Path:
        source_path = _source_path(source)
        if not source_path.is_file():
            raise FileNotFoundError(f"Source file does not exist: {source_path}")
        return self.filesystem._copy_from(
            self.ctx,
            self.relative_path,
            source_path,
            overwrite=overwrite,
        )

    def copy_to(self, destination: "FileSystemEntry", *, overwrite: bool = False) -> Path:
        if not isinstance(destination, FileSystemEntry):
            raise TypeError("destination must be a FileSystemEntry")
        return destination.copy_from(self, overwrite=overwrite)

    def delete(self, *, missing_ok: bool = True) -> None:
        self.filesystem._delete(self.ctx, self.relative_path, missing_ok=missing_ok)

    unlink = delete

    def glob(self, pattern: str = "*") -> list["FileSystemEntry"]:
        return self._glob(pattern, recursive=False)

    def rglob(self, pattern: str = "*") -> list["FileSystemEntry"]:
        return self._glob(pattern, recursive=True)

    def _glob(self, pattern: str, *, recursive: bool) -> list["FileSystemEntry"]:
        _relative_parts(pattern.replace("*", "x").replace("?", "x"))
        root = self.path
        paths: Iterator[Path] = root.rglob(pattern) if recursive else root.glob(pattern)
        result: list[FileSystemEntry] = []
        root_resolved = root.resolve()
        for path in sorted(paths):
            resolved = path.resolve()
            try:
                relative = resolved.relative_to(root_resolved)
            except ValueError:
                continue
            result.append(self.file(*relative.parts))
        return result


def _ensure_overwrite(target: Path, *, overwrite: bool) -> None:
    if not overwrite and target.exists():
        raise FileExistsError(f"File already exists: {target}")


def _write_text_file(storage, target: Path, content: str, *, encoding: str, overwrite: bool) -> Path:
    _ensure_overwrite(target, overwrite=overwrite)
    return storage.atomic_write_text(target, content, encoding=encoding)


def _write_bytes_file(storage, target: Path, content: bytes, *, overwrite: bool) -> Path:
    _ensure_overwrite(target, overwrite=overwrite)
    return storage.atomic_write_bytes(target, content)


def _copy_file(storage, source: Path, target: Path, *, overwrite: bool) -> Path:
    _ensure_overwrite(target, overwrite=overwrite)
    return storage.atomic_copy_file(source, target)


def _mkdir(path: Path, *, parents: bool, exist_ok: bool) -> Path:
    path.mkdir(parents=parents, exist_ok=exist_ok)
    return path
