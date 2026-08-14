from __future__ import annotations

import json
import os
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Iterator

from .file_helpers import (
    _copy_file,
    _mkdir,
    _relative_parts,
    _source_path,
    _write_bytes_file,
    _write_text_file,
)
from .paths import relative_path


def _read_json_path(path: Path, encoding: str) -> Any:
    """Read one already-contained immutable bulk-input path."""

    return json.loads(path.read_text(encoding=encoding))


def _is_node_input_filesystem(filesystem: Any) -> bool:
    # Import lazily to avoid the file_systems -> file_entry module cycle.
    # MWF 0.4.3 originally referenced NodeInputFileSystem here without binding
    # the name, so every FileSystemEntry.mkdir/open write path raised NameError.
    from .file_systems import NodeInputFileSystem

    return isinstance(filesystem, NodeInputFileSystem)


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
        if _is_node_input_filesystem(self.filesystem):
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
            if _is_node_input_filesystem(self.filesystem):
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

    def read_jsons(
        self,
        pattern: str = "*.json",
        *,
        recursive: bool = True,
        encoding: str | None = None,
    ) -> list[tuple[str, Any]]:
        """Read a bounded JSON set under one execution-generation check.

        Calling ``entry.read_json()`` thousands of times is correct but each
        path resolution repeats the scheduler cancellation/generation check.
        A fan-in stage already owns one immutable input frontier, so validate
        it once before and after a bulk read. Every candidate is still resolved
        and checked against the bound root to reject symlink/path escapes.
        Returned paths are relative POSIX names, sorted deterministically.
        """

        _relative_parts(pattern.replace("*", "x").replace("?", "x"))
        # Resolving the bound root performs the pre-read execution check.
        root = self.path
        root_resolved = root.resolve()
        paths: Iterator[Path] = (
            root.rglob(pattern) if recursive else root.glob(pattern)
        )
        contained: list[tuple[str, Path]] = []
        for path in sorted(paths):
            if not path.is_file():
                continue
            resolved = path.resolve()
            try:
                relative = relative_path(resolved, root_resolved)
            except ValueError:
                continue
            contained.append((PurePosixPath(*relative.parts).as_posix(), resolved))
        read_encoding = encoding or self.filesystem.encoding
        # Windows security/VPN filesystem filters can add latency to each open
        # even when the disk and Python process are otherwise idle. A bounded
        # bulk pool overlaps that independent per-file wait without retaining
        # handles after the call or weakening deterministic result order.
        workers = min(32, max(1, len(contained)))
        if workers == 1:
            values = [
                _read_json_path(path, read_encoding)
                for _relative, path in contained
            ]
        else:
            with ThreadPoolExecutor(
                max_workers=workers,
                thread_name_prefix="mwf-json-read",
            ) as executor:
                values = list(executor.map(
                    lambda item: _read_json_path(item[1], read_encoding),
                    contained,
                ))
        results = [
            (relative, value)
            for (relative, _path), value in zip(contained, values)
        ]
        self.ctx.raise_if_cancelled()
        return results

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
                relative = relative_path(resolved, root_resolved)
            except ValueError:
                continue
            result.append(self.file(*relative.parts))
        return result
