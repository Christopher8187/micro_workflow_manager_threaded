from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any


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
    from .file_entry import FileSystemEntry

    if isinstance(source, FileSystemEntry):
        return source.path
    return Path(source)

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
