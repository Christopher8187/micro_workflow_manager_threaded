from __future__ import annotations

import ntpath
import os
import re
from pathlib import Path, PurePath, PureWindowsPath

MWF_DIR_NAME = ".mwf"
MWF_CONFIG_NAME = "project.json"
MWF_RUN_NAME = "run.json"
MWF_THREADS_NAME = "threads.json"
MWF_LOCKS_NAME = "locks"
MWF_DEPLOY_NAME = "deploy"
MWF_STATE_DATABASE_NAME = "state.sqlite3"

LEGACY_CONFIG_NAME = ".mwf"
LEGACY_RUN_NAME = ".mwf_run.json"
LEGACY_THREADS_NAME = ".mwf_threads.json"
LEGACY_LOCKS_NAME = ".mwf_locks"


def mwf_dir(root: Path) -> Path:
    return root / MWF_DIR_NAME


def config_file(root: Path) -> Path:
    return mwf_dir(root) / MWF_CONFIG_NAME


def run_file(root: Path) -> Path:
    return mwf_dir(root) / MWF_RUN_NAME


def threads_file(root: Path) -> Path:
    return mwf_dir(root) / MWF_THREADS_NAME


def locks_dir(root: Path) -> Path:
    return mwf_dir(root) / MWF_LOCKS_NAME


def deploy_dir(root: Path) -> Path:
    return mwf_dir(root) / MWF_DEPLOY_NAME


def state_database_file(root: Path) -> Path:
    return mwf_dir(root) / MWF_STATE_DATABASE_NAME



def _strip_windows_extended_prefix(value: str) -> str:
    """Return a normal Windows spelling for an extended-length path alias."""
    if value.startswith("\\\\?\\UNC\\"):
        return "\\\\" + value[8:]
    if value.startswith("\\\\?\\"):
        return value[4:]
    return value


def _looks_like_windows_path(value: str) -> bool:
    return (
        value.startswith("\\\\")
        or value.startswith("\\\\?\\")
        or re.match(r"^[A-Za-z]:[\\/]", value) is not None
    )


def relative_path(path: str | os.PathLike[str], base: str | os.PathLike[str]) -> PurePath:
    """Return *path* relative to *base*, accepting Windows extended aliases.

    Windows may return a deep path as ``\\\\?\\C:\\...`` while its root remains
    ``C:\\...``. ``Path.relative_to`` rejects those two spellings even though
    they identify the same directory tree. This helper strips only that alias
    prefix for comparison and display-path construction. It retains the same
    containment semantics and raises ``ValueError`` for paths outside *base*.
    """
    path_text = os.fspath(path)
    base_text = os.fspath(base)
    if (
        os.name == "nt"
        or _looks_like_windows_path(path_text)
        or _looks_like_windows_path(base_text)
    ):
        path_plain = ntpath.normpath(_strip_windows_extended_prefix(path_text))
        base_plain = ntpath.normpath(_strip_windows_extended_prefix(base_text))
        try:
            common = ntpath.commonpath(
                [ntpath.normcase(base_plain), ntpath.normcase(path_plain)]
            )
        except ValueError as error:
            raise ValueError(f"{path_text!r} is not in the subpath of {base_text!r}") from error
        if common != ntpath.normcase(base_plain):
            raise ValueError(f"{path_text!r} is not in the subpath of {base_text!r}")
        return PureWindowsPath(ntpath.relpath(path_plain, base_plain))

    return Path(path).relative_to(Path(base))


def relative_posix(path: str | os.PathLike[str], base: str | os.PathLike[str]) -> str:
    """Return a forward-slash relative path suitable for trace/event records."""
    return relative_path(path, base).as_posix()
