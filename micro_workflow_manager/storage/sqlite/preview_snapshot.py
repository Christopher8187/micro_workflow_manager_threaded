"""Capture native SQLite state without creating files in the project."""

from __future__ import annotations

from contextlib import contextmanager, ExitStack
from dataclasses import dataclass
import os
from pathlib import Path
import sqlite3
import stat
import tempfile
import time
from typing import Iterator

from micro_workflow_manager.project_format import is_link_or_reparse_point

from .schema import SQLiteSchemaMixin


class PreviewSnapshotBusyError(RuntimeError):
    """The project database could not be captured without a path race."""


@dataclass(frozen=True)
class _FileIdentity:
    device: int
    inode: int


@dataclass(frozen=True)
class _FileCapture:
    identity: _FileIdentity
    content: bytes


@dataclass(frozen=True)
class _OriginalCapture:
    main: _FileCapture
    wal: _FileCapture | None
    shm: _FileIdentity | None


def _identity(file_stat: os.stat_result) -> _FileIdentity:
    return _FileIdentity(file_stat.st_dev, file_stat.st_ino)


def _lstat_ordinary_file(path: Path) -> os.stat_result:
    try:
        file_stat = path.lstat()
    except FileNotFoundError:
        raise
    except OSError as error:
        raise RuntimeError(f"Cannot inspect SQLite path {path}") from error
    if is_link_or_reparse_point(path) or not stat.S_ISREG(file_stat.st_mode):
        raise RuntimeError(f"SQLite path is not an ordinary file: {path}")
    return file_stat


def _optional_file_stat(path: Path) -> os.stat_result | None:
    try:
        return _lstat_ordinary_file(path)
    except FileNotFoundError:
        return None


def _read_stable_file(path: Path) -> _FileCapture:
    first = _lstat_ordinary_file(path)
    try:
        with path.open("rb") as source:
            content = source.read()
            opened = os.fstat(source.fileno())
    except OSError as error:
        raise PreviewSnapshotBusyError(
            f"SQLite file changed while it was being captured: {path}"
        ) from error
    second = _lstat_ordinary_file(path)
    first_marker = (_identity(first), first.st_size, first.st_mtime_ns)
    opened_marker = (_identity(opened), opened.st_size, opened.st_mtime_ns)
    second_marker = (_identity(second), second.st_size, second.st_mtime_ns)
    if (
        first_marker != opened_marker
        or first_marker != second_marker
        or len(content) != first.st_size
    ):
        raise PreviewSnapshotBusyError(
            f"SQLite file changed while it was being captured: {path}"
        )
    return _FileCapture(_identity(first), content)


def _read_optional_stable_file(path: Path) -> _FileCapture | None:
    if _optional_file_stat(path) is None:
        return None
    try:
        return _read_stable_file(path)
    except FileNotFoundError as error:
        raise PreviewSnapshotBusyError(
            f"SQLite sidecar disappeared during preview capture: {path}"
        ) from error


def _read_optional_identity(path: Path) -> _FileIdentity | None:
    file_stat = _optional_file_stat(path)
    return None if file_stat is None else _identity(file_stat)


def _capture_original(database: Path) -> _OriginalCapture:
    return _OriginalCapture(
        main=_read_stable_file(database),
        wal=_read_optional_stable_file(Path(f"{database}-wal")),
        shm=_read_optional_identity(Path(f"{database}-shm")),
    )


def _require_same_identity(path: Path, expected: _FileIdentity) -> None:
    try:
        observed = _identity(_lstat_ordinary_file(path))
    except FileNotFoundError as error:
        raise PreviewSnapshotBusyError(
            f"SQLite sidecar disappeared during preview capture: {path}"
        ) from error
    if observed != expected:
        raise PreviewSnapshotBusyError(
            f"SQLite path was replaced during preview capture: {path}"
        )


@contextmanager
def _pin_windows_file(
    path: Path,
    expected: _FileIdentity,
) -> Iterator[None]:
    """Pin one ordinary Windows path without granting delete sharing."""

    if os.name != "nt":
        raise RuntimeError("Windows path pinning is unavailable")

    import ctypes
    from ctypes import wintypes
    import msvcrt

    generic_read = 0x80000000
    file_share_read = 0x00000001
    file_share_write = 0x00000002
    open_existing = 3
    file_attribute_normal = 0x00000080
    file_flag_open_reparse_point = 0x00200000
    invalid_handle = ctypes.c_void_p(-1).value

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    create_file = kernel32.CreateFileW
    create_file.argtypes = (
        wintypes.LPCWSTR,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.LPVOID,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.HANDLE,
    )
    create_file.restype = wintypes.HANDLE
    close_handle = kernel32.CloseHandle
    close_handle.argtypes = (wintypes.HANDLE,)
    close_handle.restype = wintypes.BOOL

    raw_handle = create_file(
        str(path.absolute()),
        generic_read,
        file_share_read | file_share_write,
        None,
        open_existing,
        file_attribute_normal | file_flag_open_reparse_point,
        None,
    )
    if raw_handle in (None, invalid_handle):
        error = ctypes.WinError(ctypes.get_last_error())
        raise PreviewSnapshotBusyError(
            f"Cannot pin SQLite path against removal: {path}"
        ) from error

    descriptor: int | None = None
    try:
        try:
            descriptor = msvcrt.open_osfhandle(
                int(raw_handle),
                os.O_RDONLY | getattr(os, "O_BINARY", 0),
            )
        except BaseException:
            close_handle(raw_handle)
            raise
        opened = os.fstat(descriptor)
        if (
            not stat.S_ISREG(opened.st_mode)
            or _identity(opened) != expected
        ):
            raise PreviewSnapshotBusyError(
                f"SQLite path changed before it could be pinned: {path}"
            )
        _require_same_identity(path, expected)
        yield
    finally:
        if descriptor is not None:
            os.close(descriptor)


@contextmanager
def _pin_windows_live_database(
    database: Path,
    *,
    main_identity: _FileIdentity,
    wal_identity: _FileIdentity,
    shm_identity: _FileIdentity,
) -> Iterator[None]:
    """Hold main, WAL, and SHM paths so SQLite cannot recreate any of them."""

    with ExitStack() as pins:
        pins.enter_context(_pin_windows_file(database, main_identity))
        pins.enter_context(
            _pin_windows_file(Path(f"{database}-wal"), wal_identity)
        )
        pins.enter_context(
            _pin_windows_file(Path(f"{database}-shm"), shm_identity)
        )
        yield


def _validate_database_path(database: Path) -> Path:
    database = Path(database)
    try:
        parent_stat = database.parent.lstat()
    except OSError as error:
        raise RuntimeError(
            f"Cannot inspect SQLite database directory {database.parent}"
        ) from error
    if (
        is_link_or_reparse_point(database.parent)
        or not stat.S_ISDIR(parent_stat.st_mode)
    ):
        raise RuntimeError(
            f"SQLite database directory is not an ordinary directory: "
            f"{database.parent}"
        )
    try:
        _lstat_ordinary_file(database)
    except FileNotFoundError as error:
        raise FileNotFoundError(f"SQLite database does not exist: {database}") from error
    _optional_file_stat(Path(f"{database}-wal"))
    _optional_file_stat(Path(f"{database}-shm"))
    return database


def _finish_memory_snapshot(
    source: sqlite3.Connection,
    *,
    deadline: float,
) -> sqlite3.Connection:
    memory = sqlite3.connect(":memory:")

    def require_time_remaining(
        _status: int,
        _remaining: int,
        _total: int,
    ) -> None:
        if time.monotonic() >= deadline:
            raise PreviewSnapshotBusyError(
                "SQLite snapshot backup did not finish before its deadline"
            )

    try:
        source.backup(
            memory,
            pages=256,
            progress=require_time_remaining,
            sleep=0.01,
        )
        memory.row_factory = sqlite3.Row
        SQLiteSchemaMixin.validate_native_database(memory)
        memory.execute("PRAGMA query_only = ON")
        if memory.execute("PRAGMA query_only").fetchone()[0] != 1:
            raise RuntimeError("Preview snapshot is not query-only")
        return memory
    except BaseException:
        memory.close()
        raise


def _open_pinned_live_snapshot(
    database: Path,
    *,
    main_identity: _FileIdentity,
    wal_identity: _FileIdentity,
    shm_identity: _FileIdentity,
    deadline: float,
) -> sqlite3.Connection:
    wal_path = Path(f"{database}-wal")
    shm_path = Path(f"{database}-shm")
    _require_same_identity(database, main_identity)
    _require_same_identity(wal_path, wal_identity)
    _require_same_identity(shm_path, shm_identity)

    source: sqlite3.Connection | None = None
    memory: sqlite3.Connection | None = None
    try:
        uri = f"{database.resolve(strict=True).as_uri()}?mode=ro&cache=private"
        timeout = max(0.0, min(0.25, deadline - time.monotonic()))
        source = sqlite3.connect(uri, uri=True, timeout=timeout)
        source.execute("PRAGMA query_only = ON")
        source.execute("BEGIN")
        source.execute("SELECT count(*) FROM sqlite_master").fetchone()
        _require_same_identity(database, main_identity)
        _require_same_identity(wal_path, wal_identity)
        _require_same_identity(shm_path, shm_identity)
        memory = _finish_memory_snapshot(source, deadline=deadline)
    except sqlite3.Error as error:
        raise PreviewSnapshotBusyError(
            "SQLite live snapshot could not acquire a stable read view"
        ) from error
    finally:
        if source is not None:
            source.close()

    try:
        # A live writer may legitimately append to WAL after the read view is
        # established. Identity checks reject removal or replacement without
        # confusing another process's committed bytes with preview mutations.
        _require_same_identity(database, main_identity)
        _require_same_identity(wal_path, wal_identity)
        _require_same_identity(shm_path, shm_identity)
        return memory
    except BaseException:
        if memory is not None:
            memory.close()
        raise


def _open_live_snapshot(
    database: Path,
    *,
    main_identity: _FileIdentity,
    wal_identity: _FileIdentity,
    shm_identity: _FileIdentity,
    deadline: float,
) -> sqlite3.Connection:
    with _pin_windows_live_database(
        database,
        main_identity=main_identity,
        wal_identity=wal_identity,
        shm_identity=shm_identity,
    ):
        return _open_pinned_live_snapshot(
            database,
            main_identity=main_identity,
            wal_identity=wal_identity,
            shm_identity=shm_identity,
            deadline=deadline,
        )


def _write_private_file(path: Path, content: bytes) -> None:
    try:
        with path.open("xb") as destination:
            destination.write(content)
            destination.flush()
            os.fsync(destination.fileno())
    except OSError as error:
        raise RuntimeError(f"Cannot create private SQLite snapshot file {path}") from error


def _open_private_copy(
    captured: _OriginalCapture,
    *,
    deadline: float,
) -> sqlite3.Connection:
    with tempfile.TemporaryDirectory(prefix="mwf-preview-") as temporary:
        copied_database = Path(temporary) / "state.sqlite3"
        _write_private_file(copied_database, captured.main.content)
        if captured.wal is not None:
            _write_private_file(
                Path(f"{copied_database}-wal"),
                captured.wal.content,
            )
        source: sqlite3.Connection | None = None
        try:
            source = sqlite3.connect(copied_database, timeout=0.25)
            source.execute("PRAGMA query_only = ON")
            source.execute("BEGIN")
            return _finish_memory_snapshot(source, deadline=deadline)
        finally:
            if source is not None:
                source.close()


def _open_stable_private_snapshot(
    database: Path,
    *,
    attempts: int,
    deadline: float,
) -> sqlite3.Connection:
    for attempt in range(attempts):
        if time.monotonic() >= deadline:
            raise PreviewSnapshotBusyError(
                "SQLite files did not become stable before the preview deadline"
            )
        try:
            first = _capture_original(database)
            second = _capture_original(database)
            if first != second:
                raise PreviewSnapshotBusyError(
                    "SQLite files changed between private snapshot observations"
                )
            memory = _open_private_copy(first, deadline=deadline)
            try:
                third = _capture_original(database)
            except BaseException:
                memory.close()
                raise
            if third != second:
                memory.close()
                raise PreviewSnapshotBusyError(
                    "SQLite files changed while the private snapshot was opened"
                )
            return memory
        except PreviewSnapshotBusyError:
            if attempt + 1 == attempts:
                raise
            time.sleep(min(0.01, max(0.0, deadline - time.monotonic())))
    raise AssertionError("unreachable")


def open_preview_snapshot(
    database: Path,
    *,
    stable_copy_attempts: int = 30,
    timeout_seconds: float = 5.0,
) -> sqlite3.Connection:
    """Return a validated, query-only in-memory view of a native database.

    The original database connection is always closed before this function
    returns. The original database is never opened with ``immutable=1``.
    """

    if stable_copy_attempts < 1:
        raise ValueError("stable_copy_attempts must be positive")
    if timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be positive")
    database = _validate_database_path(database)
    deadline = time.monotonic() + timeout_seconds
    main_identity = _identity(_lstat_ordinary_file(database))
    wal_stat = _optional_file_stat(Path(f"{database}-wal"))
    shm_stat = _optional_file_stat(Path(f"{database}-shm"))
    if os.name == "nt" and wal_stat is not None and shm_stat is not None:
        try:
            return _open_live_snapshot(
                database,
                main_identity=main_identity,
                wal_identity=_identity(wal_stat),
                shm_identity=_identity(shm_stat),
                deadline=deadline,
            )
        except PreviewSnapshotBusyError:
            if time.monotonic() >= deadline:
                raise
    return _open_stable_private_snapshot(
        database,
        attempts=stable_copy_attempts,
        deadline=deadline,
    )
