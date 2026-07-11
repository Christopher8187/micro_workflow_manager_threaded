from __future__ import annotations

import errno
import os
from typing import Any


def _windows_process_is_alive(pid: int) -> bool:
    """Query a Windows process without sending it a signal.

    ``os.kill(pid, 0)`` is the usual POSIX liveness check, but Windows maps
    most signals to ``TerminateProcess``. Use the process query API instead so
    a second-terminal restart check can never disturb the active workflow.
    """
    import ctypes
    from ctypes import wintypes

    process_query_limited_information = 0x1000
    still_active = 259
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
    kernel32.OpenProcess.restype = wintypes.HANDLE
    kernel32.GetExitCodeProcess.argtypes = [wintypes.HANDLE, ctypes.POINTER(wintypes.DWORD)]
    kernel32.GetExitCodeProcess.restype = wintypes.BOOL
    kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel32.CloseHandle.restype = wintypes.BOOL

    handle = kernel32.OpenProcess(process_query_limited_information, False, pid)
    if not handle:
        # Access denied means the process exists but cannot be queried by this
        # account. Other failures are treated as not live.
        return ctypes.get_last_error() == 5

    try:
        exit_code = wintypes.DWORD()
        if not kernel32.GetExitCodeProcess(handle, ctypes.byref(exit_code)):
            return False
        return exit_code.value == still_active
    finally:
        kernel32.CloseHandle(handle)


def process_is_alive(pid: int) -> bool:
    if type(pid) is not int or pid < 1:
        return False

    if os.name == "nt":
        return _windows_process_is_alive(pid)

    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError as error:
        if error.errno == errno.ESRCH:
            return False
        if error.errno == errno.EPERM:
            return True
        return False
    return True


def _storage(value):
    return getattr(value, "storage", value)


def live_active_run(storage_or_workflow) -> dict[str, Any] | None:
    """Return the live run-state record, ignoring stale dead-process records."""
    storage = _storage(storage_or_workflow)
    state = storage.get_run_state()
    if state.get("status") != "running":
        return None

    pid = state.get("pid")
    if not process_is_alive(pid):
        return None

    return state


def refuse_competing_run(storage_or_workflow):
    """Prevent a second run/runfrom process from taking over one project."""
    active = live_active_run(storage_or_workflow)
    if active is None:
        return

    command = active.get("command", "workflow")
    run_id = active.get("run_id", "?")
    pid = active.get("pid", "?")
    raise RuntimeError(
        f"A {command} sequence is already active (run {run_id}, process {pid}). "
        "Do not start a competing run from a second terminal. To restart one "
        "currently running job inside that sequence, use: "
        "mwf restart <node> job <id>"
    )
