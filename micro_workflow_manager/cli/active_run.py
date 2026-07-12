from __future__ import annotations

import errno
import os
import socket
from datetime import datetime
from typing import Any


DEFAULT_HEARTBEAT_STALE_SECONDS = 15.0


def _windows_process_is_alive(pid: int) -> bool:
    """Query a Windows process without sending it a signal."""
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


def _parse_time(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def run_state_liveness(
    state: dict[str, Any],
    *,
    stale_after_seconds: float = DEFAULT_HEARTBEAT_STALE_SECONDS,
) -> dict[str, Any]:
    if not isinstance(state, dict) or state.get("status") != "running":
        return {"live": False, "reason": "no running sequence is recorded"}

    recorded_host = state.get("hostname")
    current_host = socket.gethostname()
    same_host = recorded_host in {None, "", current_host}
    pid = state.get("pid")
    pid_live = process_is_alive(pid) if same_host else None

    heartbeat = _parse_time(state.get("heartbeat_at") or state.get("started_at"))
    heartbeat_age = None
    heartbeat_fresh = False
    if heartbeat is not None:
        now = datetime.now(heartbeat.tzinfo) if heartbeat.tzinfo is not None else datetime.now()
        heartbeat_age = max(0.0, (now - heartbeat).total_seconds())
        heartbeat_fresh = heartbeat_age <= stale_after_seconds

    if same_host and pid_live:
        return {
            "live": True,
            "reason": "the recorded process is alive",
            "same_host": True,
            "pid_live": True,
            "heartbeat_age_seconds": heartbeat_age,
        }
    if not same_host and heartbeat_fresh:
        return {
            "live": True,
            "reason": "the run belongs to another host and its heartbeat is fresh",
            "same_host": False,
            "pid_live": None,
            "heartbeat_age_seconds": heartbeat_age,
        }
    if same_host and pid_live is False:
        reason = "the recorded process is no longer alive"
    elif not same_host:
        reason = "the other host heartbeat is stale"
    else:
        reason = "the run heartbeat is stale or missing"
    return {
        "live": False,
        "reason": reason,
        "same_host": same_host,
        "pid_live": pid_live,
        "heartbeat_age_seconds": heartbeat_age,
    }


def live_active_run(storage_or_workflow) -> dict[str, Any] | None:
    storage = _storage(storage_or_workflow)
    state = storage.get_run_state()
    return state if run_state_liveness(state)["live"] else None


def refuse_competing_run(storage_or_workflow):
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
