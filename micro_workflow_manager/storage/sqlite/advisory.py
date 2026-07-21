from __future__ import annotations

import json
import os
import socket
import threading
import time
from contextlib import contextmanager
from typing import Iterator
from uuid import uuid4

from micro_workflow_manager.processes import process_is_alive


class SQLiteAdvisoryLockMixin:
    """Lease-based SQLite advisory locks used across processes."""

    def _new_advisory_owner(self) -> str:
        return json.dumps(
            {
                "version": 2,
                "hostname": socket.gethostname(),
                "pid": os.getpid(),
                "thread_id": threading.get_ident(),
                "nonce": uuid4().hex,
            },
            separators=(",", ":"),
            sort_keys=True,
        )

    def _parse_advisory_owner(self, owner: str) -> dict[str, Any] | None:
        if not isinstance(owner, str) or not owner:
            return None
        try:
            data = json.loads(owner)
        except (TypeError, json.JSONDecodeError):
            data = None
        if isinstance(data, dict):
            pid = data.get("pid")
            hostname = data.get("hostname")
            if type(pid) is int and pid > 0 and isinstance(hostname, str):
                return {"pid": pid, "hostname": hostname}

        # MWF 0.3.4-0.3.6 stored ``pid:thread_id:uuid``. Those databases are
        # local project state, so an owner without a hostname is treated as a
        # local legacy owner for immediate dead-process recovery.
        parts = owner.split(":", 2)
        if len(parts) == 3:
            try:
                pid = int(parts[0])
            except ValueError:
                return None
            if pid > 0:
                return {"pid": pid, "hostname": None}
        return None

    def _register_advisory_owner(cls, owner: str) -> None:
        with cls._advisory_owner_registry_guard:
            cls._advisory_owner_registry.add(owner)

    def _unregister_advisory_owner(cls, owner: str) -> None:
        with cls._advisory_owner_registry_guard:
            cls._advisory_owner_registry.discard(owner)

    def _advisory_owner_is_registered(cls, owner: str) -> bool:
        with cls._advisory_owner_registry_guard:
            return owner in cls._advisory_owner_registry

    def _advisory_owner_liveness(self, owner: str) -> bool | None:
        parsed = self._parse_advisory_owner(owner)
        if parsed is None:
            return None

        hostname = parsed["hostname"]
        if hostname not in {None, "", socket.gethostname()}:
            # A shared project directory may be visible from another host. We
            # cannot query that process locally, so its lease remains the
            # fallback authority.
            return None

        pid = parsed["pid"]
        if pid == os.getpid():
            # The process-local registry distinguishes a genuinely held lock
            # from a row orphaned by a terminated owner thread/storage object.
            return self._advisory_owner_is_registered(owner)
        return process_is_alive(pid)

    def _advisory_lock_is_reclaimable(
        self,
        owner: str,
        expires_at: float,
        now_value: float,
    ) -> bool:
        liveness = self._advisory_owner_liveness(owner)
        if liveness is False:
            return True
        if liveness is True:
            # Do not steal a live local lock merely because a long critical
            # section exceeded its nominal lease.
            return False
        return expires_at <= now_value

    @contextmanager
    def interprocess_lock(
        self,
        name: str,
        *,
        timeout: float = 120.0,
        lease_seconds: float = 300.0,
    ):
        safe_name = str(name)
        thread_lock = self.thread_lock_for(
            self.state_database_path().parent / "logical-locks" / safe_name
        )
        with thread_lock:
            held = getattr(self._advisory_local, "held", None)
            if held is None:
                held = {}
                self._advisory_local.held = held
            existing = held.get(safe_name)
            if existing is not None:
                existing["count"] += 1
                try:
                    yield
                finally:
                    existing["count"] -= 1
                return

            owner = self._new_advisory_owner()
            deadline = time.monotonic() + timeout
            delay = 0.005
            while True:
                acquired = False
                now_value = time.time()
                with self.db_transaction() as connection:
                    row = connection.execute(
                        "SELECT owner, expires_at FROM advisory_locks WHERE name = ?",
                        (safe_name,),
                    ).fetchone()
                    reclaimable = row is None
                    if row is not None:
                        reclaimable = self._advisory_lock_is_reclaimable(
                            str(row["owner"]),
                            float(row["expires_at"]),
                            now_value,
                        )
                    if reclaimable:
                        connection.execute(
                            "INSERT INTO advisory_locks(name, owner, acquired_at, expires_at) "
                            "VALUES(?, ?, ?, ?) "
                            "ON CONFLICT(name) DO UPDATE SET "
                            "owner=excluded.owner, acquired_at=excluded.acquired_at, "
                            "expires_at=excluded.expires_at",
                            (safe_name, owner, now_value, now_value + lease_seconds),
                        )
                        acquired = True
                if acquired:
                    break
                if time.monotonic() >= deadline:
                    raise TimeoutError(f"Timed out acquiring MWF lock {safe_name!r}")
                time.sleep(delay)
                delay = min(0.2, delay * 1.5)

            self._register_advisory_owner(owner)
            held[safe_name] = {"owner": owner, "count": 1}
            try:
                yield
            finally:
                state = held.get(safe_name)
                if state is not None:
                    state["count"] -= 1
                    if state["count"] <= 0:
                        held.pop(safe_name, None)
                        try:
                            with self.db_transaction() as connection:
                                connection.execute(
                                    "DELETE FROM advisory_locks WHERE name = ? AND owner = ?",
                                    (safe_name, owner),
                                )
                        finally:
                            self._unregister_advisory_owner(owner)
