from __future__ import annotations

import json
import os
import socket
import threading
import time
from pathlib import Path
from typing import Any, Callable
from uuid import uuid4

from ..processes import process_is_alive


class StateEventStorageMixin:
    """Durable lifecycle-event reads plus local/cross-process wakeups.

    ``job_events`` remains the one durable event journal.  The broker only
    announces that a transaction committed; consumers advance their own
    ``event_id`` cursor and therefore cannot lose state when wakeups coalesce.
    """

    _state_listeners: dict[Path, set[Callable[[], None]]] = {}
    _state_listeners_guard = threading.Lock()
    _queue_listeners: dict[tuple[Path, str], set[Callable[[], None]]] = {}
    _queue_listeners_guard = threading.Lock()

    def _init_state_event_broker(self) -> None:
        self._state_cross_callbacks: set[Callable[[], None]] = set()
        self._state_cross_guard = threading.Lock()
        self._state_listener_socket: socket.socket | None = None
        self._state_listener_thread: threading.Thread | None = None
        self._state_listener_stop: threading.Event | None = None
        self._state_listener_record: Path | None = None
        self._state_subscriber_cache: tuple[tuple[str, int], ...] = ()
        self._state_subscriber_cache_until = 0.0
        self._state_last_broadcast = 0.0

    def latest_job_event_id(self) -> int:
        row = self.db_connection().execute(
            "SELECT COALESCE(MAX(event_id), 0) AS event_id FROM job_events"
        ).fetchone()
        return 0 if row is None else int(row["event_id"])

    def read_job_events_since(
        self,
        event_id: int,
        *,
        node_names: list[str] | tuple[str, ...] | set[str] | None = None,
        limit: int = 4096,
    ) -> list[dict[str, Any]]:
        if type(event_id) is not int or event_id < 0:
            raise ValueError("event_id must be an integer >= 0")
        if type(limit) is not int or limit < 1:
            raise ValueError("limit must be an integer >= 1")
        args: list[object] = [event_id]
        sql = (
            "SELECT event_id, node_name, job_id, time, event, data_json "
            "FROM job_events WHERE event_id>?"
        )
        if node_names is not None:
            normalized = sorted({self.validate_node_name(name) for name in node_names})
            if not normalized:
                return []
            placeholders = ",".join("?" for _ in normalized)
            sql += f" AND node_name IN ({placeholders})"
            args.extend(normalized)
        sql += " ORDER BY event_id LIMIT ?"
        args.append(limit)
        rows = self.db_connection().execute(sql, args).fetchall()
        result: list[dict[str, Any]] = []
        for row in rows:
            try:
                data = json.loads(row["data_json"] or "{}")
            except json.JSONDecodeError as error:
                data = {"decode_error": str(error), "raw": row["data_json"]}
            if not isinstance(data, dict):
                data = {"value": data}
            result.append(
                {
                    "event_id": int(row["event_id"]),
                    "node_name": str(row["node_name"]),
                    "job_id": int(row["job_id"]),
                    "time": str(row["time"]),
                    "event": str(row["event"]),
                    **data,
                }
            )
        return result

    def subscribe_state_changes(
        self,
        callback: Callable[[], None],
        *,
        local: bool = True,
        cross_process: bool = False,
    ) -> Callable[[], None]:
        if not callable(callback):
            raise TypeError("callback must be callable")
        path = self.state_database_path()
        if local:
            with self._state_listeners_guard:
                self._state_listeners.setdefault(path, set()).add(callback)
        if cross_process:
            with self._state_cross_guard:
                self._state_cross_callbacks.add(callback)
                self._ensure_state_listener_locked()

        def unsubscribe() -> None:
            if local:
                with self._state_listeners_guard:
                    listeners = self._state_listeners.get(path)
                    if listeners is not None:
                        listeners.discard(callback)
                        if not listeners:
                            self._state_listeners.pop(path, None)
            if cross_process:
                with self._state_cross_guard:
                    self._state_cross_callbacks.discard(callback)
                    if not self._state_cross_callbacks:
                        self._stop_state_listener_locked()

        return unsubscribe

    def notify_state_change(self) -> None:
        """Publish a coalescible wakeup after a durable SQLite commit."""
        path = self.state_database_path()
        with self._state_listeners_guard:
            listeners = tuple(self._state_listeners.get(path, ()))
        for callback in listeners:
            try:
                callback()
            except Exception:
                # A diagnostic subscriber must never break state publication.
                pass
        self._broadcast_state_change()

    def subscribe_queue_changes(
        self,
        callback: Callable[[], None],
        *,
        node_name: str | None = None,
    ) -> Callable[[], None]:
        """Subscribe to queue publication, optionally for one node only.

        A live Hoeflein component can contain many resident node pumps. Waking
        every pump for every lifecycle transition creates a thundering herd of
        empty SQLite queue probes. Node-scoped subscriptions wake only the pump
        that can consume a newly queued job; the ordinary state broker remains
        the durable/cross-process fallback for supervisors and diagnostics.
        """
        if node_name is None:
            return self.subscribe_state_changes(callback, local=True)
        if not callable(callback):
            raise TypeError("callback must be callable")
        node_name = self.validate_node_name(node_name)
        key = (self.state_database_path(), node_name)
        with self._queue_listeners_guard:
            self._queue_listeners.setdefault(key, set()).add(callback)

        def unsubscribe() -> None:
            with self._queue_listeners_guard:
                listeners = self._queue_listeners.get(key)
                if listeners is not None:
                    listeners.discard(callback)
                    if not listeners:
                        self._queue_listeners.pop(key, None)

        return unsubscribe

    def notify_queue_changes(self, node_names) -> None:
        normalized = tuple({self.validate_node_name(name) for name in node_names})
        path = self.state_database_path()
        callbacks: set[Callable[[], None]] = set()
        with self._queue_listeners_guard:
            for node_name in normalized:
                callbacks.update(self._queue_listeners.get((path, node_name), ()))
        for callback in tuple(callbacks):
            try:
                callback()
            except Exception:
                pass
        # Keep the pre-0.5.3 global state signal for monitors, supervisors and
        # cross-process writers, but live node pumps no longer subscribe to it.
        self.notify_state_change()

    def notify_queue_change(self, node_name: str | None = None) -> None:
        if node_name is None:
            self.notify_state_change()
            return
        self.notify_queue_changes((node_name,))

    def _subscriber_dir(self) -> Path:
        path = self.project_dir / ".mwf" / "state_subscribers"
        path.mkdir(parents=True, exist_ok=True)
        return path

    @staticmethod
    def _pid_alive(pid: int) -> bool:
        # ``os.kill(pid, 0)`` is a POSIX liveness probe, but on Windows signal
        # value 0 is CTRL_C_EVENT. Using it there can interrupt every process
        # attached to the console, including the parent scheduler and sibling
        # ProcessPoolExecutor workers. Keep all platform handling centralized.
        return process_is_alive(pid)

    def _ensure_state_listener_locked(self) -> None:
        thread = self._state_listener_thread
        if thread is not None and thread.is_alive():
            return
        listener = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        listener.bind(("127.0.0.1", 0))
        listener.settimeout(0.5)
        stop = threading.Event()
        token = uuid4().hex
        record = self._subscriber_dir() / f"{os.getpid()}-{token}.json"
        host, port = listener.getsockname()
        payload = {
            "pid": os.getpid(),
            "host": host,
            "port": int(port),
            "database": str(self.state_database_path()),
            "created_at": time.time(),
        }
        temporary = record.with_suffix(".tmp")
        temporary.write_text(json.dumps(payload, separators=(",", ":")), encoding="utf-8")
        os.replace(temporary, record)
        self._state_listener_socket = listener
        self._state_listener_stop = stop
        self._state_listener_record = record
        self._state_listener_thread = threading.Thread(
            target=self._state_listener_loop,
            name="mwf-state-events",
            daemon=True,
        )
        self._state_listener_thread.start()

    def _state_listener_loop(self) -> None:
        listener = self._state_listener_socket
        stop = self._state_listener_stop
        if listener is None or stop is None:
            return
        try:
            while not stop.is_set():
                try:
                    listener.recvfrom(64)
                except socket.timeout:
                    continue
                except OSError:
                    return
                with self._state_cross_guard:
                    callbacks = tuple(self._state_cross_callbacks)
                for callback in callbacks:
                    try:
                        callback()
                    except Exception:
                        pass
        finally:
            try:
                listener.close()
            except OSError:
                pass

    def _stop_state_listener_locked(self) -> None:
        stop = self._state_listener_stop
        listener = self._state_listener_socket
        record = self._state_listener_record
        if stop is not None:
            stop.set()
        if listener is not None:
            try:
                listener.close()
            except OSError:
                pass
        if record is not None:
            try:
                record.unlink()
            except FileNotFoundError:
                pass
            except OSError:
                pass
        self._state_listener_socket = None
        self._state_listener_stop = None
        self._state_listener_record = None
        self._state_listener_thread = None

    def _refresh_subscriber_cache(self, now_value: float) -> None:
        if now_value < self._state_subscriber_cache_until:
            return
        subscribers: list[tuple[str, int]] = []
        directory = self._subscriber_dir()
        for path in directory.glob("*.json"):
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
                pid = int(data["pid"])
                host = str(data["host"])
                port = int(data["port"])
                database = str(data["database"])
            except (OSError, ValueError, TypeError, KeyError, json.JSONDecodeError):
                continue
            if database != str(self.state_database_path()):
                continue
            if pid == os.getpid():
                continue
            if not self._pid_alive(pid):
                try:
                    path.unlink()
                except OSError:
                    pass
                continue
            subscribers.append((host, port))
        self._state_subscriber_cache = tuple(subscribers)
        self._state_subscriber_cache_until = now_value + 0.25

    def _broadcast_state_change(self) -> None:
        now_value = time.monotonic()
        # Cross-process wakeups are hints over a durable cursor.  Coalescing
        # avoids one UDP packet per job during large terminal waves.
        if now_value - self._state_last_broadcast < 0.01:
            return
        self._state_last_broadcast = now_value
        self._refresh_subscriber_cache(now_value)
        if not self._state_subscriber_cache:
            return
        try:
            publisher = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            publisher.setblocking(False)
            try:
                for address in self._state_subscriber_cache:
                    try:
                        publisher.sendto(b"1", address)
                    except OSError:
                        # The next cache refresh removes dead subscribers.
                        pass
            finally:
                publisher.close()
        except OSError:
            pass
