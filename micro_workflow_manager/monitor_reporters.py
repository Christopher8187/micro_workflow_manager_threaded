from __future__ import annotations

import sys
import threading
import time



def _monitor_api():
    # Use the public facade at call time. This preserves the long-standing
    # ability to monkeypatch monitor functions without coupling reporters to
    # their implementation modules.
    from . import monitor as monitor_api

    return monitor_api


def monitor_loop(
    workflow,
    nodes: list[str] | None = None,
    *,
    interval: float = 2.0,
    once: bool = False,
    json_output: bool = False,
    no_clear: bool = False,
):
    while True:
        if not once and not json_output and not no_clear:
            print("\033[2J\033[H", end="")

        _monitor_api().print_snapshot(workflow, nodes=nodes, json_output=json_output)

        if once:
            return

        try:
            time.sleep(interval)
        except KeyboardInterrupt:
            return

class _InlineReporterBase:
    def __init__(
        self,
        workflow,
        nodes: list[str] | None = None,
        *,
        enabled: bool = False,
        interval: float = 5.0,
        thread_name: str,
    ):
        self.workflow = workflow
        self.nodes = nodes
        self.enabled = enabled
        self.interval = interval
        self.thread_name = thread_name
        self.stop = threading.Event()
        self.thread: threading.Thread | None = None

    def start(self):
        if not self.enabled or self.thread is not None:
            return self
        self.thread = threading.Thread(target=self._loop, name=self.thread_name, daemon=True)
        self.thread.start()
        return self

    def stop_periodic(self):
        if not self.enabled:
            return
        self.stop.set()
        if self.thread is not None:
            self.thread.join(timeout=max(1.0, min(5.0, self.interval + 0.5)))
            self.thread = None

    def print_final(self):
        if self.enabled:
            self._safe_print(final=True)

    def __enter__(self):
        return self.start()

    def __exit__(self, exc_type, exc, tb):
        self.stop_periodic()
        self.print_final()
        return False

    def _loop(self):
        # Print immediately after the active run is claimed, then periodically.
        try:
            while not self.stop.is_set():
                self._safe_print(final=False)
                self.stop.wait(self.interval)
        finally:
            storage = getattr(self.workflow, "storage", None)
            close = getattr(storage, "close_thread_connection", None)
            if close is not None:
                close()

    def _safe_print(self, *, final: bool):
        try:
            self._print(final=final)
        except Exception as error:
            # Diagnostics must never change workflow correctness. Report a
            # snapshot failure and allow the scheduler/run finalization to
            # continue; a later interval may succeed.
            print(
                f"[{self.thread_name} error] snapshot unavailable: {error!r}",
                file=sys.stderr,
                flush=True,
            )

    def _print(self, *, final: bool):
        raise NotImplementedError

class InlineStatsReporter(_InlineReporterBase):
    def __init__(
        self,
        workflow,
        nodes: list[str] | None = None,
        *,
        enabled: bool = False,
        interval: float = 5.0,
    ):
        super().__init__(
            workflow,
            nodes,
            enabled=enabled,
            interval=interval,
            thread_name="mwf-stats",
        )

    def _print(self, *, final: bool):
        snapshot = _monitor_api().workflow_snapshot(self.workflow, nodes=self.nodes)
        totals = snapshot["totals"]
        running_nodes = snapshot.get("running_nodes") or []
        running_text = ",".join(running_nodes) if running_nodes else "none"
        prefix = "final stats" if final else "stats"
        print(
            f"[{prefix}] "
            f"time={snapshot['generated_at']} "
            f"running_nodes={running_text} "
            f"jobs={totals['jobs']} "
            f"done={totals['done']} "
            f"queued={totals['queued']} "
            f"running={totals['running']} "
            f"failed={totals['failed']} "
            f"left={totals['remaining']} "
            f"progress={totals['progress_percent']}% "
            f"rough_eta={_monitor_api().human_seconds(totals.get('rough_eta_seconds'))}",
            file=sys.stderr,
            flush=True,
        )

class InlineMonitorReporter(_InlineReporterBase):
    """Print timestamped full monitor snapshots beside an execution command.

    Inline monitoring never clears the terminal. Task output remains visible and
    every snapshot is retained as a chronological diagnostic record.
    """

    def __init__(
        self,
        workflow,
        nodes: list[str] | None = None,
        *,
        enabled: bool = False,
        interval: float = 2.0,
    ):
        super().__init__(
            workflow,
            nodes,
            enabled=enabled,
            interval=interval,
            thread_name="mwf-inline-monitor",
        )

    def _print(self, *, final: bool):
        snapshot = _monitor_api().workflow_snapshot(self.workflow, nodes=self.nodes)
        label = "final monitor" if final else "monitor"
        print(f"\n--- mwf {label} snapshot ---", file=sys.stderr, flush=True)
        print(_monitor_api().render_snapshot(snapshot), file=sys.stderr, flush=True)
