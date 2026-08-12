from __future__ import annotations

import os
import socket
import sys
import time
from contextlib import contextmanager
from uuid import uuid4

from micro_workflow_manager import __version__
from micro_workflow_manager.monitor import InlineMonitorReporter, InlineStatsReporter, now_iso
from micro_workflow_manager.system import MicroWorkflow

from .active_run import refuse_competing_run


@contextmanager
def active_workflow_run(
    workflow: MicroWorkflow,
    *,
    command: str,
    start_node: str,
    nodes: list[str],
    selected_jobs: list[int] | None = None,
    refuse_after_node: str | None = None,
    stats: bool = False,
    stats_interval: float = 5.0,
    monitor: bool = False,
    monitor_interval: float = 2.0,
):
    run_id = f"{int(time.time())}-{os.getpid()}-{uuid4().hex[:8]}"
    api_startup_strategy = os.environ.get("MWF_API_STARTUP_STRATEGY", "adaptive").strip().lower()
    if api_startup_strategy in {"single", "event", "latency", "serial", "legacy"}:
        api_startup_windows = "1"
    elif api_startup_strategy == "balanced":
        api_startup_windows = "auto:1-2"
    elif api_startup_strategy == "elastic":
        api_startup_windows = "auto:1-4"
    elif api_startup_strategy == "adaptive":
        api_startup_windows = "auto:1-12"
    elif api_startup_strategy.startswith("lanes:"):
        api_startup_windows = api_startup_strategy.split(":", 1)[1]
    else:
        api_startup_windows = "auto"
    api_completion_service_batch = (
        "8" if api_startup_strategy == "latency"
        else "12" if api_startup_strategy in {"event", "balanced"}
        else "16"
    )
    data = {
        "run_id": run_id,
        "status": "running",
        "command": command,
        "start_node": start_node,
        "nodes": list(nodes),
        "components": {
            node_name: list(workflow.component_key(workflow.component_for(node_name)))
            for node_name in nodes
        },
        "selected_jobs": list(selected_jobs or []),
        "refuse_after_node": refuse_after_node,
        "started_at": now_iso(),
        "heartbeat_at": now_iso(),
        "pid": os.getpid(),
        "hostname": socket.gethostname(),
        "mwf_version": __version__,
        "api_startup_strategy": api_startup_strategy,
        "api_event_drain_seconds": os.environ.get("MWF_API_EVENT_DRAIN_SECONDS", "0.010"),
        "api_terminal_microbatch": os.environ.get("MWF_API_TERMINAL_MICROBATCH", "1"),
        "api_max_admission_burst": os.environ.get("MWF_API_MAX_ADMISSION_BURST", "512"),
        "api_completion_service_batch": api_completion_service_batch,
        "api_admission_target_rounds": os.environ.get("MWF_API_ADMISSION_TARGET_ROUNDS", "4"),
        "api_startup_windows": api_startup_windows,
        "api_claim_transaction_rows": os.environ.get(
            "MWF_SQLITE_CLAIM_TRANSACTION_ROWS", "192"
        ),
        "api_prefetch": os.environ.get("MWF_API_PREFETCH", "0"),
    }

    # Claim the project run slot atomically. This prevents two terminals from
    # replacing .mwf/run.json at the same time. The restart command does not
    # claim this slot; it only controls a job already owned by this run.
    with workflow.storage.interprocess_lock("active-run-state"):
        refuse_competing_run(workflow)
        # Bind the pending override before publishing a running sequence. If
        # lock acquisition itself fails, no fresh stale run record is left
        # behind for the next command to recover.
        workflow.storage.bind_thread_overrides_to_run(run_id)
        workflow.storage.write_run_state(data)
        workflow.invalidate_thread_override_cache()

    # The scheduler supervisor owns both project-run heartbeats and handler
    # checkpoint deadlines. One thread services the whole workflow sequence.
    workflow.scheduler_supervisor.start_run_heartbeat(run_id, interval=2.0)

    stats_reporter = InlineStatsReporter(
        workflow,
        nodes=nodes,
        enabled=stats,
        interval=stats_interval,
    ).start()
    monitor_reporter = InlineMonitorReporter(
        workflow,
        nodes=nodes,
        enabled=monitor,
        interval=monitor_interval,
    ).start()

    finished = False

    def finish(status: str, error: str | None = None):
        nonlocal finished
        if finished:
            return

        # Stop periodic output before changing the run record, then print one
        # final snapshot after the record is terminal. This guarantees that an
        # inline or standalone monitor never labels a completed sequence active.
        stats_reporter.stop_periodic()
        monitor_reporter.stop_periodic()
        workflow.scheduler_supervisor.stop_run_heartbeat(run_id)

        # Publish the terminal run state before cleaning up the optional
        # override. A damaged/orphaned thread-overrides lock must never leave a
        # completed workflow recorded as running. Bound overrides are already
        # ignored once the run is terminal and are discarded by the next run.
        with workflow.storage.interprocess_lock("active-run-state"):
            current = workflow.storage.get_run_state()
            # Never let a stale process overwrite a newer run record.
            if current.get("run_id") == run_id:
                updates = {
                    "status": status,
                    "finished_at": now_iso(),
                }
                if error is not None:
                    updates["error"] = error
                workflow.storage.update_run_state(**updates)

        override_cleanup_error: Exception | None = None
        try:
            workflow.storage.clear_thread_overrides_for_run(run_id)
        except Exception as cleanup_error:
            override_cleanup_error = cleanup_error
        finally:
            workflow.invalidate_thread_override_cache()

        finished = True
        stats_reporter.print_final()
        monitor_reporter.print_final()
        if override_cleanup_error is not None:
            print(
                "Warning: the run completed, but its temporary thread override "
                f"could not be removed: {override_cleanup_error}",
                file=sys.stderr,
            )

    try:
        yield finish
    except Exception as error:
        finish("failed", repr(error))
        raise
    finally:
        if not finished:
            finish("done")
