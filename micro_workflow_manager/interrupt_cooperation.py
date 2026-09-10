"""Cooperative interrupt waits outside every execution publication fence."""

from __future__ import annotations

from time import sleep

from .fibers import cooperative_sleep
from .storage.execution_restart import execution_fence_held


def check_execution_without_pause(context):
    """Validate cancellation/restart without reporting progress."""
    if context._attempt_watch is not None:
        restart_error = context.system.scheduler_supervisor.execution_cancel_error(context._attempt_watch)
        if restart_error is not None:
            raise restart_error
        timeout_error = context.system.scheduler_supervisor.timeout_error(context._attempt_watch)
        if timeout_error is not None:
            raise timeout_error
    context._check_local_execution()
    # Active attempts are polled in one batch by SchedulerSupervisor. A
    # guarded mutation still verifies its exact lease under the per-job
    # filesystem fence, so omitting a per-checkpoint SELECT is race-safe.
    if context.execution_id is not None and context._attempt_watch is None:
        context.system.check_job_execution(
            context.current_node,
            context.job_id,
            context.execution_generation,
            context.execution_id,
        )


def wait_for_interrupt_pauses(context):
    if (context.execution_id is None or execution_fence_held()
            or getattr(context, '_waiting_for_interrupt', False)):
        return
    from .workflow.api_admission import (
        release_current_api_execution_permit, reacquire_current_api_execution_permit,
    )

    storage = context.system.storage
    if not storage._has_interrupt_pause_candidate(context.execution_id):
        return
    session_id, _ = context.system.execution_claim_context(context.current_node)
    identity = (session_id, context.current_node, context.job_id,
                context.execution_generation, context.execution_id)
    if not storage.read_active_interrupt_pauses(*identity):
        return
    context._waiting_for_interrupt = True
    try:
        children = storage.acknowledge_interrupt_pauses(*identity)
        release_current_api_execution_permit(context)
        while children:
            check_execution_without_pause(context)
            if not cooperative_sleep(0.05):
                if context._cancellation_event is None:
                    sleep(0.05)
                else:
                    context._cancellation_event.wait(0.05)
            check_execution_without_pause(context)
            children = storage.read_active_interrupt_pauses(*identity)
        check_execution_without_pause(context)
        reacquire_current_api_execution_permit(context)
    finally:
        context._waiting_for_interrupt = False


def claim_with_interrupt_retry(action):
    from .storage import InterruptAdmissionPaused

    while True:
        try:
            return action()
        except InterruptAdmissionPaused:
            if not cooperative_sleep(0.05):
                sleep(0.05)
