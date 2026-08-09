from __future__ import annotations

from threading import Event, Lock
from typing import Callable

from micro_workflow_manager.models import CANCELLED, DONE, FAILED, QUEUED, RUNNING, SKIPPED
from micro_workflow_manager.system import MicroWorkflow

from .graph_utils import ready_for_run_set
from .run_session import active_workflow_run


def _interactive_wait_deadlock_resolver(workflow: MicroWorkflow):
    """Return one serialized CLI chooser for Hoeflein waiting deadlocks."""
    prompt_lock = Lock()

    def resolve(
        component_nodes: tuple[str, ...],
        queued_nodes: tuple[str, ...],
        blockers: dict[str, tuple[str, ...]],
    ) -> str | None:
        with prompt_lock:
            component_text = ", ".join(component_nodes)
            print(
                "Waiting deadlock in Hoeflein component "
                f"{{{component_text}}}: every queued node is waiting for another "
                "node in the component."
            )
            print(
                "Choose one node to override waiting temporarily. It will run "
                "until its queue drains; normal waiting rules are then recalculated."
            )
            for index, node_name in enumerate(queued_nodes, start=1):
                queued_count = workflow.storage.job_status_counts(node_name).get(
                    QUEUED,
                    0,
                )
                waiting_on = ", ".join(blockers.get(node_name, ())) or "unknown"
                print(
                    f"  {index}. {node_name} "
                    f"(queued={queued_count}, waiting on: {waiting_on})"
                )

            choices = {node_name.casefold(): node_name for node_name in queued_nodes}
            while True:
                try:
                    answer = input(
                        "Node to run [number/name, q to leave blocked]: "
                    ).strip()
                except EOFError:
                    print(
                        "No interactive input available; leaving the component blocked."
                    )
                    return None

                if not answer or answer.casefold() in {"q", "quit", "cancel"}:
                    print("Leaving the Hoeflein component blocked.")
                    return None
                if answer.isdigit():
                    selected_index = int(answer)
                    if 1 <= selected_index <= len(queued_nodes):
                        selected = queued_nodes[selected_index - 1]
                        print(f"Temporarily overriding waiting for node {selected}.")
                        return selected
                selected = choices.get(answer.casefold())
                if selected is not None:
                    print(f"Temporarily overriding waiting for node {selected}.")
                    return selected
                print(
                    "Choose one of: "
                    + ", ".join(
                        [str(index) for index in range(1, len(queued_nodes) + 1)]
                        + list(queued_nodes)
                    )
                    + ", or q."
                )

    return resolve


def run_nodes(
    workflow: MicroWorkflow,
    nodes: list[str],
    start_node: str,
    ignore_external: bool = False,
    *,
    command: str = "run",
    stats: bool = False,
    stats_interval: float = 5.0,
    monitor: bool = False,
    monitor_interval: float = 2.0,
    prepare: Callable[[], None] | None = None,
    require_start_queued: bool = True,
    refuse_after_node: str | None = None,
) -> int:
    run_set = set(nodes)
    previous_allowed_run_nodes = workflow.allowed_run_nodes
    previous_autostart_mode = workflow.autostart_mode
    previous_restart_enabled = workflow.active_job_restart_enabled

    workflow.allowed_run_nodes = run_set
    workflow.autostart_mode = "queue"
    workflow.active_job_restart_enabled = True
    refusal_event = Event()
    refuse_after_component = (
        workflow.component_key(workflow.component_for(refuse_after_node))
        if refuse_after_node is not None
        else None
    )
    wait_deadlock_resolver = (
        _interactive_wait_deadlock_resolver(workflow)
        if command in {"run", "runfrom", "resume", "resumefrom"}
        else None
    )

    def refusal_target_terminal() -> bool:
        if refuse_after_component is None:
            return False
        return all(
            workflow.node_complete(item)
            or workflow.storage.get_node_status(item) in {FAILED, CANCELLED}
            for item in refuse_after_component
        )

    try:
        with active_workflow_run(
            workflow,
            command=command,
            start_node=start_node,
            nodes=nodes,
            refuse_after_node=refuse_after_node,
            stats=stats,
            stats_interval=stats_interval,
            monitor=monitor,
            monitor_interval=monitor_interval,
        ) as finish_run:
            if prepare is not None:
                prepare()

            has_any_queued = any(workflow.storage.has_queued_jobs(item) for item in nodes)
            if not has_any_queued:
                if require_start_queued:
                    workflow.storage.set_node_status(start_node, QUEUED)
                    print(
                        f"No queued jobs for {start_node}. "
                        f"Create default jobs in node_behavior/{start_node}.py with "
                        "router.create_job(number=..., params={...})."
                    )
                else:
                    print("No failed, cancelled, stale-running, or queued jobs remain in the resume set.")
                finish_run("done")
                return 0

            if workflow.runner in {"threaded", "api", "process"}:
                ran = workflow.run_concurrently(
                    nodes=nodes,
                    ready_check=lambda item: ready_for_run_set(
                        workflow,
                        item,
                        run_set,
                        ignore_external,
                    ),
                    refuse_after_component=refuse_after_component,
                    refusal_event=refusal_event,
                    wait_deadlock_resolver=wait_deadlock_resolver,
                )
            else:
                ran = []
                units = workflow.execution_components(nodes)
                blocked_units: set[tuple[str, ...]] = set()

                while True:
                    workflow.finalize_ready_nodes()
                    if refusal_target_terminal():
                        refusal_event.set()
                        break
                    ready_units = [
                        unit
                        for unit in units
                        if unit not in blocked_units
                        and any(workflow.storage.has_queued_jobs(node) for node in unit)
                        and all(
                            ready_for_run_set(workflow, node, run_set, ignore_external)
                            for node in unit
                        )
                    ]

                    if not ready_units:
                        break

                    for unit in ready_units:
                        ran.extend(workflow.run_component(
                            set(unit),
                            ignore_readiness=True,
                            wait_deadlock_resolver=wait_deadlock_resolver,
                        ))
                        if workflow.component_wait_deadlocked(set(unit)):
                            blocked_units.add(unit)
                        else:
                            blocked_units.discard(unit)
                        if refuse_after_component is not None and unit == refuse_after_component:
                            refusal_event.set()
                            break
                    if refusal_event.is_set():
                        break

            workflow.finalize_ready_nodes()
            if ignore_external:
                # A partial runfrom may intentionally process one incoming branch
                # of a later component before its other predecessors run. Mark a
                # selected component complete for this branch when all jobs that
                # currently exist are successful and quiescent. Future producers
                # will queue new jobs and reactivate it.
                for unit in workflow.execution_components(nodes):
                    counts = [workflow.storage.job_status_counts(name) for name in unit]
                    total = sum(sum(item.values()) for item in counts)
                    failed = any(item.get(FAILED, 0) for item in counts)
                    active = any(item.get(RUNNING, 0) or item.get(QUEUED, 0) for item in counts)
                    successful = sum(item.get(DONE, 0) + item.get(SKIPPED, 0) for item in counts)
                    if total > 0 and successful == total and not failed and not active:
                        for name in unit:
                            workflow.storage.set_node_status(name, "done")

            if refusal_event.is_set():
                failed_boundary = any(
                    workflow.storage.get_node_status(item) in {FAILED, CANCELLED}
                    for item in (refuse_after_component or ())
                )
                finish_run("failed" if failed_boundary else "done")
                boundary = ", ".join(refuse_after_component or ())
                print(
                    "Refused further Hoeflein-component admission after "
                    f"{{{boundary}}} terminated."
                )
                queued_after = [
                    item for item in nodes if workflow.storage.has_queued_jobs(item)
                ]
                if queued_after:
                    print("Left queued for a later run:")
                    for item in queued_after:
                        print(f"  {item}")
                if ran:
                    print("Ran:")
                    for item in ran:
                        print(f"  {item}")
                return 1 if failed_boundary else 0

            blocked = [node for node in nodes if workflow.storage.has_queued_jobs(node)]

            if blocked:
                finish_run("blocked")
                print("Stopped before these queued nodes became ready:")
                for node in blocked:
                    status = workflow.storage.get_node_status(node) or "missing"
                    print(f"  {node}: {status}")
                return 1

            unfinished = [node for node in nodes if not workflow.node_complete(node)]

            if unfinished:
                finish_run("incomplete")
                print("These nodes did not complete:")
                for node in unfinished:
                    status = workflow.storage.get_node_status(node) or "missing"
                    job_count = len(workflow.storage.list_jobs(node))
                    queued_count = len(workflow.storage.queued_job_ids(node))
                    print(f"  {node}: {status}, jobs={job_count}, queued={queued_count}")
                print("This usually means an upstream task did not create the expected downstream jobs.")
                return 1

            finish_run("done")
            print("Ran:")
            for node in ran:
                print(f"  {node}")

            return 0

    finally:
        workflow.allowed_run_nodes = previous_allowed_run_nodes
        workflow.autostart_mode = previous_autostart_mode
        workflow.active_job_restart_enabled = previous_restart_enabled
