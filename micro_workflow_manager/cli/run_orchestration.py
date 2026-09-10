from __future__ import annotations

from threading import Event, Lock
from typing import Callable

from micro_workflow_manager.system import MicroWorkflow

from .run_session import active_workflow_run


class _WaitDeadlockResolver:
    def __init__(self):
        self.lock = Lock()
        self.blocked_components: set[tuple[str, ...]] = set()

    def __call__(self, component_nodes, queued_nodes, blockers):
        component = tuple(component_nodes)
        with self.lock:
            names = ", ".join(component)
            print(f"Waiting deadlock in Hoeflein component {{{names}}}:")
            print("every queued node is waiting for another node in the component.")
            print()
            for index, node in enumerate(queued_nodes, 1):
                waiting_on = ", ".join(blockers.get(node, ())) or "component peer"
                queued = "?"
                print(f"  {index}. {node} (waiting on: {waiting_on})")
            while True:
                try:
                    answer = input("Node to run [number/name, q to leave blocked]: ").strip()
                except (EOFError, OSError):
                    answer = "q"
                if not answer or answer.casefold() in {"q", "quit", "leave"}:
                    self.blocked_components.add(component)
                    print("Leaving the Hoeflein component blocked.")
                    return None
                if answer.isdigit():
                    position = int(answer)
                    if 1 <= position <= len(queued_nodes):
                        choice = queued_nodes[position - 1]
                        print(f"Temporarily overriding waiting for node {choice}.")
                        return choice
                if answer in queued_nodes:
                    print(f"Temporarily overriding waiting for node {answer}.")
                    return answer
                print("Choose one listed number or node name, or q.")


def run_nodes(
    workflow: MicroWorkflow,
    nodes: list[str],
    start_node: str,
    *,
    command: str = "run",
    components: tuple[tuple[str, ...], ...] | None = None,
    stats: bool = False,
    stats_interval: float = 5.0,
    monitor: bool = False,
    monitor_interval: float = 2.0,
    prepare: Callable[[], None] | None = None,
    fresh_preparation: bool = False,
    interrupt_preflight=None,
    execution_session_id: str | None = None,
    refuse_after_node: str | None = None,
    refuse_before_node: str | None = None,
) -> int:
    refusal_event = Event()
    interrupt_blocked = frozenset(() if interrupt_preflight is None else interrupt_preflight.blocked_components)
    refuse_after_component = (
        workflow.component_key(workflow.component_for(refuse_after_node))
        if refuse_after_node is not None
        else None
    )
    refuse_before_component = (
        workflow.component_key(workflow.component_for(refuse_before_node))
        if refuse_before_node is not None
        else None
    )
    if refuse_after_component is not None and refuse_before_component is not None:
        raise ValueError("refuse and refuseafter are mutually exclusive")
    wait_deadlock_resolver = (
        _WaitDeadlockResolver()
        if command in {"run", "runfrom", "runbetween", "resume", "resumefrom", "resumebetween"}
        else None
    )

    with active_workflow_run(
        workflow,
        queue_autostarts=True,
        command=command,
        start_node=start_node,
        nodes=nodes,
        ordered_components=components,
        fresh_preparation=fresh_preparation,
        refuse_after_node=refuse_after_node,
        refuse_before_node=refuse_before_node,
        stats=stats,
        stats_interval=stats_interval,
        monitor=monitor,
        monitor_interval=monitor_interval,
        interrupt_preflight=interrupt_preflight,
        execution_session_id=execution_session_id,
    ) as finish_run:
        execution_context = workflow.execution_session_context
        if prepare is not None:
            prepare()

        ran = workflow._run_concurrently(
            execution_context=execution_context,
            _session_driver=finish_run,
            _sequential=workflow.runner not in {'threaded', 'api', 'process'},
            nodes=nodes,
            _components=components,
            interrupt_blocked_components=interrupt_blocked,
            refuse_after_component=refuse_after_component,
            refuse_before_component=refuse_before_component,
            refusal_event=refusal_event,
            wait_deadlock_resolver=wait_deadlock_resolver,
            wait_deadlock_blocked_components=(
                wait_deadlock_resolver.blocked_components
                if wait_deadlock_resolver is not None else None
            ),
        )

        states = workflow.storage.read_component_states(
            workflow.execution_components(nodes), expected_shape=execution_context[2],
        )

        fenced = frozenset(
            component for component in states
            if workflow.storage.read_interrupt_execution_fences(execution_context[0], component)
        )
        stopped = interrupt_blocked | fenced

        if refusal_event.is_set():
            if refuse_before_component is not None:
                finish_run("stopped" if stopped else "done")
                boundary = ", ".join(refuse_before_component)
                print(
                    "Refused Hoeflein-component admission before "
                    f"{{{boundary}}} started."
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
                return 0

            failed_boundary = states[refuse_after_component]['lifecycle'] == 'failed'
            finish_run("failed" if failed_boundary else "stopped" if stopped else "done")
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

        blocked = [component for component, state in states.items()
                   if component not in stopped and state['lifecycle'] == 'queued']

        if blocked:
            finish_run("blocked")
            print("Stopped before these queued components became ready:")
            for component in blocked:
                print(f"  {{{', '.join(component)}}}: queued")
            return 1

        unfinished = [component for component, state in states.items()
                      if component not in stopped and state['lifecycle'] != 'done']

        if unfinished:
            finish_run("incomplete")
            print("These components did not complete:")
            for component in unfinished:
                print(f"  {{{', '.join(component)}}}: {states[component]['lifecycle']}")
            return 1

        finish_run("stopped" if stopped else "done")
        if stopped:
            print("Stopped at interrupt boundaries; remaining work is available for a later command.")
            for component in sorted(stopped):
                print(f"  {{{', '.join(component)}}}")
        print("Ran:")
        for node in ran:
            print(f"  {node}")

        return 0
