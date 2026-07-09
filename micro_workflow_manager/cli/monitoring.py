from __future__ import annotations

from micro_workflow_manager.monitor import monitor_loop
from micro_workflow_manager.system import MicroWorkflow

def monitor_command(
    workflow: MicroWorkflow,
    nodes: list[str],
    *,
    interval: float,
    once: bool,
    json_output: bool,
    no_clear: bool,
) -> int:
    monitor_loop(
        workflow,
        nodes=nodes,
        interval=interval,
        once=once,
        json_output=json_output,
        no_clear=no_clear,
    )
    return 0
