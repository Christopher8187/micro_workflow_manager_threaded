"""Read component state and raw-node concurrency for user-facing inspection."""

from __future__ import annotations


def node_observation(workflow, node_name: str) -> dict:
    storage = workflow.storage
    connection = storage.db_connection()
    connection.execute("SAVEPOINT mwf_node_observation")
    try:
        component = tuple(sorted(workflow.component_for(node_name)))
        state = storage.get_component_state(component)
        lifecycle = "queued" if state is None else state["lifecycle"]
        causes = storage.read_component_misalignment_causes(component)
        threads = storage.read_thread_override_observation(node_name)
        waiting_on = sorted(workflow.waiting_blockers(node_name))
        node = workflow.nodes.get(node_name)
        declared = getattr(node, "max_threads", 1)
        runner = getattr(node, "runner_override", None) or workflow.runner
        requested = 1 if runner == "direct" else (threads["value"] or declared)
        return {
            "component": list(component),
            "state": lifecycle,
            "status": "waiting" if lifecycle == "running" and waiting_on else lifecycle,
            "stability": None if state is None else state["stability"],
            "instability_origin": None if state is None else state["instability_origin"],
            "misaligned": False if state is None else state["misaligned"],
            "misalignment_causes": causes,
            "waiting_on": waiting_on,
            "declared_max_threads": declared,
            "thread_override": threads["value"],
            "thread_override_session_id": threads["session_id"],
            "requested_max_threads": requested,
            "runner": runner,
        }
    finally:
        connection.execute("RELEASE SAVEPOINT mwf_node_observation")
