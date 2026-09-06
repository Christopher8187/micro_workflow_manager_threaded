from __future__ import annotations

from contextlib import contextmanager

from ..storage.component_states import ComponentTaskParent
from .execution_session import execution_session, validate_selected_jobs
from .preparation import observe_programmatic_fresh_preparation, prepare_admitted_programmatic_components


@contextmanager
def programmatic_execution(
    workflow, *, command, start_node, nodes, selected_jobs=None, include_driver=False, include_parent=False,
    fresh=False,
):
    """Admit an independent call, or reuse the exact owner of a current task."""
    execution_id = getattr(workflow._job_context, 'execution_id', None)
    if execution_id is not None:
        current_node = workflow._job_context.node_name
        workflow.check_job_execution(
            current_node, workflow._job_context.job_id,
            workflow._job_context.generation, execution_id,
        )
        owner = workflow.storage.get_job_execution_owner(execution_id)
        session_id, component = workflow.execution_claim_context(current_node)
        if owner is None or owner['session_id'] != session_id or owner['component'] != component:
            raise RuntimeError('The current task does not own this execution session')
        if workflow.storage.get_component_reservation(component) != {'members': component, 'session_id': session_id}:
            raise RuntimeError('The current task no longer owns its component reservation')
        for node in nodes:
            requested_session, requested_component = workflow.execution_claim_context(node)
            if requested_session != session_id:
                raise RuntimeError('The requested work belongs to a different execution session')
            if workflow.storage.get_component_reservation(requested_component) != {
                'members': requested_component, 'session_id': session_id,
            }:
                raise RuntimeError('The requested component is no longer reserved by this execution session')
        validate_selected_jobs(workflow, start_node, selected_jobs)
        context = workflow.execution_session_context
        parent = ComponentTaskParent(
            current_node, workflow._job_context.job_id, workflow._job_context.generation,
            execution_id, session_id, component,
        )
        yield (context, None, parent) if include_parent else (context, None) if include_driver else context
        return
    preparation = observe_programmatic_fresh_preparation(workflow, nodes) if fresh else None
    with execution_session(
        workflow, command=command, start_node=start_node,
        nodes=nodes, selected_jobs=selected_jobs,
    ) as driver:
        if preparation is not None:
            prepare_admitted_programmatic_components(workflow, nodes, preparation)
        context = workflow.execution_session_context
        yield (context, driver, None) if include_parent else (context, driver) if include_driver else context
