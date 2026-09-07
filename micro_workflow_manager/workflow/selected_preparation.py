"""Prepare exact selected roots without realigning their component."""

from __future__ import annotations

from ..storage.preparation_execution import prepare_component_unit
from ..storage.preparation_guards import hold_preparation_guards
from ..storage.selected_preparation import read_selected_preparation_footprint
from .preparation import observe_programmatic_fresh_preparation


def prepare_selected_jobs(root, workflow, node, job_ids, *, selection=None, keep_trace=False, operation='run'):
    storage = workflow.storage
    job_ids = tuple(storage.validate_job_id(job_id) for job_id in job_ids)
    if not job_ids or len(job_ids) != len(set(job_ids)):
        raise ValueError('Selected preparation requires distinct job IDs')
    with storage.interprocess_lock('active-run-state'):
        with workflow.lock:
            snapshot = workflow.topology.snapshot()
            component = workflow.component_id(node)
        context = workflow.execution_session_context
        session_id = None if context is None else context[0]
        if context is None:
            storage.refuse_live_sessions_for_reset()
            storage.register_component_topology(snapshot)
            roots = tuple((node, job_id, storage.read_job_instance_id(node, job_id)) for job_id in job_ids)
        else:
            if context[2] != snapshot.shape_json or set(context[1].values()) != {component}:
                raise RuntimeError('Selected preparation component changed after admission')
            observed = observe_programmatic_fresh_preparation(workflow, [node])
            if selection is not None and observed != selection:
                raise RuntimeError('Selected preparation changed after admission')
            roots = tuple(storage._read_session_job_roots(storage.db_connection(), session_id))
            if tuple((name, job_id) for name, job_id, _ in roots) != tuple((node, job_id) for job_id in job_ids):
                raise RuntimeError('Selected preparation roots changed after admission')
        connection = storage.db_connection()
        connection.execute('SAVEPOINT mwf_selected_preparation_observation')
        try:
            expected = storage._read_component_preparation(
                connection, session_id, component, snapshot.shape_json, selected_roots=roots,
            )
            footprint = read_selected_preparation_footprint(storage, component, roots, keep_trace=keep_trace)
        finally:
            connection.execute('RELEASE SAVEPOINT mwf_selected_preparation_observation')
        with hold_preparation_guards(storage, footprint, session_id, {component: expected}) as guard_id:
            return prepare_component_unit(
                storage, root, footprint.units[0], expected, session_id, guard_id, operation,
                keep_trace=keep_trace, selected_footprint=footprint,
            )
