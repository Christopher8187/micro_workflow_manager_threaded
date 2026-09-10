"""Prepare exact selected roots without realigning their component."""

from __future__ import annotations

from ..storage.preparation_execution import prepare_component_unit
from ..storage.preparation_guards import hold_preparation_guards
from ..storage.selected_preparation import read_selected_preparation_footprint
from .preparation import observe_programmatic_fresh_preparation
from .interrupt_execution import frozen_component_readiness


def _normalize_selected_addresses(storage, addresses):
    normalized = []
    for address in addresses:
        try:
            node, job_id = address
        except (TypeError, ValueError) as error:
            raise ValueError('Selected preparation requires node/job-ID pairs') from error
        normalized.append((storage.validate_node_name(node), storage.validate_job_id(job_id)))
    if not normalized or len(normalized) != len(set(normalized)):
        raise ValueError('Selected preparation requires distinct job addresses')
    return tuple(normalized)


def prepare_selected_addresses(
    root, workflow, addresses, *, selection=None, keep_trace=False, operation='run',
):
    """Prepare ordered roots from one captured component without changing their order."""
    storage = workflow.storage
    addresses = _normalize_selected_addresses(storage, addresses)
    selected_nodes = list(dict.fromkeys(node for node, _ in addresses))
    with storage.interprocess_lock('active-run-state'):
        with workflow.lock:
            snapshot = workflow.topology.snapshot()
            if any(node not in workflow.graph_obj for node in selected_nodes):
                raise ValueError('Selected preparation contains a node outside the captured graph')
            components = {workflow.component_id(node) for node in selected_nodes}
            if len(components) != 1:
                raise ValueError('Selected preparation requires one exact component')
            component, = components
        context = workflow.execution_session_context
        session_id = None if context is None else context[0]
        if context is None:
            storage.refuse_live_sessions_for_reset()
            storage.register_component_topology(snapshot)
            roots = tuple(
                (node, job_id, storage.read_job_instance_id(node, job_id))
                for node, job_id in addresses
            )
        else:
            if context[2] != snapshot.shape_json or set(context[1].values()) != {component}:
                raise RuntimeError('Selected preparation component changed after admission')
            frozen = frozen_component_readiness(workflow, component)
            observed = observe_programmatic_fresh_preparation(
                workflow, selected_nodes,
                interrupt_component=component if frozen is not None else None,
            )
            if selection is not None and observed != selection:
                raise RuntimeError('Selected preparation changed after admission')
            roots = tuple(storage._read_session_job_roots(storage.db_connection(), session_id))
            if tuple((node, job_id) for node, job_id, _ in roots) != addresses:
                raise RuntimeError('Selected preparation roots changed after admission')
        connection = storage.db_connection()
        connection.execute('SAVEPOINT mwf_selected_preparation_observation')
        try:
            expected = storage._read_component_preparation(
                connection, session_id, component, snapshot.shape_json, selected_roots=roots,
            )
            footprint = read_selected_preparation_footprint(
                storage, component, roots, keep_trace=keep_trace,
            )
        finally:
            connection.execute('RELEASE SAVEPOINT mwf_selected_preparation_observation')
        with hold_preparation_guards(storage, footprint, session_id, {component: expected}, operation=operation) as guard_id:
            return prepare_component_unit(
                storage, root, footprint.units[0], expected, session_id, guard_id, operation,
                keep_trace=keep_trace, selected_footprint=footprint,
            )


def prepare_selected_jobs(
    root, workflow, node, job_ids, *, selection=None, keep_trace=False, operation='run',
):
    """Preserve the public single-node selected-preparation behavior."""
    storage = workflow.storage
    job_ids = tuple(storage.validate_job_id(job_id) for job_id in job_ids)
    if not job_ids or len(job_ids) != len(set(job_ids)):
        raise ValueError('Selected preparation requires distinct job IDs')
    return prepare_selected_addresses(
        root, workflow, ((node, job_id) for job_id in job_ids),
        selection=selection, keep_trace=keep_trace, operation=operation,
    )
