"""Capture exact selected roots and the historical work they produced."""

from __future__ import annotations

from dataclasses import replace

from .job_preparation import read_job_preparation
from .preparation_footprint import PreparationFootprint, PreparationUnit, read_prepared_inputs


def read_selected_preparation_footprint(storage, component, roots, *, keep_trace=False):
    connection = storage.db_connection()
    connection.execute('SAVEPOINT mwf_selected_preparation')
    try:
        owners = storage._read_selected_preparation_owners(connection, component, roots)
        executions = {owner['execution_id'] for owner in owners}
        retained = set(roots)
        nodes = sorted(set(component) | {row['node_name'] for row in connection.execute(
            "SELECT node_name FROM jobs UNION SELECT node_name FROM job_events WHERE event='created'",
        )})
        plans = []
        for plan in read_job_preparation(storage, nodes, {component}, reset_retained=False, preserve_external=True):
            reset = tuple(job.job_id for job in plan.jobs if (plan.node, job.job_id, job.instance_id) in retained)
            deleted = tuple(job.job_id for job in plan.jobs
                            if job.created_by_execution_id in executions and job.job_id not in reset)
            orphans = []
            if not keep_trace:
                for job_id in plan.orphan_ids:
                    creators = connection.execute(
                        'SELECT created_by_execution_id FROM job_execution_owners WHERE node_name=? AND job_id=?',
                        (plan.node, job_id),
                    ).fetchall()
                    if creators and all(row['created_by_execution_id'] in executions for row in creators):
                        orphans.append(job_id)
            if deleted or reset or orphans:
                plans.append(replace(plan, delete_ids=deleted, reset_ids=reset, orphan_ids=tuple(orphans),
                                     clear_output=False, mark_queued=False))
        inputs = read_prepared_inputs(storage, connection, executions)
        excluded = ({plan.node for plan in plans} | {item.receiver for item in inputs}) - set(component)
        unit = PreparationUnit(component, tuple(plans), tuple(inputs), tuple(sorted(excluded)))
        return PreparationFootprint((unit,), owners, roots)
    finally:
        connection.execute('RELEASE SAVEPOINT mwf_selected_preparation')
