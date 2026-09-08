"""Assign historical publications to current component preparation units."""

from dataclasses import dataclass

from .job_preparation import read_job_preparation
from .preparation_footprint import PreparationFootprint, PreparationUnit, read_prepared_inputs
from .execution_ownership import JobExecutionOwnerStorageMixin


def selected_membership_owners(connection, change):
    identities = {
        (item.members, item.shape_id, item.alignment_generation)
        for item in change.historical_memberships
    }
    result = []
    for row in connection.execute('SELECT execution_id FROM job_execution_owners ORDER BY execution_id'):
        owner = JobExecutionOwnerStorageMixin._read_execution_owner(connection, row['execution_id'])
        if (owner['component'], owner['shape_id'], owner['alignment_generation']) in identities:
            result.append(owner)
    return tuple(result)


@dataclass(frozen=True, kw_only=True)
class MembershipPreparationFootprint(PreparationFootprint):
    change: object
    start_component: tuple[str, ...]
    guarded_preparation_nodes: tuple[str, ...]

    @property
    def excluded_nodes(self):
        return tuple(sorted(set(super().excluded_nodes) | set(self.guarded_preparation_nodes)))


def read_membership_preparation_footprint(storage, change, *, start_component, keep_trace=False):
    start_component = tuple(start_component)
    if not change.requested_components or change.requested_components[0] != start_component:
        raise ValueError('Membership preparation must retain the original requested start component')
    connection = storage.db_connection()
    components = change.preparation_components
    selected_nodes = {node: component for component in components for node in component}
    execution_nodes = {node for component in change.requested_components for node in component}
    connection.execute('SAVEPOINT mwf_membership_footprint')
    try:
        owners = selected_membership_owners(connection, change)
        selected_ids = {owner['execution_id'] for owner in owners}
        assignments = {}
        for owner in owners:
            unit = selected_nodes.get(owner['node_name'])
            if unit is None:
                overlap = [component for component in components if set(component).intersection(owner['component'])]
                if not overlap:
                    # Audit identities can reach an automatically reconciled,
                    # unprepared component without carrying any live effects.
                    continue
                unit = overlap[0]
            assignments[owner['execution_id']] = unit
        inputs = {component: [] for component in components}
        for item in read_prepared_inputs(storage, connection, selected_ids):
            unit = selected_nodes.get(item.receiver, assignments.get(item.producer['execution_id']))
            if unit is None:
                raise RuntimeError('Historical managed input has no preparation unit')
            inputs[unit].append(item)
        outside = sorted({row['node_name'] for row in connection.execute(
            "SELECT node_name FROM jobs UNION SELECT node_name FROM job_events WHERE event='created'",
        )} - set(selected_nodes))
        units = []
        for component in components:
            jobs = read_job_preparation(
                storage, component, (), reset_retained=True,
                preserve_external=component != start_component,
                producer_executions=selected_ids,
            )
            assigned = {execution_id for execution_id, unit in assignments.items() if unit == component}
            external = tuple(plan for plan in read_job_preparation(
                storage, outside, (), reset_retained=False, preserve_external=True,
                producer_executions=assigned,
            ) if plan.delete_ids or (plan.orphan_ids and not keep_trace))
            excluded = {plan.node for plan in external}
            excluded.update(item.receiver for item in inputs[component] if item.receiver not in selected_nodes)
            excluded.update(node for node in component if node not in execution_nodes)
            units.append(PreparationUnit(component, jobs + external, tuple(inputs[component]), tuple(sorted(excluded))))
        return MembershipPreparationFootprint(
            units=tuple(units), owners=owners, change=change, start_component=start_component,
            guarded_preparation_nodes=tuple(sorted(set(selected_nodes) - execution_nodes)),
        )
    finally:
        connection.execute('RELEASE SAVEPOINT mwf_membership_footprint')
