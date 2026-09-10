"""Read reusable membership history without relabelling immutable executions."""

from micro_workflow_manager.component_identity import decode_component_key, encode_component_key
from .component_definitions import component_snapshot_from_shape
from .component_membership import HistoricalMembership, read_active_component_partition
from .component_states import read_component_state_snapshot
from .execution_ownership import JobExecutionOwnerStorageMixin


def read_membership_history(connection):
    active = read_active_component_partition(connection)
    if active is None:
        if connection.execute('SELECT 1 FROM component_definitions LIMIT 1').fetchone():
            raise RuntimeError('Stored component history has no active membership partition')
        return ()
    active_keys = {item.members for item in active.components}
    definitions = {}
    for row in connection.execute(
        'SELECT definition.component_key, definition.shape_id, shape.shape_json '
        'FROM component_definitions AS definition LEFT JOIN graph_shapes AS shape USING(shape_id) '
        'ORDER BY definition.component_key, definition.shape_id',
    ):
        try:
            component = decode_component_key(row['component_key'])
            snapshot = component_snapshot_from_shape(row['shape_json'])
        except (TypeError, ValueError) as error:
            raise RuntimeError('Damaged historical membership definition') from error
        if encode_component_key(component) != row['component_key'] or component not in snapshot.components:
            raise RuntimeError('Historical membership is outside its producing graph shape')
        definitions[component, row['shape_id']] = snapshot

    identities = {}

    def remember(component, shape_id, generation, reusable=False):
        if (component, shape_id) not in definitions:
            raise RuntimeError('Membership history has no exact historical definition')
        item = HistoricalMembership(component, shape_id, generation, reusable)
        key = component, shape_id, generation
        identities[key] = identities.get(key, False) or item.has_reusable_work
        return key

    states = {}
    for row in connection.execute('SELECT * FROM component_states ORDER BY component_key'):
        component = decode_component_key(row['component_key'])
        state = read_component_state_snapshot(connection, component)
        if state is None:
            raise RuntimeError('Historical component state disappeared during observation')
        states[component] = state
        remember(component, row['shape_id'], state['alignment_generation'])
        retained_shape = row['retained_result_shape_id']
        retained_generation = row['retained_result_alignment_generation']
        if retained_shape is not None:
            remember(component, retained_shape, retained_generation, component in active_keys)
        if component in active_keys and state['lifecycle'] in ('sampled', 'done') and retained_shape is None:
            raise RuntimeError('Successful active membership has no retained result identity')
    if {component for component, _ in definitions} - set(states):
        raise RuntimeError('Historical membership has a missing state row')
    if active_keys - set(states):
        raise RuntimeError('Active membership has a missing state row')

    results = {}
    for row in connection.execute('SELECT * FROM component_successful_results'):
        component = decode_component_key(row['component_key'])
        key = remember(component, row['shape_id'], row['alignment_generation'])
        if row['lifecycle'] not in ('sampled', 'done') or row['stability'] not in ('stable', 'unstable'):
            raise RuntimeError('Invalid historical component result')
        if row['stability'] == 'stable':
            if row['instability_origin'] is not None:
                raise RuntimeError('Stable historical result has an instability origin')
        else:
            origin = connection.execute(
                "SELECT session_id FROM execution_sessions WHERE session_id=? "
                "AND session_kind='interrupt' AND scope_admitted=1 "
                "AND typeof(scope_admitted)='integer'",
                (row['instability_origin'],),
            ).fetchone()
            if origin is None:
                raise RuntimeError('Unstable historical result has no admitted interrupt origin')
        results[key] = row
    for row in connection.execute('SELECT * FROM component_states WHERE retained_result_shape_id IS NOT NULL'):
        key = decode_component_key(row['component_key']), row['retained_result_shape_id'], row['retained_result_alignment_generation']
        if key not in results or row['alignment_generation'] != key[2]:
            raise RuntimeError('Retained membership result identity is damaged')
        if row['lifecycle'] in ('sampled', 'done'):
            fields = ('lifecycle', 'stability', 'instability_origin')
            if tuple(row[field] for field in fields) != tuple(results[key][field] for field in fields):
                raise RuntimeError('Current membership result disagrees with retained successful history')

    owners = {}
    for row in connection.execute('SELECT execution_id FROM job_execution_owners ORDER BY execution_id'):
        owner = JobExecutionOwnerStorageMixin._read_execution_owner(connection, row['execution_id'])
        owners[row['execution_id']] = owner
        remember(owner['component'], owner['shape_id'], owner['alignment_generation'])

    reusable_executions = set()
    for row in connection.execute(
        'SELECT job.node_name, job.job_id, instance.created_by_execution_id '
        'FROM jobs AS job LEFT JOIN job_instances AS instance USING(node_name, job_id)',
    ):
        observed = JobExecutionOwnerStorageMixin._read_job_owner_observation(
            connection, row['node_name'], row['job_id'],
        )
        if observed['owner'] is not None:
            reusable_executions.add(observed['owner']['execution_id'])
        if row['created_by_execution_id'] is not None:
            reusable_executions.add(row['created_by_execution_id'])
    for row in connection.execute(
        'SELECT execution_id FROM managed_input_producers UNION '
        "SELECT execution_id FROM input_publications WHERE state='prepared'",
    ):
        reusable_executions.add(row['execution_id'])
    for execution_id in reusable_executions:
        owner = owners.get(execution_id)
        if owner is None:
            raise RuntimeError('Reusable membership work has no historical execution owner')
        remember(owner['component'], owner['shape_id'], owner['alignment_generation'], True)
    return tuple(
        HistoricalMembership(component, shape_id, generation, reusable)
        for (component, shape_id, generation), reusable in sorted(identities.items())
    )
