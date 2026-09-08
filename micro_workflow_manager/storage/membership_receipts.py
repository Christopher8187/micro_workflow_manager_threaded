"""Validate historical producer assignment in committed membership receipts."""

import json

from micro_workflow_manager.component_identity import encode_component_key
from .component_definitions import component_snapshot_from_shape
from .component_membership import (
    ActiveComponent, ActiveComponentPartition, HistoricalMembership, observe_membership_change,
)


def _validate_state(connection, state):
    fields = {
        'component_key', 'shape_id', 'retained_result_shape_id',
        'retained_result_alignment_generation', 'lifecycle', 'stability',
        'instability_origin', 'misaligned', 'alignment_generation',
    }
    if not isinstance(state, dict) or set(state) != fields:
        raise ValueError('Receipt has incomplete component state')
    for name, minimum in (('shape_id', 1), ('alignment_generation', 0), ('misaligned', 0)):
        if type(state[name]) is not int or state[name] < minimum:
            raise ValueError('Receipt has invalid component state identity')
    if state['misaligned'] not in (0, 1):
        raise ValueError('Receipt has invalid component misalignment')
    shape, generation = state['retained_result_shape_id'], state['retained_result_alignment_generation']
    if (shape is None) != (generation is None):
        raise ValueError('Receipt has incomplete retained result identity')
    if shape is not None and (type(shape) is not int or shape < 1
                             or type(generation) is not int or generation != state['alignment_generation']):
        raise ValueError('Receipt has invalid retained result identity')
    lifecycle, stability, origin = state['lifecycle'], state['stability'], state['instability_origin']
    if type(lifecycle) is not str or lifecycle not in ('queued', 'running', 'sampled', 'done', 'failed'):
        raise ValueError('Receipt has invalid component lifecycle')
    if stability is not None and (type(stability) is not str or stability not in ('stable', 'unstable')):
        raise ValueError('Receipt has invalid component stability')
    if origin is not None and (type(origin) is not str or not origin.strip()):
        raise ValueError('Receipt has invalid component origin')
    lineage = (stability == 'stable' and origin is None) or (stability == 'unstable' and origin is not None)
    if not ((lifecycle in ('queued', 'running', 'failed') and stability is None and origin is None)
            or (lifecycle in ('running', 'sampled', 'done') and lineage)):
        raise ValueError('Receipt has inconsistent component lineage')
    if lifecycle == 'queued' and state['misaligned']:
        raise ValueError('Receipt has misaligned queued state')
    if lifecycle in ('sampled', 'done') and shape is None:
        raise ValueError('Receipt lacks its successful result identity')
    if type(state['component_key']) is not str or connection.execute(
        'SELECT 1 FROM component_definitions WHERE component_key=? AND shape_id=?',
        (state['component_key'], state['shape_id']),
    ).fetchone() is None:
        raise ValueError('Receipt state lacks its historical definition')


def validate_membership_receipt_producer(connection, receipt, owner):
    try:
        manifest = json.loads(receipt['manifest_json'])
        metadata = manifest.get('membership')
        if metadata is None:
            if receipt['component_key'] != encode_component_key(owner['component']):
                raise ValueError('Ordinary receipt has a different producing component')
            return
        fields = {
            'source_partition', 'start_component', 'target_shape_id', 'target_shape_json',
            'requested_components', 'preparation_components', 'unit_position',
            'before_state', 'after_state', 'historical_memberships', 'historical_owners',
        }
        if not isinstance(metadata, dict) or set(metadata) != fields:
            raise ValueError('Incomplete membership preparation identity')
        requested = metadata['requested_components']
        start = metadata['start_component']
        if (not isinstance(requested, list) or not requested
                or not isinstance(start, list) or not start
                or any(type(node) is not str or not node for node in start)
                or start != sorted(set(start)) or start != requested[0]):
            raise ValueError('Receipt lost its exact original start component')
        if type(metadata['target_shape_id']) is not int or metadata['target_shape_id'] < 1:
            raise ValueError('Invalid receipt target shape identity')
        source = metadata['source_partition']
        active = ActiveComponentPartition(
            source['revision'], tuple(ActiveComponent(tuple(item['members']), item['established_revision'])
                                      for item in source['components']),
        )
        target = component_snapshot_from_shape(metadata['target_shape_json'])
        shape = connection.execute('SELECT shape_json FROM graph_shapes WHERE shape_id=?',
                                   (metadata['target_shape_id'],)).fetchone()
        if shape is None or shape['shape_json'] != target.shape_json:
            raise ValueError('Receipt target shape does not match its historical row')
        history = tuple(HistoricalMembership(
            tuple(item['members']), item['shape_id'], item['alignment_generation'], item['has_reusable_work'],
        ) for item in metadata['historical_memberships'])
        change = observe_membership_change(active, target, metadata['requested_components'], history)
        components = tuple(tuple(item) for item in metadata['preparation_components'])
        if change.preparation_components != components:
            raise ValueError('Receipt preparation differs from its historical overlap')
        position = metadata['unit_position']
        if type(position) is not int or not 0 <= position < len(components):
            raise ValueError('Invalid receipt preparation position')
        unit = components[position]
        if receipt['component_key'] != encode_component_key(unit):
            raise ValueError('Receipt unit does not match its component')
        expected_owner = dict(owner, component=list(owner['component']))
        exact_owner = json.dumps(expected_owner, sort_keys=True, separators=(',', ':'))
        if (not isinstance(metadata['historical_owners'], list)
                or not any(json.dumps(item, sort_keys=True, separators=(',', ':')) == exact_owner
                           for item in metadata['historical_owners'])):
            raise ValueError('Receipt lacks its exact immutable producing owner')
        owner_unit = next((component for component in components if owner['node_name'] in component), None)
        if owner_unit is None:
            owner_unit = next((component for component in components if set(component).intersection(owner['component'])), None)
        if owner_unit != unit:
            raise ValueError('Receipt assigns the producer to a different current component')
        before, after = metadata['before_state'], metadata['after_state']
        _validate_state(connection, before)
        _validate_state(connection, after)
        floor = change.preparation[position].generation_floor
        expected_generation = max(before['alignment_generation'], 0 if floor is None else floor) + 1
        expected_after = dict(
            before, shape_id=metadata['target_shape_id'], retained_result_shape_id=None,
            retained_result_alignment_generation=None, lifecycle='queued', stability=None,
            instability_origin=None, misaligned=0, alignment_generation=expected_generation,
        )
        if before['component_key'] != receipt['component_key'] or after != expected_after:
            raise ValueError('Receipt target state does not match its full preparation')
    except (KeyError, TypeError, ValueError, StopIteration) as error:
        raise RuntimeError('Damaged membership preparation producing identity') from error
