"""Bind session selections to their exact admitted historical graph shape."""

from micro_workflow_manager.component_identity import encode_component_key
from .component_definitions import component_snapshot_from_shape
from .component_membership import read_active_component_partition


def read_admission_shape(connection, expected_shape, components):
    try:
        snapshot = component_snapshot_from_shape(expected_shape)
    except (TypeError, ValueError) as error:
        raise RuntimeError('Admission requires a valid registered graph shape') from error
    if not components or any(component not in snapshot.components for component in components):
        raise RuntimeError('Session selection is outside its admitted graph shape')
    row = connection.execute(
        'SELECT shape_id FROM graph_shapes WHERE shape_json=?', (expected_shape,),
    ).fetchone()
    if row is None:
        raise RuntimeError('Session admission requires its registered graph shape')
    registered = {
        item['component_key'] for item in connection.execute(
            'SELECT component_key FROM component_definitions WHERE shape_id=?', (row['shape_id'],),
        )
    }
    if registered != {encode_component_key(component) for component in snapshot.components}:
        raise RuntimeError('Session admitted shape has incomplete component definitions')
    active = read_active_component_partition(connection)
    if active is None:
        raise RuntimeError('Session admission requires initialized active membership')
    return row['shape_id'], active.revision


def validate_session_shape_snapshot(connection, session):
    shape_id = session['admitted_shape_id']
    revision = session['partition_revision']
    if (type(shape_id) is not int or shape_id < 1
            or type(revision) is not int or revision < 0):
        raise RuntimeError('Session has an invalid admitted membership identity')
    shape = connection.execute(
        'SELECT shape_json FROM graph_shapes WHERE shape_id=?', (shape_id,),
    ).fetchone()
    if shape is None:
        raise RuntimeError('Session admitted graph shape is missing')
    observed_id, current_revision = read_admission_shape(
        connection, shape['shape_json'], session['selected_components'],
    )
    if observed_id != shape_id or revision > current_revision:
        raise RuntimeError('Session admitted membership identity is inconsistent')
    return shape['shape_json']
