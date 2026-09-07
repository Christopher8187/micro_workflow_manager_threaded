"""Observe full preparation effects through immutable producing executions."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from micro_workflow_manager.component_identity import encode_component_key
from .input_publication_files import checked_input_path
from .job_preparation import read_job_preparation, _read_jobs, _read_orphan_creations


@dataclass(frozen=True)
class PreparedInput:
    receiver: str
    relative: str
    owners: tuple[dict, ...]

    @property
    def producer(self):
        return self.owners[0]


@dataclass(frozen=True)
class PreparationUnit:
    component: tuple[str, ...]
    jobs: tuple
    inputs: tuple[PreparedInput, ...]
    excluded_nodes: tuple[str, ...]


@dataclass(frozen=True)
class PreparationFootprint:
    units: tuple[PreparationUnit, ...]
    owners: tuple[dict, ...]
    roots: tuple[tuple[str, int, str], ...] = ()

    @property
    def excluded_nodes(self):
        return tuple(sorted({node for unit in self.units for node in unit.excluded_nodes}))


def _selected_owners(storage, connection, components):
    result = []
    for component in components:
        for row in connection.execute(
            'SELECT execution_id FROM job_execution_owners WHERE component_key=? ORDER BY execution_id',
            (encode_component_key(component),),
        ):
            owner = storage._read_execution_owner(connection, row['execution_id'])
            if owner is None:
                raise RuntimeError('Missing preparation producing execution')
            result.append(owner)
    return tuple(result)


class _SnapshotPreparationStorage:
    """Only the validated readers required by preparation observation."""

    def __init__(self, connection, project_root):
        self._connection = connection
        self.project_dir = Path(project_root)

    def db_connection(self):
        return self._connection

    @staticmethod
    def validate_node_name(node_name):
        from .base import FileStorageBase

        return FileStorageBase.validate_node_name(node_name)

    @staticmethod
    def _read_execution_owner(connection, execution_id):
        from .execution_ownership import JobExecutionOwnerStorageMixin

        return JobExecutionOwnerStorageMixin._read_execution_owner(connection, execution_id)

    @staticmethod
    def _read_job_owner_observation(connection, node_name, job_id):
        from .execution_ownership import JobExecutionOwnerStorageMixin

        return JobExecutionOwnerStorageMixin._read_job_owner_observation(
            connection, node_name, job_id,
        )

    @staticmethod
    def _require_settled_input_publications(connection, receiver):
        from .input_publications import InputPublicationStorageMixin

        return InputPublicationStorageMixin._require_settled_input_publications(
            connection, receiver,
        )

    @staticmethod
    def _validate_input_edge(connection, owner, receiver):
        from .input_publications import InputPublicationStorageMixin

        return InputPublicationStorageMixin._validate_input_edge(
            connection, owner, receiver,
        )

    def _read_input_ownership(self, connection, receiver, relative):
        from .input_publications import InputPublicationStorageMixin

        return InputPublicationStorageMixin._read_input_ownership(
            self, connection, receiver, relative,
        )

    def _validate_pending_component_row(self, connection, row):
        from .component_settlement import ComponentSettlementStorageMixin

        return ComponentSettlementStorageMixin._validate_pending_component_row(
            self, connection, row,
        )


def read_preparation_footprint(storage, components, *, keep_trace=False):
    """Observe a live store through the same connection/root snapshot reader."""
    return _read_preparation_footprint(
        storage, storage.db_connection(), components, keep_trace=keep_trace,
    )


def read_preparation_footprint_snapshot(
    connection, project_root, components, *, keep_trace=False,
):
    """Observe full fresh-preparation effects from a query-only snapshot."""
    components = tuple(components)
    storage = _SnapshotPreparationStorage(connection, project_root)
    footprint = _read_preparation_footprint(
        storage, connection, components, keep_trace=keep_trace,
    )
    from .preparation_guards import refuse_unfinished_preparation

    for component in components:
        key = encode_component_key(component)
        if connection.execute(
            "SELECT 1 FROM preparation_receipts WHERE component_key=? AND state='prepared' LIMIT 1",
            (key,),
        ).fetchone():
            raise RuntimeError('Fresh planning requires recovery of unfinished component preparation: ' + key)
    selected_executions = tuple(owner['execution_id'] for owner in footprint.owners)
    if selected_executions:
        placeholders = ','.join('?' for _ in selected_executions)
        if connection.execute(
            "SELECT 1 FROM input_publications WHERE state='prepared' "
            f"AND execution_id IN ({placeholders}) LIMIT 1",
            selected_executions,
        ).fetchone():
            raise RuntimeError('Fresh planning requires recovery of a selected producer publication')
    receivers = {
        plan.node for unit in footprint.units for plan in unit.jobs
    } | {
        item.receiver for unit in footprint.units for item in unit.inputs
    }
    for receiver in sorted(receivers):
        refuse_unfinished_preparation(connection, receiver)
        storage._require_settled_input_publications(connection, receiver)
    return footprint


def _read_preparation_footprint(storage, connection, components, *, keep_trace):
    """Assign selected receiver changes to that receiver's own commit unit."""
    components = tuple(components)
    selected = set(components)
    selected_nodes = {node: component for component in components for node in component}
    connection.execute('SAVEPOINT mwf_preparation_footprint')
    try:
        owners = _selected_owners(storage, connection, components)
        selected_ids = {owner['execution_id'] for owner in owners}
        inputs = {component: [] for component in components}
        for item in read_prepared_inputs(storage, connection, selected_ids):
            component = selected_nodes.get(item.receiver, item.producer['component'])
            inputs[component].append(item)
        outside = sorted({row['node_name'] for row in connection.execute(
            "SELECT node_name FROM jobs UNION SELECT node_name FROM job_events WHERE event='created'",
        )}
                         - set(selected_nodes))
        units = []
        for position, component in enumerate(components):
            jobs = read_job_preparation(storage, component, selected,
                                        reset_retained=True, preserve_external=position > 0)
            external = tuple(plan for plan in read_job_preparation(
                storage, outside, {component}, reset_retained=False, preserve_external=True,
            ) if plan.delete_ids or (plan.orphan_ids and not keep_trace))
            excluded = {plan.node for plan in external}
            excluded.update(item.receiver for item in inputs[component] if item.receiver not in selected_nodes)
            units.append(PreparationUnit(component, jobs + external, tuple(inputs[component]), tuple(sorted(excluded))))
        return PreparationFootprint(tuple(units), owners)
    finally:
        connection.execute('RELEASE SAVEPOINT mwf_preparation_footprint')


def validate_preparation_footprint(storage, connection, footprint):
    components = tuple(unit.component for unit in footprint.units)
    owners = (storage._read_selected_preparation_owners(connection, components[0], footprint.roots)
              if footprint.roots else _selected_owners(storage, connection, components))
    if owners != footprint.owners:
        raise RuntimeError('Producing executions changed during preparation preflight')
    checked = set()
    for unit in footprint.units:
        for plan in unit.jobs:
            if plan.node in checked:
                continue
            checked.add(plan.node)
            if (_read_jobs(connection, plan.node) != plan.jobs
                    or _read_orphan_creations(connection, plan.node) != plan.orphan_creations):
                raise RuntimeError('Jobs changed during preparation preflight: ' + plan.node)
        validate_prepared_inputs(storage, connection, unit.inputs)


def validate_prepared_inputs(storage, connection, inputs):
    for item in inputs:
        storage._require_settled_input_publications(connection, item.receiver)
        ownership = storage._read_input_ownership(connection, item.receiver, item.relative)
        if (ownership is None or ownership[0]
                or tuple(sorted(ownership[1], key=lambda owner: owner['execution_id'])) != item.owners):
            raise RuntimeError(f'Managed input changed during preparation: {item.receiver}/{item.relative}')


def read_prepared_inputs(storage, connection, executions):
    """Capture only unambiguous managed files owned by exact selected executions."""
    result = []
    for row in connection.execute('SELECT receiver_node, relative_path FROM managed_input_files '
                                  'ORDER BY receiver_node, relative_path'):
        receiver, relative = row['receiver_node'], row['relative_path']
        unowned, claimants = storage._read_input_ownership(connection, receiver, relative)
        matching = [owner for owner in claimants if owner['execution_id'] in executions]
        if not matching:
            continue
        storage._require_settled_input_publications(connection, receiver)
        if unowned or len(matching) != len(claimants):
            raise RuntimeError(f'Preparation cannot restore ambiguous managed input: {receiver}/{relative}')
        if len({owner['component'] for owner in matching}) != 1:
            raise RuntimeError(f'Preparation input has multiple producing components: {receiver}/{relative}')
        path = checked_input_path(storage, receiver, relative)
        if path.exists() and not path.is_file():
            raise ValueError(f'Expected managed input file: {path}')
        result.append(PreparedInput(receiver, relative, tuple(sorted(claimants, key=lambda owner: owner['execution_id']))))
    return tuple(result)
