from __future__ import annotations

import json

from micro_workflow_manager.component_identity import encode_component_key


class JobProducerStorageMixin:
    """Keep job creation ancestry independently of optional trace events."""

    @staticmethod
    def _job_parent_json(job):
        parent = dict(job.parent) if job.parent is not None else None
        if job.producer_component is not None:
            parent = {} if parent is None else parent
            parent['_mwf_from_component'] = list(job.producer_component)
            parent['_mwf_job_kind'] = job.job_kind
        return json.dumps(parent, ensure_ascii=False) if parent is not None else None

    def _validate_job_producer(self, connection, job, execution_id):
        if execution_id is None:
            return
        self._session_text(execution_id, 'producer execution')
        producer = connection.execute(
            'SELECT node_name, job_id FROM job_execution_owners WHERE execution_id=?', (execution_id,),
        ).fetchone()
        if producer is None:
            raise RuntimeError('Job creation has no recorded producer execution')
        node, job_id = producer['node_name'], producer['job_id']
        observed = self._read_job_owner_observation(connection, node, job_id)
        owner = None if observed is None else observed['owner']
        if (owner is None or observed['active_execution_id'] != execution_id
                or observed['status'] != 'running' or owner['execution_id'] != execution_id
                or owner['component'] != job.producer_component):
            raise RuntimeError('Job creation requires its exact active producer execution')
        reservation = connection.execute(
            'SELECT session_id FROM component_reservations WHERE component_key=?',
            (encode_component_key(owner['component']),),
        ).fetchone()
        if (observed['session']['status'] != 'running' or reservation is None
                or reservation['session_id'] != owner['session_id']):
            raise RuntimeError('Job producer no longer owns its execution component')
        if self._read_component_producing_identity(connection, owner['component']) != (
            owner['shape_id'], owner['alignment_generation'],
        ):
            raise RuntimeError('Job producing component changed before creation')
        expected_kind = 'component' if job.node_name in owner['component'] else 'dag'
        if job.job_kind != expected_kind:
            raise RuntimeError('Created job kind does not match its producing component')

    @staticmethod
    def _record_job_producer(connection, node_name, job_id, execution_id):
        if execution_id is None:
            return
        changed = connection.execute(
            'UPDATE job_instances SET created_by_execution_id=? '
            'WHERE node_name=? AND job_id=? AND created_by_execution_id IS NULL',
            (execution_id, node_name, job_id),
        ).rowcount
        if changed != 1:
            raise RuntimeError('Job creation did not record its exact producer')

    def _job_descends_from_selected_root(self, connection, creator, roots, session_id, component, producing_identity):
        visited = set()
        while creator is not None:
            if creator in visited:
                raise RuntimeError('Cyclic selected job creation ancestry')
            visited.add(creator)
            owner = self._read_execution_owner(connection, creator)
            if owner is None:
                raise RuntimeError('Missing selected job creation ancestry')
            if (owner['session_id'] != session_id or owner['component'] != component
                    or (owner['shape_id'], owner['alignment_generation']) != producing_identity):
                return False
            if (owner['node_name'], owner['job_id'], owner['job_instance_id']) in roots:
                return True
            creator = owner['created_by_execution_id']
        return False

    def _read_selected_preparation_owners(self, connection, component, roots):
        """Walk historical descendants forward, stopping at component boundaries."""
        owners = {}
        for node, job_id, instance in roots:
            for row in connection.execute(
                'SELECT execution_id FROM job_execution_owners '
                'WHERE node_name=? AND job_id=? AND job_instance_id=? ORDER BY execution_id',
                (node, job_id, instance),
            ):
                owner = self._read_execution_owner(connection, row['execution_id'])
                if owner['component'] == component:
                    owners[owner['execution_id']] = owner
        pending = list(owners)
        while pending:
            producer = pending.pop()
            for row in connection.execute(
                'SELECT execution_id FROM job_execution_owners '
                'WHERE created_by_execution_id=? AND component_key=? ORDER BY execution_id',
                (producer, encode_component_key(component)),
            ):
                execution_id = row['execution_id']
                if execution_id not in owners:
                    owners[execution_id] = self._read_execution_owner(connection, execution_id)
                    pending.append(execution_id)
        return tuple(owners[key] for key in sorted(owners))

    def _selected_job_claim_error(self, connection, batch, node_rows, producing_identity, checked):
        try:
            if batch.session_id not in checked:
                checked[batch.session_id] = set(self._read_session_job_roots(connection, batch.session_id))
            roots = checked[batch.session_id]
            if not roots:
                return None
            for job_id in batch.job_ids:
                row = node_rows[job_id]
                if (batch.node_name, job_id, row['instance_id']) in roots:
                    continue
                if not self._job_descends_from_selected_root(
                    connection, row['created_by_execution_id'], roots,
                    batch.session_id, batch.component, producing_identity,
                ):
                    raise RuntimeError(f'Job {batch.node_name}/{job_id} is outside selected job causal scope')
        except Exception as error:
            return error
        return None
