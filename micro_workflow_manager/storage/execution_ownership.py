from __future__ import annotations

import json

from micro_workflow_manager.component_identity import decode_component_key


class JobExecutionOwnerStorageMixin:
    """Read exact current execution ownership and accepted successor metadata."""

    def get_job_execution_owner(self, execution_id: str) -> dict | None:
        self._require_execution_session_storage()
        row = self.db_connection().execute(
            'SELECT * FROM job_execution_owners WHERE execution_id=?', (execution_id,),
        ).fetchone()
        if row is None:
            return None
        result = dict(row)
        result['component'] = decode_component_key(result.pop('component_key'))
        return result

    def read_job_current_owner(self, node_name: str, job_id: int) -> dict | None:
        """Read the active claim or explicit last committed claim for this job."""
        self._require_execution_session_storage()
        node_name = self.validate_node_name(node_name)
        job_id = self.validate_job_id(job_id)
        return self._read_job_current_owner(self.db_connection(), node_name, job_id)

    @staticmethod
    def _read_job_current_owner(connection, node_name: str, job_id: int) -> dict | None:
        observation = JobExecutionOwnerStorageMixin._read_job_owner_observation(connection, node_name, job_id)
        return None if observation is None else observation['owner']

    def read_job_owner_observation(self, node_name: str, job_id: int) -> dict | None:
        """Read one coherent job, ownership, and owning-session observation."""
        self._require_execution_session_storage()
        node_name = self.validate_node_name(node_name)
        job_id = self.validate_job_id(job_id)
        return self._read_job_owner_observation(self.db_connection(), node_name, job_id)

    @staticmethod
    def _read_owned_restart(connection, node, job_id, session_id, component):
        observed = JobExecutionOwnerStorageMixin._read_job_owner_observation(connection, node, job_id)
        owner = None if observed is None else observed['owner']
        if (owner is None or owner['session_id'] != session_id or owner['component'] != component
                or observed['status'] != 'queued' or observed['active_execution_id'] is not None
                or observed['generation'] <= owner['generation']):
            return None
        marker = connection.execute(
            'SELECT restart_requested_at FROM jobs WHERE node_name=? AND job_id=?', (node, job_id),
        ).fetchone()['restart_requested_at']
        event = connection.execute(
            "SELECT time, data_json FROM job_events WHERE node_name=? AND job_id=? "
            "AND event='restart_requested' ORDER BY event_id DESC LIMIT 1", (node, job_id),
        ).fetchone()
        data = {} if event is None else json.loads(event['data_json'])
        if (marker is None or event is None or event['time'] != marker
                or data.get('generation') != observed['generation']
                or data.get('job_instance_id') != observed['job_instance_id']
                or data.get('execution_id') != owner['execution_id']
                or data.get('session_id') != session_id or data.get('component') != list(component)):
            raise RuntimeError('Nested component restart has inconsistent accepted-request metadata')
        return {'job_instance_id': observed['job_instance_id'], 'generation': observed['generation'],
                'owner': owner, 'restart_requested_at': marker}

    @staticmethod
    def _is_claimed_component_restart(connection, observed):
        owner = observed['owner']
        if owner['generation'] != observed['generation'] or observed['generation'] == 0:
            return False
        event = connection.execute(
            "SELECT data_json FROM job_events WHERE node_name=? AND job_id=? "
            "AND event='restart_requested' ORDER BY event_id DESC LIMIT 1",
            (owner['node_name'], owner['job_id']),
        ).fetchone()
        if event is None:
            return False
        data = json.loads(event['data_json'])
        if (data.get('generation') != observed['generation']
                or data.get('job_instance_id') != observed['job_instance_id']):
            return False
        previous = connection.execute(
            'SELECT * FROM job_execution_owners WHERE execution_id=?', (data.get('execution_id'),),
        ).fetchone()
        if (data.get('session_id') != owner['session_id'] or data.get('component') != list(owner['component'])
                or previous is None or previous['node_name'] != owner['node_name']
                or previous['job_id'] != owner['job_id'] or previous['session_id'] != owner['session_id']
                or decode_component_key(previous['component_key']) != owner['component']
                or previous['job_instance_id'] != observed['job_instance_id']
                or type(previous['generation']) is not int or previous['generation'] >= owner['generation']):
            raise RuntimeError('Claimed component restart has inconsistent accepted-request metadata')
        return True

    @staticmethod
    def _read_job_owner_observation(connection, node_name: str, job_id: int) -> dict | None:
        row = connection.execute(
            'SELECT j.generation AS job_generation, j.status AS job_status, '
            'j.active_execution_id, j.active_pid, j.active_started_at, '
            'i.instance_id, i.last_execution_id, o.*, s.session_id AS owner_session, '
            'selected.session_id AS selected_session, '
            's.session_kind AS owner_session_kind, s.status AS owner_session_status, '
            's.parent_session_id AS owner_parent_session, s.outcome AS owner_session_outcome, '
            's.hostname AS owner_hostname, s.pid AS owner_pid, '
            's.process_identity AS owner_process_identity, s.heartbeat_at AS owner_heartbeat, '
            's.started_at AS owner_started_at, s.finished_at AS owner_finished_at '
            'FROM jobs AS j LEFT JOIN job_instances AS i USING(node_name, job_id) '
            'LEFT JOIN job_execution_owners AS o '
            'ON o.execution_id=COALESCE(j.active_execution_id, i.last_execution_id) '
            'LEFT JOIN execution_sessions AS s ON s.session_id=o.session_id '
            'LEFT JOIN session_components AS selected '
            'ON selected.session_id=o.session_id AND selected.component_key=o.component_key '
            'WHERE j.node_name=? AND j.job_id=?', (node_name, job_id),
        ).fetchone()
        if row is None:
            return None
        instance = row['instance_id']
        if (type(instance) is not str or len(instance) != 32
                or any(character not in '0123456789abcdef' for character in instance)):
            raise RuntimeError(f'Missing or damaged current job instance for {node_name}/{job_id}')
        active = row['active_execution_id']
        last = row['last_execution_id']
        if active is not None and active != last:
            raise RuntimeError(f'Ambiguous current execution ownership for {node_name}/{job_id}')
        observation = {
            'job_instance_id': instance, 'generation': row['job_generation'],
            'status': row['job_status'], 'active_execution_id': active,
            'active_pid': row['active_pid'], 'active_started_at': row['active_started_at'],
            'state': 'unclaimed', 'owner': None, 'session': None,
        }
        if active is None and last is None:
            return observation
        if (row['execution_id'] is None or row['owner_session'] is None or row['selected_session'] is None
                or row['node_name'] != node_name or row['job_id'] != job_id
                or row['job_instance_id'] != instance
                or type(row['generation']) is not int or row['generation'] < 0
                or row['generation'] > row['job_generation']
                or (active is not None and row['generation'] != row['job_generation'])):
            raise RuntimeError(f'Damaged current execution ownership for {node_name}/{job_id}')
        component = decode_component_key(row['component_key'])
        if node_name not in component:
            raise RuntimeError(f'Damaged current execution component for {node_name}/{job_id}')
        observation['owner'] = {
            'execution_id': row['execution_id'], 'node_name': row['node_name'],
            'job_id': row['job_id'], 'generation': row['generation'],
            'session_id': row['session_id'], 'component': component,
            'job_instance_id': row['job_instance_id'],
        }
        observation['state'] = 'active' if active is not None else 'last'
        observation['session'] = {
            'session_id': row['owner_session'], 'session_kind': row['owner_session_kind'],
            'status': row['owner_session_status'], 'parent_session_id': row['owner_parent_session'],
            'outcome': row['owner_session_outcome'],
            'hostname': row['owner_hostname'], 'pid': row['owner_pid'],
            'process_identity': row['owner_process_identity'], 'heartbeat_at': row['owner_heartbeat'],
            'started_at': row['owner_started_at'], 'finished_at': row['owner_finished_at'],
        }
        return observation
