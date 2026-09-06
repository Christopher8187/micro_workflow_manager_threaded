from __future__ import annotations

import json
import os
import warnings
from datetime import datetime
from uuid import uuid4

from micro_workflow_manager.component_identity import encode_component_key
from micro_workflow_manager.file_helpers import _relative_file_parts
from .input_publication_files import StagedInputFiles, checked_input_path, input_relative_path
from .events import JobEventAppend


class InputPublicationStorageMixin:
    """Record historical producing executions alongside managed input bytes."""

    def read_node_input_owner(self, receiver_node, relative_path):
        receiver = self.validate_node_name(receiver_node)
        relative = '/'.join(_relative_file_parts(relative_path))
        with self.interprocess_lock(f'node-{receiver}-input'):
            path = checked_input_path(self, receiver, relative)
            relative = input_relative_path(self, receiver, path, relative.split('/')[0])
            return self._read_committed_input_owner(receiver, relative)

    def _read_committed_input_owner(self, receiver, relative):
        connection = self._new_db_connection()
        try:
            connection.execute('BEGIN')
            self._require_settled_input_publications(connection, receiver)
            ownership = self._read_input_ownership(connection, receiver, relative)
            if ownership is None:
                return None
            unowned, owners = ownership
            if unowned or len(owners) != 1:
                raise RuntimeError(f'Ambiguous managed input ownership: {receiver}/{relative}')
            return owners[0]
        finally:
            connection.close()

    @staticmethod
    def _require_settled_input_publications(connection, receiver):
        if connection.execute("SELECT 1 FROM input_publications WHERE receiver_node=? AND state='prepared'",
                              (receiver,)).fetchone():
            raise RuntimeError('Managed input receiver has an unfinished publication requiring recovery')

    def _read_input_ownership(self, connection, receiver, relative):
        row = connection.execute(
            'SELECT unowned_predecessor FROM managed_input_files WHERE receiver_node=? AND relative_path=?',
            (receiver, relative),
        ).fetchone()
        claimants = connection.execute(
            'SELECT execution_id FROM managed_input_producers WHERE receiver_node=? AND relative_path=?',
            (receiver, relative),
        ).fetchall()
        if row is None:
            if claimants:
                raise RuntimeError('Damaged managed input ownership: orphan producers')
            return None
        if type(row['unowned_predecessor']) is not int or row['unowned_predecessor'] not in (0, 1) or not claimants:
            raise RuntimeError('Damaged managed input ownership')
        owners = []
        for claimant in claimants:
            owner = self._read_execution_owner(connection, claimant['execution_id'])
            if owner is None or relative.split('/')[0] != owner['node_name']:
                raise RuntimeError('Damaged managed input producing execution')
            self._validate_input_edge(connection, owner, receiver)
            owners.append(owner)
        return bool(row['unowned_predecessor']), owners

    @staticmethod
    def _validate_input_edge(connection, owner, receiver):
        shape = connection.execute('SELECT shape_json FROM graph_shapes WHERE shape_id=?',
                                   (owner['shape_id'],)).fetchone()
        if shape is None:
            raise RuntimeError('Managed input producing graph shape is missing')
        captured = json.loads(shape['shape_json'])
        names = [os.path.normcase(node) for node in captured['nodes']]
        if len(set(names)) != len(names):
            raise RuntimeError('Distinct raw node names share filesystem storage')
        if [owner['node_name'], receiver] not in captured['edges']:
            raise RuntimeError('Managed input receiver is outside the producing graph edge')

    def _validate_input_producer(self, connection, node, job_id, generation, execution_id, receiver):
        observed = self._read_job_owner_observation(connection, node, job_id)
        owner = None if observed is None else observed['owner']
        if (owner is None or observed['status'] != 'running'
                or observed['active_execution_id'] != execution_id
                or owner['execution_id'] != execution_id or owner['generation'] != generation):
            raise RuntimeError('Managed input publication requires its exact active producer execution')
        reservation = connection.execute(
            'SELECT session_id FROM component_reservations WHERE component_key=?',
            (encode_component_key(owner['component']),),
        ).fetchone()
        if (observed['session']['status'] != 'running' or reservation is None
                or reservation['session_id'] != owner['session_id']):
            raise RuntimeError('Managed input producer no longer owns its execution component')
        if self._read_component_producing_identity(connection, owner['component']) != (
            owner['shape_id'], owner['alignment_generation'],
        ):
            raise RuntimeError('Managed input producing component changed before publication')
        self._validate_input_edge(connection, owner, receiver)

    def publish_managed_inputs(self, node, job_id, generation, execution_id, receiver, changes, *, event_data=None):
        node = self.validate_node_name(node)
        job_id = self.validate_job_id(job_id)
        if type(generation) is not int or generation < 0:
            raise ValueError('Input producer generation must be a nonnegative integer')
        receiver = self.validate_node_name(receiver)
        self._session_text(execution_id, 'input producer execution')
        changes = tuple(changes)
        event_data = [None] * len(changes) if event_data is None else list(event_data)
        if len(event_data) != len(changes):
            raise ValueError('Input publication event count differs from change count')
        with self.guard_job_execution(node, job_id, generation, execution_id):
            with self.interprocess_lock(f'node-{receiver}-input'):
                if not changes:
                    return []
                connection = self._new_db_connection()
                try:
                    connection.execute('BEGIN')
                    self._validate_input_producer(connection, node, job_id, generation, execution_id, receiver)
                    self._require_settled_input_publications(connection, receiver)
                finally:
                    connection.close()
                operation_id = uuid4().hex
                files = StagedInputFiles(self, node, receiver, operation_id, changes)
                return self._publish_input_files(
                    node, job_id, generation, execution_id, receiver, operation_id, files, event_data,
                )

    def _publish_input_files(self, node, job_id, generation, execution_id, receiver, operation_id, files, event_data):
        def validate(connection):
            self._validate_input_producer(connection, node, job_id, generation, execution_id, receiver)
            for entry in files.entries:
                self._read_input_ownership(connection, receiver, entry['relative'])

        def prepare(connection):
            validate(connection)
            self._require_settled_input_publications(connection, receiver)
            connection.execute(
                'INSERT INTO input_publications VALUES(?,?,?,\'prepared\',?)',
                (operation_id, execution_id, receiver, json.dumps(files.manifest(), separators=(',', ':'))),
            )

        def commit(connection):
            validate(connection)
            for entry in files.entries:
                relative = entry['relative']
                if entry['change'].kind == 'delete':
                    connection.execute('DELETE FROM managed_input_files WHERE receiver_node=? AND relative_path=?',
                                       (receiver, relative))
                    continue
                connection.execute(
                    'INSERT INTO managed_input_files VALUES(?,?,?) ON CONFLICT DO NOTHING',
                    (receiver, relative, int(entry['existed'])),
                )
                connection.execute(
                    'INSERT INTO managed_input_producers VALUES(?,?,?) ON CONFLICT DO NOTHING',
                    (receiver, relative, execution_id),
                )
            for succeeded, error in self._apply_job_event_appends(connection, appends):
                if not succeeded:
                    raise error
            changed = connection.execute(
                "UPDATE input_publications SET state='committed' WHERE operation_id=? AND state='prepared'",
                (operation_id,),
            ).rowcount
            if changed != 1:
                raise RuntimeError('Managed input publication lost its prepared receipt')

        def abort(connection):
            changed = connection.execute(
                "UPDATE input_publications SET state='aborted' WHERE operation_id=? AND state='prepared'",
                (operation_id,),
            ).rowcount
            if changed != 1:
                raise RuntimeError('Managed input restoration lost its prepared receipt')

        try:
            files.stage()
            appends = []
            for entry, data in zip(files.entries, event_data):
                if data is None:
                    continue
                data = dict(data, target_node=receiver, path=f"{receiver}/input/{entry['relative']}")
                if entry['change'].kind == 'copy':
                    data['size'] = entry['size']
                appends.append(JobEventAppend(
                    node, job_id, datetime.now().isoformat(timespec='milliseconds'), 'input_forwarded',
                    json.dumps(data, ensure_ascii=False, separators=(',', ':')), generation, execution_id,
                ))
            self._submit_input_decision(operation_id, 'prepared', prepare)
            files.publish()
            self._submit_input_decision(operation_id, 'committed', commit)
        except BaseException as error:
            # A durable receipt distinguishes a failed transaction from a
            # notification error after COMMIT. If receipt reads fail, retain
            # the backups and intent instead of guessing the committed state.
            try:
                state = self._input_publication_state(operation_id)
                if state != 'committed':
                    files.restore()
                    if state == 'prepared':
                        self._submit_input_decision(operation_id, 'aborted', abort)
                    files.discard()
            except BaseException as cleanup_error:
                error.__notes__ = [*getattr(error, '__notes__', ()),
                                   f'Input publication {operation_id} retained recovery material: {cleanup_error}']
            raise
        try:
            files.discard()
        except Exception as error:
            warnings.warn(f'Committed input publication {operation_id} retained staging files: {error}', RuntimeWarning)
        return [entry['path'] for entry in files.entries]

    def _input_publication_state(self, operation_id):
        connection = self._new_db_connection()
        try:
            row = connection.execute('SELECT state FROM input_publications WHERE operation_id=?',
                                     (operation_id,)).fetchone()
            return None if row is None else row['state']
        finally:
            connection.close()

    def _submit_input_decision(self, operation_id, expected, operation):
        future = self.submit_db_mutation(operation, wait=False)
        try:
            future.result()
        except BaseException as error:
            # An interrupted waiter must not restore files while its queued
            # database decision can still commit. Drain that exact request.
            while not future.done():
                try:
                    future.result()
                except BaseException:
                    pass
            if self._input_publication_state(operation_id) != expected:
                raise
            if not isinstance(error, Exception):
                raise
            warnings.warn(f'Input publication {operation_id} committed {expected} before notification failed',
                          RuntimeWarning)
