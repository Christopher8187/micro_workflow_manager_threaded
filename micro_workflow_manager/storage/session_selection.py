from __future__ import annotations

from micro_workflow_manager.component_identity import decode_component_key, encode_component_key
from .base import FileStorageBase


class SessionSelectionStorageMixin:
    """Read retained selection scope without depending on current job rows."""

    @staticmethod
    def _stored_session_component(key):
        try:
            component = decode_component_key(key)
            if not component or encode_component_key(component) != key:
                raise ValueError('Noncanonical component key')
            for node in component:
                FileStorageBase.validate_node_name(node)
            return component
        except (TypeError, ValueError) as error:
            raise RuntimeError('Damaged selected component declaration') from error

    @staticmethod
    def _read_session_components(connection, session_id):
        components, nodes = [], set()
        for position, row in enumerate(connection.execute(
            'SELECT position, component_key FROM session_components WHERE session_id=? ORDER BY position',
            (session_id,),
        )):
            component = SessionSelectionStorageMixin._stored_session_component(row['component_key'])
            if row['position'] != position or nodes.intersection(component):
                raise RuntimeError('Damaged selected component scope')
            components.append(component)
            nodes.update(component)
        return components

    @staticmethod
    def _read_session_job_roots(connection, session_id, *, components=None):
        session = connection.execute(
            'SELECT selection_kind, start_component FROM execution_sessions WHERE session_id=?', (session_id,),
        ).fetchone()
        rows = connection.execute(
            'SELECT position, node_name, job_id, job_instance_id FROM session_jobs '
            'WHERE session_id=? ORDER BY position', (session_id,),
        ).fetchall()
        if (session is None or session['selection_kind'] not in ('components', 'jobs')
                or (session['selection_kind'] == 'jobs') != bool(rows)):
            raise RuntimeError('Damaged selected job scope')
        if components is None:
            components = SessionSelectionStorageMixin._read_session_components(connection, session_id)
        start = SessionSelectionStorageMixin._stored_session_component(session['start_component'])
        if start not in components:
            raise RuntimeError('Session start is outside selected components')
        selected_nodes = {node for component in components for node in component}
        roots = []
        for position, row in enumerate(rows):
            instance = row['job_instance_id']
            try:
                FileStorageBase.validate_node_name(row['node_name'])
            except ValueError as error:
                raise RuntimeError('Damaged selected job node') from error
            if (row['node_name'] not in selected_nodes or row['position'] != position
                    or type(row['job_id']) is not int or row['job_id'] < 1
                    or type(instance) is not str or len(instance) != 32
                    or any(character not in '0123456789abcdef' for character in instance)):
                raise RuntimeError('Damaged selected job identity')
            roots.append((row['node_name'], row['job_id'], instance))
        return roots
