from __future__ import annotations

from micro_workflow_manager.component_identity import decode_component_key, encode_component_key


class ComponentStateStorageMixin:
    """Read private component lifecycle records at their producing shape."""

    def get_component_state(self, component) -> dict | None:
        self._require_execution_session_storage()
        members = self._session_component(component)
        row = self.db_connection().execute(
            "SELECT d.component_key, g.shape_json, s.component_key AS state_key, "
            "s.lifecycle, s.stability, s.instability_origin, s.misaligned, s.alignment_generation, "
            "origin.session_kind AS origin_kind "
            "FROM component_definitions d "
            "LEFT JOIN graph_shapes g USING(shape_id) "
            "LEFT JOIN component_states s USING(component_key) "
            "LEFT JOIN execution_sessions origin ON origin.session_id=s.instability_origin "
            "WHERE d.component_key=?",
            (encode_component_key(members),),
        ).fetchone()
        if row is None:
            return None
        if row['state_key'] is None or row['shape_json'] is None:
            raise RuntimeError('Incomplete component state or producing graph shape')
        self._validate_component_state_row(row)
        return {
            'members': decode_component_key(row['component_key']),
            'shape_json': row['shape_json'],
            'lifecycle': row['lifecycle'],
            'stability': row['stability'],
            'instability_origin': row['instability_origin'],
            'misaligned': bool(row['misaligned']),
            'alignment_generation': row['alignment_generation'],
        }

    @staticmethod
    def _validate_component_state_row(row) -> None:
        lifecycle = row['lifecycle']
        stability = row['stability']
        origin = row['instability_origin']
        misaligned = row['misaligned']
        generation = row['alignment_generation']
        no_lineage = stability is None and origin is None
        result_lineage = (
            (stability == 'stable' and origin is None)
            or (stability == 'unstable' and isinstance(origin, str) and bool(origin.strip())
                and row['origin_kind'] == 'interrupt')
        )
        valid_lifecycle = (
            (lifecycle == 'queued' and no_lineage and misaligned == 0)
            or (lifecycle == 'running' and (no_lineage or result_lineage) and misaligned == 0)
            or (lifecycle in ('sampled', 'done') and result_lineage)
            or (lifecycle == 'failed' and no_lineage)
        )
        if not (
            valid_lifecycle
            and type(misaligned) is int and misaligned in (0, 1)
            and type(generation) is int and generation >= 0
        ):
            raise RuntimeError('Invalid component state for ' + row['component_key'])
