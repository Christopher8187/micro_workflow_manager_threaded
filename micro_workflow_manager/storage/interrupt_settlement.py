"""Release and return explicit-interrupt component scope atomically."""

from __future__ import annotations

from micro_workflow_manager.component_identity import encode_component_key
from micro_workflow_manager.session_liveness import execution_session_liveness

from .interrupt_common import _session_selected


def read_interrupt_terminal_rows(connection, session_id):
    tables = (
        ("execution_session_parents", "child_session_id"),
        ("interrupt_admissions", "session_id"),
        ("interrupt_scope_transfers", "child_session_id"),
        ("interrupt_pause_requests", "child_session_id"),
        ("interrupt_paused_executions", "child_session_id"),
        ("interrupt_frozen_parent_states", "child_session_id"),
        ("post_interrupt_fences", "source_session_id"),
        ("session_fence_authorizations", "session_id"),
    )
    return {
        table: [dict(row) for row in connection.execute(
            f"SELECT * FROM {table} WHERE {column}=? ORDER BY rowid", (session_id,),
        )]
        for table, column in tables
    }


def require_terminal_interrupt_scope(connection, session_id):
    admission = connection.execute(
        "SELECT state FROM interrupt_admissions WHERE session_id=?", (session_id,),
    ).fetchone()
    if admission is None:
        return
    if admission["state"] != "settled":
        raise RuntimeError("Terminal interrupt session retains an active admission")
    for table in ("interrupt_scope_transfers", "interrupt_pause_requests"):
        if connection.execute(
            f"SELECT 1 FROM {table} WHERE child_session_id=? "
            "AND state IN ('active','requested','acknowledged') LIMIT 1",
            (session_id,),
        ).fetchone() is not None:
            raise RuntimeError("Terminal interrupt session retains active scope")


class InterruptSettlementStorageMixin:
    def _settle_interrupt_component(self, connection, session_id, outcome, *, settled_at):
        key = encode_component_key(outcome.component)
        admission = connection.execute(
            "SELECT * FROM interrupt_admissions WHERE session_id=?", (session_id,),
        ).fetchone()
        if admission is None or admission["target_component_key"] != key:
            return False
        if admission["state"] != "frozen":
            raise RuntimeError("Interrupt target settlement requires its frozen admission")
        hold = connection.execute(
            "SELECT hold_count FROM component_holds WHERE session_id=? AND component_key=?",
            (session_id, key),
        ).fetchone()
        if hold is None or hold["hold_count"] != 1:
            raise RuntimeError("Interrupt target lost its exact hold before settlement")
        if connection.execute(
            "DELETE FROM component_holds WHERE session_id=? AND component_key=? AND hold_count=1",
            (session_id, key),
        ).rowcount != 1:
            raise RuntimeError("Interrupt target hold changed before settlement")
        pauses = connection.execute(
            "SELECT COUNT(*) FROM interrupt_pause_requests WHERE child_session_id=? "
            "AND state IN ('requested','acknowledged')", (session_id,),
        ).fetchone()[0]
        changed_pauses = connection.execute(
            "UPDATE interrupt_pause_requests SET state='released', released_at=? "
            "WHERE child_session_id=? AND state IN ('requested','acknowledged')",
            (settled_at, session_id),
        ).rowcount
        if changed_pauses != pauses:
            raise RuntimeError("Interrupt predecessor pauses changed before target settlement")
        if outcome.lifecycle != "failed":
            if admission["readiness_overridden"] or outcome.lifecycle == "sampled":
                state = connection.execute(
                    "SELECT retained_result_shape_id, retained_result_alignment_generation "
                    "FROM component_states WHERE component_key=?", (key,),
                ).fetchone()
                if (state is None or state["retained_result_shape_id"] is None
                        or state["retained_result_alignment_generation"] is None):
                    raise RuntimeError("Interrupt target has no exact successful result for its fence")
                if connection.execute(
                    "INSERT INTO post_interrupt_fences("
                    "component_key, shape_id, alignment_generation, source_session_id, created_at) "
                    "VALUES(?, ?, ?, ?, ?)",
                    (key, state["retained_result_shape_id"],
                     state["retained_result_alignment_generation"], session_id, settled_at),
                ).rowcount != 1:
                    raise RuntimeError("Post-interrupt fence was not recorded")
        if connection.execute(
            "UPDATE interrupt_admissions SET state='settled', settled_at=? "
            "WHERE session_id=? AND state='frozen'", (settled_at, session_id),
        ).rowcount != 1:
            raise RuntimeError("Interrupt admission changed before target settlement")
        self._read_interrupt_admission(connection, session_id)
        return True

    def _finish_interrupt_session_scope(self, connection, session_id, *, finished_at):
        admission = connection.execute(
            "SELECT * FROM interrupt_admissions WHERE session_id=?", (session_id,),
        ).fetchone()
        if admission is None:
            return {"returned": 0, "released": None}
        if admission["state"] == "frozen":
            hold = connection.execute(
                "DELETE FROM component_holds WHERE session_id=? AND component_key=? AND hold_count=1",
                (session_id, admission["target_component_key"]),
            ).rowcount
            if hold != 1:
                raise RuntimeError("Interrupt exit lost its exact target hold")
        if admission["state"] in ("admitted", "frozen"):
            pauses = connection.execute(
                "SELECT COUNT(*) FROM interrupt_pause_requests WHERE child_session_id=? "
                "AND state IN ('requested','acknowledged')", (session_id,),
            ).fetchone()[0]
            changed_pauses = connection.execute(
                "UPDATE interrupt_pause_requests SET state='released', released_at=? "
                "WHERE child_session_id=? AND state IN ('requested','acknowledged')",
                (finished_at, session_id),
            ).rowcount
            if changed_pauses != pauses:
                raise RuntimeError("Interrupt pauses changed before session exit")
            if connection.execute(
                "UPDATE interrupt_admissions SET state='settled', settled_at=? "
                "WHERE session_id=? AND state=?",
                (finished_at, session_id, admission["state"]),
            ).rowcount != 1:
                raise RuntimeError("Interrupt admission changed before session exit")
        returned = released = 0
        transfers = connection.execute(
            "SELECT * FROM interrupt_scope_transfers "
            "WHERE child_session_id=? AND state='active' ORDER BY component_key", (session_id,),
        ).fetchall()
        for transfer in transfers:
            source = _session_selected(
                connection, transfer["source_session_id"], transfer["component_key"],
            )
            source_session = connection.execute(
                "SELECT * FROM execution_sessions WHERE session_id=?",
                (transfer["source_session_id"],),
            ).fetchone()
            can_return = (
                source is not None and source["status"] == "running"
                and source["component_key"] is not None
                and source_session is not None
                and execution_session_liveness(dict(source_session))["live"]
            )
            if can_return:
                changed = connection.execute(
                    "UPDATE component_reservations SET session_id=? "
                    "WHERE component_key=? AND session_id=?",
                    (transfer["source_session_id"], transfer["component_key"], session_id),
                ).rowcount
                state = "returned"
                returned += 1
            else:
                changed = connection.execute(
                    "DELETE FROM component_reservations WHERE component_key=? AND session_id=?",
                    (transfer["component_key"], session_id),
                ).rowcount
                state = "released"
                released += 1
            if changed != 1 or connection.execute(
                "UPDATE interrupt_scope_transfers SET state=?, finished_at=? "
                "WHERE child_session_id=? AND component_key=? AND state='active'",
                (state, finished_at, session_id, transfer["component_key"]),
            ).rowcount != 1:
                raise RuntimeError("Interrupt scope transfer changed before session exit")
        return {"returned": returned, "released": released}

    def _require_terminal_interrupt_scope(self, connection, session_id):
        require_terminal_interrupt_scope(connection, session_id)
