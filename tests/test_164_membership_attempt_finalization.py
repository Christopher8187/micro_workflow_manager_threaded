"""Membership repair attempts finish only after the regional switch commits."""

from __future__ import annotations

import json

import pytest

from micro_workflow_manager import cli
from micro_workflow_manager.cli.project import load_workflow
from micro_workflow_manager.storage import FileStorage
from micro_workflow_manager.storage import component_membership
from micro_workflow_manager.storage import membership_preparation
from micro_workflow_manager.storage import native_recovery
from micro_workflow_manager.storage import preparation_attempts
from micro_workflow_manager.storage.preparation_receipts import (
    submit_preparation_decision,
)
from tests.test_064_read_only_previews import _snapshot
from tests.test_090_component_session_settlement import _close
from tests.test_137_between_run_membership import (
    _establish_old_membership,
    _owner_rows,
)
from tests.test_139_membership_commit_boundaries import _active_component


def _attempts(storage):
    return {
        row["operation_id"]: dict(row)
        for row in storage.db_connection().execute(
            "SELECT * FROM preparation_attempts ORDER BY operation_id"
        )
    }


def _preparation_metadata(storage):
    connection = storage.db_connection()
    return {
        "attempts": {
            row["operation_id"]: dict(row)
            for row in connection.execute(
                "SELECT * FROM preparation_attempts ORDER BY operation_id"
            )
        },
        "receipts": {
            row["operation_id"]: dict(row)
            for row in connection.execute(
                "SELECT * FROM preparation_receipts ORDER BY operation_id"
            )
        },
        "guards": {
            row["receiver_node"]: dict(row)
            for row in connection.execute(
                "SELECT * FROM receiver_mutation_guards ORDER BY receiver_node"
            )
        },
    }


def _assert_prior_preparation_metadata(storage, before):
    current = _preparation_metadata(storage)
    for section in before:
        for identity, expected in before[section].items():
            assert current[section][identity] == expected
    return current


def _new_attempt(storage, before):
    rows = _attempts(storage)
    created = set(rows) - set(before)
    assert len(created) == 1
    return rows[created.pop()]


def _active_partition(storage):
    active = component_membership.read_active_component_partition(
        storage.db_connection()
    )
    assert active is not None
    return active


def _immutable_rows(storage):
    return {
        table: tuple(
            tuple(row)
            for row in storage.db_connection().execute(
                f"SELECT * FROM {table} ORDER BY rowid"
            )
        )
        for table in (
            "graph_shapes",
            "component_definitions",
            "component_successful_results",
            "job_execution_owners",
            "input_publications",
        )
    }


def _attempt_receipts(storage, operation_id):
    return tuple(
        dict(row)
        for row in storage.db_connection().execute(
            "SELECT * FROM preparation_receipts WHERE guard_id=? "
            "ORDER BY operation,component_key,operation_id",
            (operation_id,),
        )
    )


def _attempt_guards(storage, operation_id):
    return tuple(
        dict(row)
        for row in storage.db_connection().execute(
            "SELECT * FROM receiver_mutation_guards WHERE operation_id=? "
            "ORDER BY receiver_node",
            (operation_id,),
        )
    )


def _assert_attempt_result(
    storage,
    attempt,
    before_active,
    *,
    state,
    completed,
    guards,
):
    assert attempt["state"] == state
    assert type(attempt["membership_revision"]) is int
    assert attempt["membership_revision"] == before_active.revision + 1
    assert attempt["completed_membership_revision"] == (
        attempt["membership_revision"] if completed else None
    )
    assert (attempt["finished_at"] is None) == (state == "interrupted")
    if state != "interrupted":
        assert isinstance(attempt["finished_at"], str) and attempt["finished_at"]
    assert attempt["session_id"] is None or isinstance(attempt["session_id"], str)
    receipts = _attempt_receipts(storage, attempt["operation_id"])
    assert len(receipts) == 2
    assert {row["state"] for row in receipts} == {"committed"}
    receipt_membership = [
        json.loads(row["manifest_json"])["membership"] for row in receipts
    ]
    assert {
        item["source_partition"]["revision"] for item in receipt_membership
    } == {before_active.revision}
    assert {
        tuple(tuple(component) for component in item["preparation_components"])
        for item in receipt_membership
    } == {(('A',), ('B',))}
    actual_guards = _attempt_guards(storage, attempt["operation_id"])
    if guards:
        assert actual_guards
        assert {row["operation_id"] for row in actual_guards} == {
            attempt["operation_id"]
        }
    else:
        assert actual_guards == ()
    return receipts


def _assert_prepared_membership_effects(
    storage, root, retained, *, active="old", allow_new_session=False
):
    if active == "old":
        assert _active_component(storage, "A") == ("A", "B")
        assert _active_component(storage, "B") == ("A", "B")
    else:
        assert active == "split"
        assert _active_component(storage, "A") == ("A",)
        assert _active_component(storage, "B") == ("B",)
    for node in ("A", "B"):
        assert storage.read_node_input_owner("R", f"{node}/owned.txt") is None
        assert not (storage.node_input_dir("R") / node / "owned.txt").exists()
        job_id = retained["jobs"][node]["job_id"]
        assert not storage.job_exists("R", job_id)
        assert not storage.job_base_dir("R", job_id).exists()
    assert _snapshot(storage.node_output_dir("R")) == retained["receiver_output"]
    assert storage.get_component_state(("R",))["misaligned"] is True
    assert storage.get_component_state(("U",)) == retained["unrelated_state"]
    assert storage.read_job_current_owner("U", 1) == retained["unrelated_owner"]
    assert _snapshot(root / "node" / "U") == retained["unrelated_tree"]
    assert _owner_rows(storage) == retained["owners"]
    sessions = storage.list_execution_sessions()
    if allow_new_session:
        by_id = {row["session_id"]: row for row in sessions}
        for expected in retained["sessions"]:
            assert by_id[expected["session_id"]] == expected
    else:
        assert sessions == retained["sessions"]
    assert (root / "component-executions.txt").read_bytes() == retained[
        "execution_log"
    ]


def _baseline(tmp_path, monkeypatch, capsys):
    retained = _establish_old_membership(
        tmp_path, monkeypatch, capsys, "split"
    )
    _register_target_shape(tmp_path)
    storage = FileStorage(tmp_path)
    try:
        return {
            "retained": retained,
            "attempts": _attempts(storage),
            "preparation": _preparation_metadata(storage),
            "active": _active_partition(storage),
            "immutable": _immutable_rows(storage),
        }
    finally:
        _close(storage)


def _register_target_shape(root):
    workflow = load_workflow(root)
    try:
        workflow.storage.register_component_topology(
            workflow.topology.snapshot()
        )
        workflow.storage.db_mutation_barrier()
    finally:
        _close(workflow.storage)


def _assert_failed_completion(storage, root, baseline):
    failed = _new_attempt(storage, baseline["attempts"])
    assert failed["state"] == "aborted"
    _assert_attempt_result(
        storage,
        failed,
        baseline["active"],
        state="aborted",
        completed=False,
        guards=False,
    )
    _assert_prior_preparation_metadata(storage, baseline["preparation"])
    assert _active_partition(storage) == baseline["active"]
    assert _immutable_rows(storage) == baseline["immutable"]
    _assert_prepared_membership_effects(
        storage, root, baseline["retained"]
    )
    return failed


def _retry_membership_repair(tmp_path, capsys, baseline, failed):
    assert cli.main(["reset", "A", "--yes"]) == 0
    capsys.readouterr()
    storage = FileStorage(tmp_path)
    try:
        attempts = _attempts(storage)
        assert attempts[failed["operation_id"]] == failed
        created = set(attempts) - set(baseline["attempts"]) - {
            failed["operation_id"]
        }
        assert len(created) == 1
        completed = attempts[created.pop()]
        _assert_attempt_result(
            storage,
            completed,
            baseline["active"],
            state="committed",
            completed=True,
            guards=False,
        )
        _assert_prior_preparation_metadata(storage, baseline["preparation"])
        assert _active_component(storage, "A") == ("A",)
        assert _active_component(storage, "B") == ("B",)
        assert _immutable_rows(storage) == baseline["immutable"]
        assert _owner_rows(storage) == baseline["retained"]["owners"]
    finally:
        _close(storage)


def _install_completion_writer_fault(monkeypatch, mode):
    assert mode in {"ignore", "revert"}
    original = membership_preparation.MembershipPreparation.finish
    reached = False

    def finish_with_fault(self, guard_id):
        nonlocal reached
        reached = True
        if mode == "ignore":
            trigger = "suppress_membership_completion"
            statement = f"""
                CREATE TRIGGER {trigger}
                BEFORE UPDATE ON preparation_attempts
                WHEN OLD.operation_id='{guard_id}'
                 AND OLD.state='preparing' AND NEW.state='preparing'
                BEGIN SELECT RAISE(IGNORE); END
            """
        else:
            trigger = "revert_membership_completion"
            statement = f"""
                CREATE TRIGGER {trigger}
                AFTER UPDATE ON preparation_attempts
                WHEN OLD.operation_id='{guard_id}'
                 AND OLD.state='preparing' AND NEW.state='preparing'
                BEGIN
                    UPDATE preparation_attempts SET completed_membership_revision=NULL
                    WHERE operation_id=NEW.operation_id;
                END
            """

        def install(connection):
            connection.execute(statement)

        def remove(connection):
            connection.execute(f"DROP TRIGGER IF EXISTS {trigger}")

        submit_preparation_decision(self.storage, install)
        try:
            return original(self, guard_id)
        finally:
            submit_preparation_decision(self.storage, remove)

    monkeypatch.setattr(
        membership_preparation.MembershipPreparation,
        "finish",
        finish_with_fault,
    )
    return original, lambda: reached


def _interrupt_attempt_release_after_membership_commit(
    tmp_path, monkeypatch, capsys,
):
    baseline = _baseline(tmp_path, monkeypatch, capsys)
    original = preparation_attempts._finish
    reached = False

    def fail_release(storage, expected, guards, requested_outcome):
        nonlocal reached
        if (
            requested_outcome == "committed"
            and expected["membership_revision"] is not None
        ):
            reached = True
            raise OSError("membership attempt release failed")
        return original(storage, expected, guards, requested_outcome)

    monkeypatch.setattr(preparation_attempts, "_finish", fail_release)
    assert cli.main(["reset", "A", "--yes"]) == 1
    captured = capsys.readouterr()
    assert "membership attempt release failed" in captured.err
    assert reached
    monkeypatch.setattr(preparation_attempts, "_finish", original)

    storage = FileStorage(tmp_path)
    try:
        interrupted = _new_attempt(storage, baseline["attempts"])
        _assert_attempt_result(
            storage,
            interrupted,
            baseline["active"],
            state="interrupted",
            completed=True,
            guards=True,
        )
        _assert_prior_preparation_metadata(storage, baseline["preparation"])
        assert _active_component(storage, "A") == ("A",)
        assert _active_component(storage, "B") == ("B",)
        assert _active_partition(storage).revision == interrupted[
            "completed_membership_revision"
        ]
        assert _immutable_rows(storage) == baseline["immutable"]
        _assert_prepared_membership_effects(
            storage,
            tmp_path,
            baseline["retained"],
            active="split",
        )
        interrupted_before_recovery = dict(interrupted)
        receipts_before_recovery = _attempt_receipts(
            storage, interrupted["operation_id"]
        )
        guards_before_recovery = _attempt_guards(
            storage, interrupted["operation_id"]
        )
    finally:
        _close(storage)
    return (
        baseline,
        interrupted_before_recovery,
        receipts_before_recovery,
        guards_before_recovery,
    )


def test_failed_final_membership_switch_aborts_attempt_and_remains_retryable(
    tmp_path, monkeypatch, capsys,
):
    retained = _establish_old_membership(
        tmp_path, monkeypatch, capsys, "split"
    )
    _register_target_shape(tmp_path)
    storage = FileStorage(tmp_path)
    before_attempts = _attempts(storage)
    before_preparation = _preparation_metadata(storage)
    before_active = _active_partition(storage)
    before_immutable = _immutable_rows(storage)
    _close(storage)

    original = component_membership._replace_active_component_closure
    reached = False

    def fail_final_switch(connection, change):
        nonlocal reached
        reached = True
        raise OSError("membership completion stopped before regional switch")

    monkeypatch.setattr(
        component_membership,
        "_replace_active_component_closure",
        fail_final_switch,
    )
    assert cli.main(["reset", "A", "--yes"]) == 1
    captured = capsys.readouterr()
    assert "membership completion stopped before regional switch" in captured.err
    assert reached

    storage = FileStorage(tmp_path)
    try:
        failed = _new_attempt(storage, before_attempts)
        # Keep this behavior assertion before the future schema fields. It is
        # the RED boundary on the source preceding membership completion.
        assert failed["state"] == "aborted"
        _assert_attempt_result(
            storage,
            failed,
            before_active,
            state="aborted",
            completed=False,
            guards=False,
        )
        assert failed["session_id"] is None
        _assert_prior_preparation_metadata(storage, before_preparation)
        assert _active_partition(storage) == before_active
        assert _immutable_rows(storage) == before_immutable
        _assert_prepared_membership_effects(storage, tmp_path, retained)
        failed_before_retry = dict(failed)
    finally:
        _close(storage)

    monkeypatch.setattr(
        component_membership,
        "_replace_active_component_closure",
        original,
    )
    assert cli.main(["reset", "A", "--yes"]) == 0
    capsys.readouterr()

    storage = FileStorage(tmp_path)
    try:
        attempts = _attempts(storage)
        assert attempts[failed_before_retry["operation_id"]] == failed_before_retry
        created = set(attempts) - set(before_attempts) - {
            failed_before_retry["operation_id"]
        }
        assert len(created) == 1
        completed = attempts[created.pop()]
        _assert_attempt_result(
            storage,
            completed,
            before_active,
            state="committed",
            completed=True,
            guards=False,
        )
        _assert_prior_preparation_metadata(storage, before_preparation)
        assert _active_component(storage, "A") == ("A",)
        assert _active_component(storage, "B") == ("B",)
        assert _immutable_rows(storage) == before_immutable
        assert _owner_rows(storage) == retained["owners"]
    finally:
        _close(storage)

    assert cli.main(["run", "A", "--plan"]) == 0
    assert "membership repair preparation:" not in capsys.readouterr().out


@pytest.mark.parametrize("fault", ["ignore", "revert"])
def test_membership_completion_update_must_persist_in_final_writer(
    tmp_path, monkeypatch, capsys, fault,
):
    baseline = _baseline(tmp_path, monkeypatch, capsys)
    original, reached = _install_completion_writer_fault(monkeypatch, fault)

    assert cli.main(["reset", "A", "--yes"]) == 1
    captured = capsys.readouterr()
    assert "preparation" in captured.err.lower() or "membership" in captured.err.lower()
    assert reached()

    storage = FileStorage(tmp_path)
    try:
        failed = _assert_failed_completion(storage, tmp_path, baseline)
    finally:
        _close(storage)

    monkeypatch.setattr(
        membership_preparation.MembershipPreparation,
        "finish",
        original,
    )
    _retry_membership_repair(tmp_path, capsys, baseline, failed)


def test_postcommit_notification_failure_keeps_membership_attempt_committed(
    tmp_path, monkeypatch, capsys,
):
    baseline = _baseline(tmp_path, monkeypatch, capsys)
    original = membership_preparation.MembershipPreparation.finish
    reached = False

    def fail_notification_after_finish(self, guard_id):
        nonlocal reached
        notify = self.storage.notify_state_change

        def fail_notification():
            nonlocal reached
            reached = True
            raise OSError("membership completion notification failed")

        self.storage.notify_state_change = fail_notification
        try:
            return original(self, guard_id)
        finally:
            self.storage.notify_state_change = notify

    monkeypatch.setattr(
        membership_preparation.MembershipPreparation,
        "finish",
        fail_notification_after_finish,
    )
    assert cli.main(["run", "A", "--runner", "direct"]) == 1
    captured = capsys.readouterr()
    assert "membership completion notification failed" in captured.err
    assert reached

    storage = FileStorage(tmp_path)
    try:
        completed = _new_attempt(storage, baseline["attempts"])
        _assert_attempt_result(
            storage,
            completed,
            baseline["active"],
            state="committed",
            completed=True,
            guards=False,
        )
        assert completed["session_id"] is not None
        session = storage.db_connection().execute(
            "SELECT * FROM execution_sessions WHERE session_id=?",
            (completed["session_id"],),
        ).fetchone()
        assert session is not None
        assert session["status"] == "terminal"
        assert session["outcome"] == "failed"
        assert session["partition_revision"] == completed["membership_revision"]
        assert _active_partition(storage).revision == completed["membership_revision"]
        _assert_prior_preparation_metadata(storage, baseline["preparation"])
        assert _immutable_rows(storage) == baseline["immutable"]
        _assert_prepared_membership_effects(
            storage,
            tmp_path,
            baseline["retained"],
            active="split",
            allow_new_session=True,
        )
    finally:
        _close(storage)


def test_admitted_session_revision_reversion_rolls_back_membership_completion(
    tmp_path, monkeypatch, capsys,
):
    baseline = _baseline(tmp_path, monkeypatch, capsys)
    original = membership_preparation.MembershipPreparation.finish
    reached = False

    def finish_with_reverted_session(self, guard_id):
        nonlocal reached
        reached = True
        trigger = "revert_admitted_membership_revision"
        statement = f"""
            CREATE TRIGGER {trigger}
            AFTER UPDATE OF partition_revision ON execution_sessions
            WHEN NEW.session_id='{self.session_id}'
             AND OLD.status='running' AND NEW.status='running'
             AND OLD.partition_revision!=NEW.partition_revision
            BEGIN
                UPDATE execution_sessions
                SET partition_revision=OLD.partition_revision
                WHERE session_id=NEW.session_id;
            END
        """
        submit_preparation_decision(
            self.storage, lambda connection: connection.execute(statement)
        )
        try:
            return original(self, guard_id)
        finally:
            submit_preparation_decision(
                self.storage,
                lambda connection: connection.execute(
                    f"DROP TRIGGER IF EXISTS {trigger}"
                ),
            )

    monkeypatch.setattr(
        membership_preparation.MembershipPreparation,
        "finish",
        finish_with_reverted_session,
    )
    assert cli.main(["run", "A", "--runner", "direct"]) == 1
    captured = capsys.readouterr()
    assert "session" in captured.err.lower() or "membership" in captured.err.lower()
    assert reached

    storage = FileStorage(tmp_path)
    try:
        failed = _new_attempt(storage, baseline["attempts"])
        assert failed["state"] == "aborted"
        _assert_attempt_result(
            storage,
            failed,
            baseline["active"],
            state="aborted",
            completed=False,
            guards=False,
        )
        assert failed["session_id"] is not None
        session = storage.db_connection().execute(
            "SELECT * FROM execution_sessions WHERE session_id=?",
            (failed["session_id"],),
        ).fetchone()
        assert session is not None
        assert session["status"] == "terminal"
        assert session["outcome"] == "failed"
        assert session["partition_revision"] == baseline["active"].revision
        _assert_prior_preparation_metadata(storage, baseline["preparation"])
        assert _active_partition(storage) == baseline["active"]
        assert _immutable_rows(storage) == baseline["immutable"]
        _assert_prepared_membership_effects(
            storage,
            tmp_path,
            baseline["retained"],
            allow_new_session=True,
        )
    finally:
        _close(storage)


def test_interrupted_attempt_release_recovers_completed_membership(
    tmp_path, monkeypatch, capsys,
):
    (
        baseline,
        interrupted,
        receipts_before,
        guards_before,
    ) = _interrupt_attempt_release_after_membership_commit(
        tmp_path, monkeypatch, capsys
    )

    assert cli.main(["recover"]) == 0
    captured = capsys.readouterr()
    assert f"Recovered file operation {interrupted['operation_id']}: committed" in captured.out

    storage = FileStorage(tmp_path)
    try:
        recovered = _attempts(storage)[interrupted["operation_id"]]
        _assert_attempt_result(
            storage,
            recovered,
            baseline["active"],
            state="committed",
            completed=True,
            guards=False,
        )
        assert recovered == dict(
            interrupted,
            state="committed",
            finished_at=recovered["finished_at"],
        )
        assert _attempt_receipts(storage, interrupted["operation_id"]) == (
            receipts_before
        )
        assert guards_before
        _assert_prior_preparation_metadata(storage, baseline["preparation"])
        assert _active_component(storage, "A") == ("A",)
        assert _active_component(storage, "B") == ("B",)
        assert _immutable_rows(storage) == baseline["immutable"]
        _assert_prepared_membership_effects(
            storage,
            tmp_path,
            baseline["retained"],
            active="split",
        )
    finally:
        _close(storage)

    assert cli.main(["recover"]) == 0
    repeat = capsys.readouterr()
    assert "No stale native sessions needed recovery." in repeat.out


def test_cleanup_attempt_terminal_update_reversion_rolls_back_guard_release(
    tmp_path, monkeypatch, capsys,
):
    (
        baseline,
        interrupted,
        receipts_before,
        guards_before,
    ) = _interrupt_attempt_release_after_membership_commit(
        tmp_path, monkeypatch, capsys
    )
    original = native_recovery._finish_cleanup_receipt
    reached = False

    def finish_with_reverted_attempt(storage, plan, state):
        nonlocal reached
        if plan.kind != "attempt" or plan.operation_id != interrupted["operation_id"]:
            return original(storage, plan, state)
        reached = True
        trigger = "revert_cleanup_attempt_terminal_update"
        statement = f"""
            CREATE TRIGGER {trigger}
            AFTER UPDATE ON preparation_attempts
            WHEN OLD.operation_id='{interrupted['operation_id']}'
             AND OLD.state='interrupted' AND NEW.state='committed'
            BEGIN
                UPDATE preparation_attempts
                SET state='interrupted', finished_at=NULL
                WHERE operation_id=NEW.operation_id;
            END
        """
        submit_preparation_decision(
            storage, lambda connection: connection.execute(statement)
        )
        try:
            return original(storage, plan, state)
        finally:
            submit_preparation_decision(
                storage,
                lambda connection: connection.execute(
                    f"DROP TRIGGER IF EXISTS {trigger}"
                ),
            )

    monkeypatch.setattr(
        native_recovery,
        "_finish_cleanup_receipt",
        finish_with_reverted_attempt,
    )
    assert cli.main(["recover"]) == 1
    captured = capsys.readouterr()
    assert "Recovery refused:" in captured.err
    assert interrupted["operation_id"] in captured.err
    assert reached

    storage = FileStorage(tmp_path)
    try:
        assert _attempts(storage)[interrupted["operation_id"]] == interrupted
        assert _attempt_receipts(storage, interrupted["operation_id"]) == (
            receipts_before
        )
        assert _attempt_guards(storage, interrupted["operation_id"]) == (
            guards_before
        )
        _assert_prior_preparation_metadata(storage, baseline["preparation"])
        assert _active_component(storage, "A") == ("A",)
        assert _active_component(storage, "B") == ("B",)
        assert _immutable_rows(storage) == baseline["immutable"]
        _assert_prepared_membership_effects(
            storage,
            tmp_path,
            baseline["retained"],
            active="split",
        )
    finally:
        _close(storage)

    monkeypatch.setattr(
        native_recovery,
        "_finish_cleanup_receipt",
        original,
    )
    assert cli.main(["recover"]) == 0
    capsys.readouterr()
    storage = FileStorage(tmp_path)
    try:
        recovered = _attempts(storage)[interrupted["operation_id"]]
        _assert_attempt_result(
            storage,
            recovered,
            baseline["active"],
            state="committed",
            completed=True,
            guards=False,
        )
    finally:
        _close(storage)
