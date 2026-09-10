"""Observe and recover abandoned native clipboard file decisions."""

from __future__ import annotations

from contextlib import ExitStack
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import os
import re
import sqlite3

from .clipboard_files import ClipboardFiles
from .clipboard_operation import (
    _attempt, _guards, _require_exact_attempt, _require_terminal_decision,
    _validate_terminal_decision,
)
from .clipboard_snapshot import canonical, digest, observe_clipboard_node
from .file_operation_receipts import (
    finish_file_receipt,
    insert_file_receipt,
    prepared_receipt,
    read_file_receipt,
    require_prepared_file_receipt,
)
from .operation_lock import OperationStillActive, operation_lock, recovery_advisory_lock
from .preparation_receipts import submit_preparation_decision
from .recovery_errors import add_recovery_note
from .recovery_output import recovery_path


@dataclass(frozen=True, slots=True)
class ClipboardCleanupPlan:
    attempt: dict
    receipt: dict | None
    component: tuple[str, ...]
    guards: tuple[tuple, ...]

    @property
    def operation_id(self):
        return self.attempt["operation_id"]

    @property
    def files(self):
        raw = self.attempt["manifest_json"]
        return None if raw is None else json.loads(raw)


@dataclass(frozen=True, slots=True)
class ClipboardCleanupObservation:
    plans: tuple[ClipboardCleanupPlan, ...]
    errors: tuple[str, ...]
    retained_material: tuple[str, ...]


def _validate_attempt(row):
    operation_id = row["operation_id"]
    if not isinstance(operation_id, str) or not re.fullmatch(r"[0-9a-f]{32}", operation_id):
        raise RuntimeError("Invalid clipboard operation identity")
    try:
        observation = json.loads(row["observation_json"])
    except (TypeError, ValueError) as error:
        raise RuntimeError("Invalid clipboard state observation") from error
    if (row["operation"] not in ("copy", "paste") or row["state"] not in (
        "allocating", "prepared", "committed", "aborted",
    ) or not isinstance(observation, dict)
            or row["observation_digest"] != digest(observation)
            or observation.get("project_id") != row["project_id"]
            or observation.get("node") != row["node_name"]):
        raise RuntimeError("Clipboard attempt immutable observation changed")
    component = observation.get("component")
    if (not isinstance(component, list) or not component
            or any(not isinstance(node, str) or not node for node in component)
            or component != sorted(set(component)) or row["node_name"] not in component):
        raise RuntimeError("Invalid clipboard attempt component")
    if row["manifest_json"] is not None:
        manifest = json.loads(row["manifest_json"])
        if not isinstance(manifest, dict):
            raise RuntimeError("Invalid clipboard file manifest")
        node = row["node_name"]
        source = "node/" + node if row["operation"] == "copy" else "clipboard/" + node
        destination = "clipboard/" + node if row["operation"] == "copy" else "node/" + node
        staging = manifest.get("staging")
        source_item = manifest.get("source")
        destination_item = manifest.get("destination")
        if (not isinstance(staging, dict) or not isinstance(source_item, dict)
                or not isinstance(destination_item, dict)
                or staging.get("path")
                != ".mwf/clipboard-operations/" + operation_id
                or source_item.get("path") != source
                or destination_item.get("path") != destination):
            raise RuntimeError("Clipboard file paths differ from the recorded operation")
    return tuple(component)


def observe_clipboard_cleanup(connection, root):
    plans, errors, recorded = [], [], set()
    for row in connection.execute(
        "SELECT * FROM clipboard_attempts ORDER BY operation_id"
    ).fetchall():
        operation_id = row["operation_id"]
        recorded.add(operation_id)
        try:
            attempt = dict(row)
            component = _validate_attempt(attempt)
            guards = _guards(connection, operation_id)
            expected_guards = tuple(
                (node, operation_id, None, attempt["owner_pid"],
                 attempt["process_identity"], attempt["hostname"])
                for node in component
            )
            if (attempt["state"] in ("allocating", "prepared")
                    and guards != expected_guards) or (
                attempt["state"] in ("committed", "aborted") and guards
            ):
                raise RuntimeError("Clipboard attempt receiver guards changed")
            receipt = read_file_receipt(connection, "clipboard_receipts", operation_id)
            if attempt["state"] == "allocating":
                if receipt is not None:
                    raise RuntimeError("Allocating clipboard attempt already has a file receipt")
            elif receipt is None or receipt["state"] != attempt["state"]:
                raise RuntimeError("Clipboard attempt and file decision disagree")
            path = recovery_path(root, ".mwf/clipboard-operations/" + operation_id)
            if attempt["manifest_json"] is None:
                raise RuntimeError("Clipboard attempt lost its file manifest")
            ClipboardFiles(root, json.loads(attempt["manifest_json"]))
            if attempt["state"] in ("committed", "aborted"):
                _validate_terminal_decision(attempt, receipt)
                if not os.path.lexists(path):
                    continue
            plans.append(ClipboardCleanupPlan(attempt, receipt, component, guards))
        except (RuntimeError, ValueError, TypeError, KeyError, OSError, sqlite3.Error) as error:
            errors.append(str(operation_id) + ": " + str(error))
    retained = []
    parent = recovery_path(root, ".mwf/clipboard-operations")
    if parent.exists():
        for path in parent.iterdir():
            if path.name.endswith(".lock") and path.name[:-5] in recorded:
                continue
            if path.name not in recorded:
                retained.append(str(path))
    return ClipboardCleanupObservation(tuple(plans), tuple(errors), tuple(retained))


def _require_plan_current(storage, plan):
    connection = storage._new_db_connection()
    try:
        _require_exact_attempt(connection, plan.attempt, plan.guards)
        if read_file_receipt(
            connection, "clipboard_receipts", plan.operation_id,
        ) != plan.receipt:
            raise RuntimeError("Clipboard cleanup receipt changed after observation")
        if plan.attempt["state"] in ("committed", "aborted"):
            _validate_terminal_decision(plan.attempt, plan.receipt)
        elif observe_clipboard_node(
            connection, plan.attempt["node_name"],
        ).digest != plan.attempt["observation_digest"]:
            raise RuntimeError("Clipboard state changed after cleanup observation")
    finally:
        connection.close()


def _terminal_receipt(receipt, state, details):
    decision = {
        "table": "clipboard_receipts", "operation_id": receipt["operation_id"],
        "state": state, "intent_digest": receipt["intent_digest"], "details": details,
    }
    return dict(
        receipt, state=state, decision_json=canonical(decision),
        decision_digest=digest(decision),
    )


def _require_terminal(storage, attempt, receipt, *, files=None, restored=False):
    connection = storage._new_db_connection()
    try:
        _require_exact_attempt(connection, attempt, ())
        if read_file_receipt(
            connection, "clipboard_receipts", attempt["operation_id"],
        ) != receipt:
            raise RuntimeError("Clipboard recovery terminal receipt changed")
        _require_terminal_decision(connection, attempt, receipt)
    finally:
        connection.close()
    if files is not None:
        files.require_restored() if restored else files.require_published()


def _submit_terminal(storage, operation, readback):
    try:
        submit_preparation_decision(storage, operation)
    except BaseException as error:
        try:
            readback()
        except BaseException as observation_error:
            add_recovery_note(
                error, "Clipboard recovery readback failed: " + str(observation_error),
            )
            raise error
    else:
        readback()


def _finish_allocating(storage, plan):
    expected = plan.attempt
    guards = plan.guards
    receipt = prepared_receipt(
        "clipboard_receipts", operation_id=plan.operation_id,
        operation=expected["operation"], node_name=expected["node_name"],
        manifest_json=expected["manifest_json"],
    )
    terminal_attempt = terminal_receipt = None

    def finish(connection):
        nonlocal terminal_attempt, terminal_receipt
        _require_exact_attempt(connection, expected, guards)
        if observe_clipboard_node(
            connection, expected["node_name"],
        ).digest != expected["observation_digest"]:
            raise RuntimeError("Clipboard allocation state changed before recovery")
        insert_file_receipt(connection, "clipboard_receipts", receipt)
        details = {
            "restored": True,
            "abort_from": "allocating",
            "observation_digest": expected["observation_digest"],
        }
        finish_file_receipt(
            connection, "clipboard_receipts", receipt, "aborted",
            details,
        )
        terminal_receipt = _terminal_receipt(receipt, "aborted", details)
        if connection.execute(
            "DELETE FROM receiver_mutation_guards WHERE operation_id=?", (plan.operation_id,)
        ).rowcount != len(guards):
            raise RuntimeError("Clipboard allocation guard count changed")
        finished = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
        if connection.execute(
            "UPDATE clipboard_attempts SET state='aborted', finished_at=? "
            "WHERE operation_id=? AND state='allocating' AND manifest_json=?",
            (finished, plan.operation_id, expected["manifest_json"]),
        ).rowcount != 1:
            raise RuntimeError("Clipboard allocation changed before abort")
        terminal_attempt = dict(expected, state="aborted", finished_at=finished)
        _require_exact_attempt(connection, terminal_attempt, ())
        if read_file_receipt(
            connection, "clipboard_receipts", plan.operation_id,
        ) != terminal_receipt:
            raise RuntimeError("Clipboard allocation terminal receipt changed")

    _submit_terminal(
        storage, finish,
        lambda: _require_terminal(storage, terminal_attempt, terminal_receipt),
    )


def _finish_prepared(storage, plan, files):
    expected = plan.attempt
    receipt = plan.receipt
    guards = plan.guards
    terminal_attempt = terminal_receipt = None

    def finish(connection):
        nonlocal terminal_attempt, terminal_receipt
        _require_exact_attempt(connection, expected, guards)
        require_prepared_file_receipt(connection, "clipboard_receipts", receipt)
        if observe_clipboard_node(
            connection, expected["node_name"],
        ).digest != expected["observation_digest"]:
            raise RuntimeError("Clipboard state changed before recovery")
        files.require_restored()
        details = {
            "restored": True,
            "abort_from": "prepared",
            "observation_digest": expected["observation_digest"],
        }
        finish_file_receipt(connection, "clipboard_receipts", receipt, "aborted", details)
        terminal_receipt = _terminal_receipt(receipt, "aborted", details)
        if connection.execute(
            "DELETE FROM receiver_mutation_guards WHERE operation_id=?", (plan.operation_id,)
        ).rowcount != len(guards):
            raise RuntimeError("Clipboard recovery guard count changed")
        finished = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
        if connection.execute(
            "UPDATE clipboard_attempts SET state='aborted', finished_at=? "
            "WHERE operation_id=? AND state='prepared'",
            (finished, plan.operation_id),
        ).rowcount != 1:
            raise RuntimeError("Clipboard attempt changed during recovery")
        terminal_attempt = dict(expected, state="aborted", finished_at=finished)
        _require_exact_attempt(connection, terminal_attempt, ())
        if read_file_receipt(
            connection, "clipboard_receipts", plan.operation_id,
        ) != terminal_receipt:
            raise RuntimeError("Clipboard recovery terminal receipt changed")

    _submit_terminal(
        storage, finish,
        lambda: _require_terminal(
            storage, terminal_attempt, terminal_receipt, files=files, restored=True,
        ),
    )


def recover_clipboard_cleanup(storage, observation):
    recovered, errors, live = [], list(observation.errors), []
    for plan in observation.plans:
        try:
            with ExitStack() as locks:
                try:
                    locks.enter_context(operation_lock(
                        storage, "clipboard-operation-locks", plan.operation_id,
                    ))
                except OperationStillActive:
                    live.append(plan.operation_id)
                    continue
                for node in plan.component:
                    locks.enter_context(recovery_advisory_lock(storage, f"node-{node}-input"))
                    locks.enter_context(recovery_advisory_lock(storage, f"node-{node}-jobs"))
                    locks.enter_context(recovery_advisory_lock(storage, f"node-{node}-debug"))
                _require_plan_current(storage, plan)
                files = ClipboardFiles(storage.project_dir, plan.files)
                if plan.attempt["state"] == "allocating":
                    if files.directory.exists():
                        files.require_restored()
                        files.discard(committed=False)
                    _finish_allocating(storage, plan)
                    recovered.append({"operation_id": plan.operation_id, "state": "aborted"})
                    continue
                if plan.attempt["state"] == "prepared":
                    files.restore()
                    _finish_prepared(storage, plan, files)
                    files.discard(committed=False)
                    recovered.append({"operation_id": plan.operation_id, "state": "aborted"})
                else:
                    files.discard(committed=plan.attempt["state"] == "committed")
                    recovered.append({"operation_id": plan.operation_id,
                                      "state": plan.attempt["state"]})
        except (RuntimeError, ValueError, TypeError, KeyError, OSError, sqlite3.Error) as error:
            errors.append(plan.operation_id + ": " + str(error))
    return {"cleanup": recovered, "errors": errors,
            "live_operations": tuple(sorted(set(live)))}
