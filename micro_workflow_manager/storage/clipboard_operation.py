"""Atomic same-project native node copy and paste."""

from __future__ import annotations

from contextlib import ExitStack
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import socket
from tempfile import TemporaryDirectory
from uuid import uuid4

from micro_workflow_manager.processes import process_identity
from .clipboard_files import prepare_clipboard_allocation
from .clipboard_snapshot import (
    SNAPSHOT_NAME,
    canonical,
    digest,
    observe_clipboard_node,
    open_native_snapshot,
    read_project_id,
    require_clipboard_payload,
    require_idle_clipboard_component,
    write_native_snapshot,
)
from .clipboard_state import replace_clipboard_node_state, validate_clipboard_support
from .file_operation_receipts import (
    finish_file_receipt,
    insert_file_receipt,
    prepared_receipt,
    read_file_receipt,
    require_prepared_file_receipt,
)
from .operation_lock import operation_lock
from .preparation_guards import refuse_receiver_mutation
from .preparation_receipts import submit_preparation_decision
from .recovery_errors import add_recovery_note
from .recovery_output import recovery_path


def _attempt(connection, operation_id):
    row = connection.execute("SELECT * FROM clipboard_attempts WHERE operation_id=?",
                             (operation_id,)).fetchone()
    return None if row is None else dict(row)


def _guards(connection, operation_id):
    return tuple(tuple(row) for row in connection.execute(
        "SELECT * FROM receiver_mutation_guards WHERE operation_id=? ORDER BY receiver_node",
        (operation_id,),
    ))


def _require_exact_attempt(connection, expected, guards):
    if _attempt(connection, expected["operation_id"]) != expected or _guards(
        connection, expected["operation_id"]
    ) != guards:
        raise RuntimeError("Clipboard attempt or receiver guards changed")


def _validate_terminal_decision(attempt, receipt):
    if any(receipt[name] != attempt[name] for name in
           ("operation_id", "operation", "node_name", "manifest_json")):
        raise RuntimeError("Clipboard terminal decision differs from its attempt")
    try:
        decision = json.loads(receipt["decision_json"])
        details = decision["details"]
    except (TypeError, ValueError, KeyError) as error:
        raise RuntimeError("Invalid clipboard terminal decision") from error
    observation_digest = details.get("observation_digest")
    if (not isinstance(observation_digest, str) or len(observation_digest) != 64
            or any(character not in "0123456789abcdef" for character in observation_digest)):
        raise RuntimeError("Invalid clipboard terminal observation identity")
    if receipt["state"] == "aborted":
        if (set(details) != {"restored", "abort_from", "observation_digest"}
                or type(details["restored"]) is not bool
                or details["abort_from"] not in ("allocating", "prepared")
                or (details["abort_from"] == "prepared" and not details["restored"])
                or observation_digest != attempt["observation_digest"]):
            raise RuntimeError("Invalid aborted clipboard decision details")
    elif (receipt["state"] != "committed"
            or set(details) != {"node", "alignment_generation", "observation_digest"}
            or details["node"] != attempt["node_name"]
            or (attempt["operation"] == "copy" and details["alignment_generation"] is not None)
            or (attempt["operation"] == "copy"
                and observation_digest != attempt["observation_digest"])
            or (attempt["operation"] == "paste"
                and (type(details["alignment_generation"]) is not int
                     or details["alignment_generation"] < 0))):
        raise RuntimeError("Invalid committed clipboard decision details")
    return details


def _require_terminal_decision(connection, attempt, receipt):
    details = _validate_terminal_decision(attempt, receipt)
    observed = observe_clipboard_node(connection, attempt["node_name"])
    if details["observation_digest"] != observed.digest:
        raise RuntimeError("Clipboard terminal state differs from its file decision")
    if (receipt["state"] == "committed" and attempt["operation"] == "paste"
            and details["alignment_generation"]
            != observed.data["component_state"]["alignment_generation"]):
        raise RuntimeError("Clipboard paste generation differs from its file decision")


def _observation_json(observation):
    return canonical({
        "project_id": observation.project_id,
        "node": observation.node,
        "component": observation.component,
        "data": observation.data,
    })


class ClipboardOperationStorageMixin:
    def _clipboard_attempt_values(self, operation_id, operation, observation, manifest):
        identity = process_identity(os.getpid())
        if not identity:
            raise RuntimeError("Clipboard operation cannot identify this process")
        timestamp = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
        observation_json = _observation_json(observation)
        expected = {
            "operation_id": operation_id, "operation": operation,
            "node_name": observation.node, "project_id": observation.project_id,
            "state": "allocating", "owner_pid": os.getpid(),
            "process_identity": identity, "hostname": socket.gethostname(),
            "started_at": timestamp, "finished_at": None,
            "observation_json": observation_json,
            "observation_digest": digest(json.loads(observation_json)),
            "manifest_json": canonical(manifest),
        }
        guards = tuple(
            (node, operation_id, None, os.getpid(), identity, socket.gethostname())
            for node in observation.component
        )
        return expected, guards

    def _begin_clipboard_attempt(self, expected, guards, observation):
        def begin(connection):
            current = observe_clipboard_node(connection, observation.node)
            require_idle_clipboard_component(
                connection, current, allowed_guard=expected["operation_id"],
            )
            if _observation_json(current) != _observation_json(observation):
                raise RuntimeError("Clipboard source or destination state changed before staging")
            for node in observation.component:
                refuse_receiver_mutation(connection, node)
            names = tuple(expected)
            if connection.execute(
                "INSERT INTO clipboard_attempts(" + ",".join(names) + ") VALUES("
                + ",".join("?" for _ in names) + ")", tuple(expected.values()),
            ).rowcount != 1:
                raise RuntimeError("Clipboard attempt was not recorded")
            connection.executemany(
                "INSERT INTO receiver_mutation_guards VALUES(?,?,?,?,?,?)", guards,
            )
            _require_exact_attempt(connection, expected, guards)

        submit_preparation_decision(self, begin)
        return expected, guards

    def _prepare_clipboard_receipt(self, expected, guards, files, observation):
        manifest_json = canonical(files.manifest)
        if expected["manifest_json"] != manifest_json:
            raise RuntimeError("Clipboard allocated files differ from their recorded intent")
        receipt = prepared_receipt(
            "clipboard_receipts", operation_id=expected["operation_id"],
            operation=expected["operation"], node_name=expected["node_name"],
            manifest_json=manifest_json,
        )

        def prepare(connection):
            _require_exact_attempt(connection, expected, guards)
            current = observe_clipboard_node(connection, observation.node)
            require_idle_clipboard_component(
                connection, current, allowed_guard=expected["operation_id"],
            )
            if _observation_json(current) != expected["observation_json"]:
                raise RuntimeError("Clipboard state changed during private capture")
            files.require_prepared()
            insert_file_receipt(connection, "clipboard_receipts", receipt)
            changed = connection.execute(
                "UPDATE clipboard_attempts SET state='prepared' "
                "WHERE operation_id=? AND state='allocating' AND manifest_json=?",
                (expected["operation_id"], manifest_json),
            ).rowcount
            if changed != 1:
                raise RuntimeError("Clipboard attempt did not retain its file manifest")
            _require_exact_attempt(connection, dict(expected, state="prepared"), guards)
        submit_preparation_decision(self, prepare)
        prepared = dict(expected, state="prepared")
        return prepared, receipt

    def _abort_clipboard(self, expected, guards, receipt, files):
        if expected is None:
            return False
        state = None
        connection = self._new_db_connection()
        try:
            observed = read_file_receipt(connection, "clipboard_receipts", expected["operation_id"])
            state = None if observed is None else observed["state"]
        finally:
            connection.close()
        if state == "committed":
            connection = self._new_db_connection()
            try:
                terminal = _attempt(connection, expected["operation_id"])
                if terminal is None or terminal["state"] != "committed" or not terminal["finished_at"]:
                    raise RuntimeError("Clipboard committed attempt is missing")
                normalized = dict(terminal, state=expected["state"], finished_at=expected["finished_at"])
                if normalized != expected:
                    raise RuntimeError("Clipboard committed attempt changed")
                _require_exact_attempt(connection, terminal, ())
                actual_receipt = read_file_receipt(
                    connection, "clipboard_receipts", expected["operation_id"],
                )
                if (receipt is None or actual_receipt is None
                        or dict(actual_receipt, state="prepared", decision_json=None,
                                decision_digest=None) != receipt
                        or actual_receipt["state"] != "committed"):
                    raise RuntimeError("Clipboard committed receipt is missing")
                _require_terminal_decision(connection, terminal, actual_receipt)
            finally:
                connection.close()
            if files is not None:
                files.require_published()
            return True
        if files is not None:
            files.restore()

        def abort(connection):
            current = _attempt(connection, expected["operation_id"])
            if current is None:
                return
            _require_exact_attempt(connection, expected, guards)
            actual_receipt = read_file_receipt(connection, "clipboard_receipts", expected["operation_id"])
            prepared = receipt
            if actual_receipt is None:
                prepared = prepared_receipt(
                    "clipboard_receipts", operation_id=expected["operation_id"],
                    operation=expected["operation"], node_name=expected["node_name"],
                    manifest_json=expected["manifest_json"],
                )
                insert_file_receipt(connection, "clipboard_receipts", prepared)
                actual_receipt = prepared
            if actual_receipt["state"] == "prepared":
                if expected["state"] == "prepared" and files is None:
                    raise RuntimeError("Prepared clipboard attempt has no restorable files")
                if files is not None:
                    files.require_restored()
                details = {
                    "restored": files is not None,
                    "abort_from": expected["state"],
                    "observation_digest": expected["observation_digest"],
                }
                finish_file_receipt(
                    connection, "clipboard_receipts", prepared, "aborted",
                    details,
                )
            else:
                raise RuntimeError("Clipboard decision changed during abort")
            finished = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
            if connection.execute(
                "DELETE FROM receiver_mutation_guards WHERE operation_id=?",
                (expected["operation_id"],),
            ).rowcount != len(guards):
                raise RuntimeError("Clipboard receiver guard count changed")
            if connection.execute(
                "UPDATE clipboard_attempts SET state='aborted', finished_at=? "
                "WHERE operation_id=? AND state IN ('allocating','prepared')",
                (finished, expected["operation_id"]),
            ).rowcount != 1:
                raise RuntimeError("Clipboard attempt changed during abort")
            terminal_attempt = dict(expected, state="aborted", finished_at=finished)
            decision = {
                "table": "clipboard_receipts", "operation_id": prepared["operation_id"],
                "state": "aborted", "intent_digest": prepared["intent_digest"],
                "details": details,
            }
            terminal_receipt = dict(
                prepared, state="aborted", decision_json=canonical(decision),
                decision_digest=digest(decision),
            )
            _require_exact_attempt(connection, terminal_attempt, ())
            if read_file_receipt(
                connection, "clipboard_receipts", expected["operation_id"],
            ) != terminal_receipt:
                raise RuntimeError("Clipboard abort terminal receipt changed")
            _require_terminal_decision(connection, terminal_attempt, terminal_receipt)
        submit_preparation_decision(self, abort)
        return False

    def _clipboard_locks(self, component):
        stack = ExitStack()
        for node in sorted(component):
            stack.enter_context(self.interprocess_lock(f"node-{node}-input"))
            stack.enter_context(self.interprocess_lock(f"node-{node}-jobs"))
            stack.enter_context(self.interprocess_lock(f"node-{node}-debug"))
        return stack

    def capture_node_clipboard(self, node, destination):
        node = self.validate_node_name(node)
        destination = Path(destination)
        destination_relative = destination.relative_to(self.project_dir).as_posix()
        recovery_path(self.project_dir, destination_relative)
        operation_id = uuid4().hex
        files = receipt = expected = guards = None
        observation = observe_clipboard_node(self.db_connection(), node)
        require_idle_clipboard_component(self.db_connection(), observation)
        require_clipboard_payload(self.project_dir / "node" / node, observation)
        destination.parent.mkdir(parents=True, exist_ok=True)
        with TemporaryDirectory(prefix="mwf-clipboard-") as temporary:
            snapshot_path = Path(temporary) / SNAPSHOT_NAME
            with operation_lock(self, "clipboard-operation-locks", operation_id), \
                    self._clipboard_locks(observation.component):
                current = observe_clipboard_node(self.db_connection(), node)
                require_idle_clipboard_component(self.db_connection(), current)
                require_clipboard_payload(self.project_dir / "node" / node, current)
                if _observation_json(current) != _observation_json(observation):
                    raise RuntimeError("Clipboard source changed while acquiring its locks")
                write_native_snapshot(self.db_connection(), snapshot_path)
                try:
                    allocation = prepare_clipboard_allocation(
                        self.project_dir, temporary, operation_id,
                        source_relative="node/" + node,
                        destination_relative=destination_relative,
                        include_snapshot=True, snapshot_source=snapshot_path,
                    )
                    expected, guards = self._clipboard_attempt_values(
                        operation_id, "copy", observation, allocation.manifest,
                    )
                    self._begin_clipboard_attempt(expected, guards, observation)
                    files = allocation.install()
                    expected, receipt = self._prepare_clipboard_receipt(
                        expected, guards, files, observation,
                    )
                    files.publish()
                    self._commit_clipboard(expected, guards, receipt, files, observation, None)
                except BaseException as error:
                    try:
                        committed = self._abort_clipboard(expected, guards, receipt, files)
                        if committed and files is not None:
                            files.discard(committed=True)
                        elif files is not None:
                            files.discard(committed=False)
                    except BaseException as cleanup_error:
                        add_recovery_note(error, "Clipboard copy requires recovery: " + str(cleanup_error))
                    raise
                files.discard(committed=True)
        return {"operation_id": operation_id, "node": node, "jobs": len(observation.data["jobs"])}

    def restore_node_clipboard(self, node, source):
        node = self.validate_node_name(node)
        source = Path(source)
        source_relative = source.relative_to(self.project_dir).as_posix()
        recovery_path(self.project_dir, source_relative)
        project_id = read_project_id(self.db_connection())
        snapshot, saved = open_native_snapshot(source / SNAPSHOT_NAME, node, project_id)
        operation_id = uuid4().hex
        files = receipt = expected = guards = None
        try:
            current = observe_clipboard_node(self.db_connection(), node)
            require_idle_clipboard_component(self.db_connection(), current)
            require_clipboard_payload(self.project_dir / "node" / node, current)
            require_clipboard_payload(source, saved)
            validate_clipboard_support(self.db_connection(), saved, current)
            with TemporaryDirectory(prefix="mwf-clipboard-") as temporary:
                with operation_lock(self, "clipboard-operation-locks", operation_id), \
                        self._clipboard_locks(saved.component):
                    locked = observe_clipboard_node(self.db_connection(), node)
                    require_idle_clipboard_component(self.db_connection(), locked)
                    require_clipboard_payload(self.project_dir / "node" / node, locked)
                    require_clipboard_payload(source, saved)
                    validate_clipboard_support(self.db_connection(), saved, locked)
                    if _observation_json(locked) != _observation_json(current):
                        raise RuntimeError("Clipboard destination changed while acquiring its locks")
                    current = locked
                    try:
                        allocation = prepare_clipboard_allocation(
                            self.project_dir, temporary, operation_id,
                            source_relative=source_relative,
                            destination_relative="node/" + node,
                            include_snapshot=False,
                        )
                        expected, guards = self._clipboard_attempt_values(
                            operation_id, "paste", current, allocation.manifest,
                        )
                        self._begin_clipboard_attempt(expected, guards, current)
                        files = allocation.install()
                        expected, receipt = self._prepare_clipboard_receipt(
                            expected, guards, files, current,
                        )
                        files.publish()
                        generation = self._commit_clipboard(
                            expected, guards, receipt, files, current, saved,
                        )
                    except BaseException as error:
                        try:
                            committed = self._abort_clipboard(expected, guards, receipt, files)
                            if committed and files is not None:
                                files.discard(committed=True)
                            elif files is not None:
                                files.discard(committed=False)
                        except BaseException as cleanup_error:
                            add_recovery_note(error, "Clipboard paste requires recovery: " + str(cleanup_error))
                        raise
                    files.discard(committed=True)
            return {"operation_id": operation_id, "node": node,
                    "jobs": len(saved.data["jobs"]), "alignment_generation": generation}
        finally:
            snapshot.close()

    def _commit_clipboard(self, expected, guards, receipt, files, observation, saved):
        result = None
        terminal_attempt = terminal_receipt = post_digest = None

        def commit(connection):
            nonlocal result, terminal_attempt, terminal_receipt, post_digest
            _require_exact_attempt(connection, expected, guards)
            require_prepared_file_receipt(
                connection, "clipboard_receipts", receipt,
            )
            files.require_published()
            current = observe_clipboard_node(connection, observation.node)
            if _observation_json(current) != expected["observation_json"]:
                raise RuntimeError("Clipboard state changed before its writer decision")
            result = None if saved is None else replace_clipboard_node_state(
                connection, saved, current, expected["operation_id"],
            )
            post = observe_clipboard_node(connection, observation.node)
            post_digest = post.digest
            observed_generation = post.data["component_state"]["alignment_generation"]
            if saved is not None and result != observed_generation:
                raise RuntimeError("Clipboard paste generation changed before commit")
            details = {
                "node": observation.node,
                "alignment_generation": None if saved is None else observed_generation,
                "observation_digest": post_digest,
            }
            finish_file_receipt(
                connection, "clipboard_receipts", receipt, "committed",
                details,
            )
            decision = {
                "table": "clipboard_receipts",
                "operation_id": receipt["operation_id"],
                "state": "committed",
                "intent_digest": receipt["intent_digest"],
                "details": details,
            }
            terminal_receipt = dict(
                receipt, state="committed", decision_json=canonical(decision),
                decision_digest=digest(decision),
            )
            finished = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
            if connection.execute(
                "DELETE FROM receiver_mutation_guards WHERE operation_id=?",
                (expected["operation_id"],),
            ).rowcount != len(guards):
                raise RuntimeError("Clipboard receiver guard count changed before commit")
            if connection.execute(
                "UPDATE clipboard_attempts SET state='committed', finished_at=? "
                "WHERE operation_id=? AND state='prepared'",
                (finished, expected["operation_id"]),
            ).rowcount != 1:
                raise RuntimeError("Clipboard attempt changed before commit")
            terminal_attempt = dict(expected, state="committed", finished_at=finished)
            _require_exact_attempt(connection, terminal_attempt, ())
            if read_file_receipt(
                connection, "clipboard_receipts", expected["operation_id"],
            ) != terminal_receipt:
                raise RuntimeError("Clipboard terminal receipt changed before commit")

        def require_committed():
            connection = self._new_db_connection()
            try:
                _require_exact_attempt(connection, terminal_attempt, ())
                if read_file_receipt(
                    connection, "clipboard_receipts", expected["operation_id"],
                ) != terminal_receipt:
                    raise RuntimeError("Clipboard terminal receipt changed after commit")
                if observe_clipboard_node(connection, observation.node).digest != post_digest:
                    raise RuntimeError("Clipboard committed node state changed after commit")
            finally:
                connection.close()
            files.require_published()

        try:
            submit_preparation_decision(self, commit)
        except BaseException as error:
            try:
                require_committed()
            except BaseException as observation_error:
                add_recovery_note(
                    error, "Clipboard commit readback failed: " + str(observation_error),
                )
                raise error
        else:
            require_committed()
        return result
