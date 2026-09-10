"""Grouped API permit decisions preserve native ownership and request order."""

from __future__ import annotations

from threading import Event

import pytest

from micro_workflow_manager.models import Job, now
from micro_workflow_manager.storage.priorities import ADMISSION_PRIORITY
from tests.test_064_read_only_previews import (
    _initialize_native_project,
    _live_execution_session_identity,
)
from tests.test_090_component_session_settlement import _close


def _active_api_owners(tmp_path, monkeypatch, count=4):
    workflow = _initialize_native_project(
        tmp_path, monkeypatch, edges=[("A", "A")],
    )
    storage = workflow.storage
    shape = workflow.topology.snapshot().shape_json
    session_id = "grouped-api-owner"
    for job_id in range(1, count + 1):
        storage.create_job(Job(node_name="A", job_id=job_id, params={"value": job_id}))
    storage.create_execution_session(
        session_id,
        session_kind="main",
        command="run",
        start_component=("A",),
        selected_components=[("A",)],
        **_live_execution_session_identity(),
        expected_shape=shape,
    )
    storage.reserve_execution_components(session_id, expected_shape=shape)
    storage.begin_queued_component_execution(
        session_id,
        ("A",),
        expected_shape=shape,
        expected_alignment_generation=0,
        successful_lineage=("stable", None),
    )
    claims = storage.claim_job_executions_batch(
        "A",
        list(range(1, count + 1)),
        started_at=now(),
        session_id=session_id,
        component=("A",),
    )
    identities = [
        (session_id, "A", job_id, generation, execution_id)
        for job_id, (generation, execution_id) in enumerate(claims, start=1)
    ]
    owners = [
        storage.get_job_execution_owner(execution_id)
        for _session, _node, _job, _generation, execution_id in identities
    ]
    return workflow, identities, owners


def _permit_rows(storage):
    return [
        tuple(row)
        for row in storage.db_connection().execute(
            "SELECT session_id, node_name, job_id, generation, execution_id "
            "FROM api_execution_permits ORDER BY node_name, job_id, generation"
        )
    ]


def _business_rows(storage):
    connection = storage.db_connection()
    tables = [
        row[0]
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name NOT LIKE 'sqlite_%' ORDER BY name"
        )
        if row[0] != "api_execution_permits"
    ]
    return {
        table: [tuple(row) for row in connection.execute(
            f'SELECT * FROM "{table.replace(chr(34), chr(34) * 2)}" ORDER BY rowid'
        )]
        for table in tables
    }


def _queue_together(storage, submitters, *, trigger_sql=None, trigger_name=None):
    entered = Event()
    release = Event()

    def hold_writer(connection):
        if trigger_sql is not None:
            connection.execute(trigger_sql)
        entered.set()
        assert release.wait(10)

    blocker = storage.submit_db_mutation(
        hold_writer, wait=False, priority=ADMISSION_PRIORITY,
    )
    assert entered.wait(10)
    try:
        futures = [submit() for submit in submitters]
        cleanup = None
        if trigger_name is not None:
            cleanup = storage.submit_db_mutation(
                lambda connection: connection.execute(
                    f'DROP TRIGGER temp."{trigger_name}"'
                ),
                wait=False,
                priority=ADMISSION_PRIORITY,
            )
    finally:
        release.set()
    blocker.result(timeout=10)
    return futures, cleanup


def test_mixed_acquire_release_requests_keep_writer_serial_order(
    tmp_path, monkeypatch,
):
    workflow, identities, owners = _active_api_owners(tmp_path, monkeypatch, count=2)
    storage = workflow.storage
    storage.set_api_total_limit(1)
    before = _business_rows(storage)
    try:
        futures, cleanup = _queue_together(storage, [
            lambda: storage.try_acquire_api_execution_permit(*identities[0], _wait=False),
            lambda: storage.release_api_execution_permit(*identities[0], _wait=False),
            lambda: storage.try_acquire_api_execution_permit(*identities[1], _wait=False),
        ])
        assert cleanup is None
        first, released, second = futures
        assert first.result(timeout=10) is True
        assert released.result(timeout=10) is True
        assert second.result(timeout=10) is True
        assert _permit_rows(storage) == [identities[1]]
        assert _business_rows(storage) == before
        assert [
            storage.get_job_execution_owner(identity[-1]) for identity in identities
        ] == owners
    finally:
        if _permit_rows(storage):
            assert storage.release_api_execution_permit(*identities[1]) is True
        _close(storage)


def test_grouped_acquire_returns_stable_cap_results_for_duplicate_requests(
    tmp_path, monkeypatch,
):
    workflow, identities, owners = _active_api_owners(tmp_path, monkeypatch, count=3)
    storage = workflow.storage
    storage.set_api_total_limit(2)
    before = _business_rows(storage)
    try:
        requested = [identities[0], identities[0], identities[1], identities[2]]
        futures, cleanup = _queue_together(storage, [
            lambda identity=identity: storage.try_acquire_api_execution_permit(
                *identity, _wait=False,
            )
            for identity in requested
        ])
        assert cleanup is None
        assert [future.result(timeout=10) for future in futures] == [
            True, True, True, False,
        ]
        assert _permit_rows(storage) == identities[:2]
        assert _business_rows(storage) == before
        assert [
            storage.get_job_execution_owner(identity[-1]) for identity in identities
        ] == owners
    finally:
        for identity in identities[:2]:
            if identity in _permit_rows(storage):
                assert storage.release_api_execution_permit(*identity) is True
        _close(storage)


def test_grouped_acquire_refuses_one_unknown_owner_without_rolling_back_peers(
    tmp_path, monkeypatch,
):
    workflow, identities, owners = _active_api_owners(tmp_path, monkeypatch, count=2)
    storage = workflow.storage
    before = _business_rows(storage)
    unknown = (*identities[0][:-1], "0" * 32)
    try:
        requested = [identities[0], unknown, identities[1]]
        futures, cleanup = _queue_together(storage, [
            lambda identity=identity: storage.try_acquire_api_execution_permit(
                *identity, _wait=False,
            )
            for identity in requested
        ])
        assert cleanup is None
        assert futures[0].result(timeout=10) is True
        with pytest.raises(RuntimeError, match="immutable owner"):
            futures[1].result(timeout=10)
        assert futures[2].result(timeout=10) is True
        assert _permit_rows(storage) == identities
        assert _business_rows(storage) == before
        assert [
            storage.get_job_execution_owner(identity[-1]) for identity in identities
        ] == owners
    finally:
        for identity in identities:
            if identity in _permit_rows(storage):
                assert storage.release_api_execution_permit(*identity) is True
        _close(storage)


def test_grouped_acquire_rolls_back_one_suppressed_insert_and_commits_peers(
    tmp_path, monkeypatch,
):
    workflow, identities, owners = _active_api_owners(tmp_path, monkeypatch, count=3)
    storage = workflow.storage
    before = _business_rows(storage)
    refused = identities[1]
    cleanup = None
    try:
        futures, cleanup = _queue_together(
            storage,
            [
                lambda identity=identity: storage.try_acquire_api_execution_permit(
                    *identity, _wait=False,
                )
                for identity in identities
            ],
            trigger_sql=(
                "CREATE TEMP TRIGGER suppress_grouped_api_permit_insert "
                "BEFORE INSERT ON api_execution_permits "
                f"WHEN NEW.execution_id='{refused[-1]}' "
                "BEGIN SELECT RAISE(IGNORE); END"
            ),
            trigger_name="suppress_grouped_api_permit_insert",
        )
        assert futures[0].result(timeout=10) is True
        with pytest.raises(RuntimeError, match="not recorded"):
            futures[1].result(timeout=10)
        assert futures[2].result(timeout=10) is True
        assert _permit_rows(storage) == [identities[0], identities[2]]
        assert _business_rows(storage) == before
        assert [
            storage.get_job_execution_owner(identity[-1]) for identity in identities
        ] == owners
    finally:
        if cleanup is not None:
            cleanup.result(timeout=10)
        for identity in (identities[0], identities[2]):
            if identity in _permit_rows(storage):
                assert storage.release_api_execution_permit(*identity) is True
        _close(storage)


def test_grouped_release_rolls_back_one_suppressed_delete_and_commits_peers(
    tmp_path, monkeypatch,
):
    workflow, identities, owners = _active_api_owners(tmp_path, monkeypatch, count=3)
    storage = workflow.storage
    for identity in identities:
        assert storage.try_acquire_api_execution_permit(*identity) is True
    before = _business_rows(storage)
    retained = identities[1]
    cleanup = None
    try:
        futures, cleanup = _queue_together(
            storage,
            [
                lambda identity=identity: storage.release_api_execution_permit(
                    *identity, _wait=False,
                )
                for identity in identities
            ],
            trigger_sql=(
                "CREATE TEMP TRIGGER suppress_grouped_api_permit_delete "
                "BEFORE DELETE ON api_execution_permits "
                f"WHEN OLD.execution_id='{retained[-1]}' "
                "BEGIN SELECT RAISE(IGNORE); END"
            ),
            trigger_name="suppress_grouped_api_permit_delete",
        )
        assert futures[0].result(timeout=10) is True
        with pytest.raises(RuntimeError, match="changed before release"):
            futures[1].result(timeout=10)
        assert futures[2].result(timeout=10) is True
        assert _permit_rows(storage) == [retained]
        assert _business_rows(storage) == before
        assert [
            storage.get_job_execution_owner(identity[-1]) for identity in identities
        ] == owners
    finally:
        if cleanup is not None:
            cleanup.result(timeout=10)
        if retained in _permit_rows(storage):
            assert storage.release_api_execution_permit(*retained) is True
        _close(storage)
