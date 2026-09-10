"""Diagnostics read native owners without importing or mutating the project."""

from __future__ import annotations

import pytest

from micro_workflow_manager import cli
from micro_workflow_manager.cli.inspect import inspect_failed
from micro_workflow_manager.models import FAILED, now
from tests.test_064_read_only_previews import (
    _close_without_sidecars, _initialize_native_project,
    _install_import_sentinels, _snapshot,
)
from tests.test_146_native_applied_recovery import _create_active_scope


def test_doctor_does_not_import_project_code_or_write_closed_project(tmp_path, monkeypatch, capsys):
    edges = [('A', 'B')]
    workflow = _initialize_native_project(tmp_path, monkeypatch, edges=edges)
    _close_without_sidecars(workflow.storage, tmp_path)
    marker = _install_import_sentinels(tmp_path, edges)
    before = _snapshot(tmp_path)
    capsys.readouterr()

    assert cli.main(['doctor']) == 0

    assert 'Healthy' in capsys.readouterr().out
    assert not marker.exists()
    assert _snapshot(tmp_path) == before


@pytest.mark.parametrize('live', [False, True])
def test_doctor_reports_each_exact_native_owner_and_preserves_state(
    tmp_path, monkeypatch, capsys, live,
):
    workflow = _initialize_native_project(tmp_path, monkeypatch, edges=[('A', 'B')])
    storage = workflow.storage
    shape = workflow.topology.graph_shape()
    _create_active_scope(storage, shape, 'A', 'main-A', live=live)
    _create_active_scope(storage, shape, 'B', 'interrupt-B', live=True)
    _close_without_sidecars(storage, tmp_path)
    before = _snapshot(tmp_path)
    capsys.readouterr()

    assert cli.main(['doctor']) == 0

    output = capsys.readouterr().out
    assert 'main session main-A' in output
    assert 'interrupt session interrupt-B' in output
    assert ('main session main-A requires recovery' in output) is not live
    assert 'interrupt session interrupt-B is live' in output
    assert ('A/1' in output) is not live
    assert _snapshot(tmp_path) == before


def test_inspect_failed_uses_the_jobs_exact_live_session(tmp_path, monkeypatch, capsys):
    workflow = _initialize_native_project(tmp_path, monkeypatch, edges=[('A', 'B')])
    storage = workflow.storage
    shape = workflow.topology.graph_shape()
    owner = _create_active_scope(storage, shape, 'A', 'main-A', live=True)
    _create_active_scope(storage, shape, 'B', 'interrupt-B', live=True)
    storage.finalize_job_execution(
        'A', 1, owner['generation'], owner['execution_id'], FAILED,
        finished_at=now(),
    )
    capsys.readouterr()
    try:
        assert inspect_failed(workflow, 'A') == 0
        output = capsys.readouterr().out
        assert 'restart jobs in session main-A: mwf restart A jobs 1' in output
        assert 'interrupt-B' not in output
    finally:
        storage.close_database_connections()
