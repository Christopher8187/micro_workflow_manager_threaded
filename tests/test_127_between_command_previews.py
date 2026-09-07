"""Public previews select half-open quotient intervals without changing a project."""

from __future__ import annotations

import pytest

from micro_workflow_manager import cli
from tests.test_064_read_only_previews import (
    _close_without_sidecars, _initialize_native_project,
    _install_import_sentinels, _snapshot,
)


@pytest.mark.parametrize('command,flag', [
    ('runbetween', '--plan'),
    ('resumebetween', '--plan'),
    ('resetbetween', '--dry-run'),
])
def test_between_previews_show_closed_start_excluded_end_and_crossing_edges(
    tmp_path, monkeypatch, capsys, command, flag,
):
    edges = [
        ('P', 'A'), ('A', 'L'), ('A', 'R'), ('L', 'B'), ('R', 'B'),
        ('B', 'Z'), ('A', 'Off'), ('B', 'Bpeer'), ('Bpeer', 'B'), ('X', 'R'),
    ]
    workflow = _initialize_native_project(tmp_path, monkeypatch, edges=edges)
    _close_without_sidecars(workflow.storage, tmp_path)
    sentinel = _install_import_sentinels(tmp_path, edges)
    before = _snapshot(tmp_path)
    capsys.readouterr()

    assert cli.main([command, 'A', 'Bpeer', flag]) == 0

    output = capsys.readouterr().out
    assert 'selected Hoeflein components: {A}, {L}, {R}' in output
    assert 'selected nodes: A, L, R' in output
    assert 'excluded end component: {B, Bpeer}' in output
    assert 'entering edges: P -> A, X -> R' in output
    assert 'leaving edges: A -> Off, L -> B, R -> B' in output
    assert 'unselected receivers: B, Off' in output
    assert 'P: queued' in output
    assert 'X: queued' in output
    assert 'user code was not loaded' in output
    assert _snapshot(tmp_path) == before
    assert not sentinel.exists()
    assert not (tmp_path / '.mwf' / 'state.sqlite3-wal').exists()
    assert not (tmp_path / '.mwf' / 'state.sqlite3-shm').exists()


@pytest.mark.parametrize('command,flag', [
    ('runbetween', '--plan'),
    ('resumebetween', '--plan'),
    ('resetbetween', '--dry-run'),
])
@pytest.mark.parametrize('edges,start,end', [
    ([('A', 'B')], 'A', 'A'),
    ([('A', 'B'), ('B', 'A')], 'A', 'B'),
    ([('A', 'B')], 'B', 'A'),
    ([('A', 'M'), ('B', 'M')], 'A', 'B'),
])
def test_between_previews_refuse_non_descendant_endpoints_without_changes(
    tmp_path, monkeypatch, capsys, command, flag, edges, start, end,
):
    workflow = _initialize_native_project(tmp_path, monkeypatch, edges=edges)
    _close_without_sidecars(workflow.storage, tmp_path)
    sentinel = _install_import_sentinels(tmp_path, edges)
    before = _snapshot(tmp_path)
    capsys.readouterr()

    assert cli.main([command, start, end, flag]) == 1

    assert 'strict directed descendant' in capsys.readouterr().err
    assert _snapshot(tmp_path) == before
    assert not sentinel.exists()
    assert not (tmp_path / '.mwf' / 'state.sqlite3-wal').exists()
    assert not (tmp_path / '.mwf' / 'state.sqlite3-shm').exists()
