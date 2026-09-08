"""Automatic reconciliation preserves unowned work and does not revive retired results."""

from pathlib import Path

import pytest

from micro_workflow_manager import cli
from micro_workflow_manager.cli.project import load_workflow
from micro_workflow_manager.storage import FileStorage
from tests.test_036_hoeflein_scheduling import make_project
from tests.test_064_read_only_previews import _snapshot
from tests.test_090_component_session_settlement import _close
from tests.test_133_readonly_reset_live_refusal import _closed_database_rows
from tests.test_137_between_run_membership import (
    _close_finished_project, _establish_old_membership, _graph, _membership_edges, _owner_rows,
)


def _unowned_task(node):
    return f"""
from micro_workflow_manager import NodeRouter
router = NodeRouter({node!r}, runner='direct')
router.create_job(number=2)
@router.task
def run(ctx):
    ctx.write_output(f'{{ctx.job_id}}.txt', {node!r})
    return {node!r}
"""


@pytest.mark.parametrize('arguments,completed', [
    (['resume', 'A'], (1, 2)),
    (['run', 'A', 'job', '1'], (1,)),
])
def test_unowned_jobs_survive_automatic_split_before_execution(
    tmp_path, monkeypatch, capsys, arguments, completed,
):
    make_project(
        tmp_path, monkeypatch, edges=_graph([('A', 'B'), ('B', 'A')]),
        files={node: _unowned_task(node) for node in ('A', 'B')}, runner='direct',
    )
    workflow = load_workflow(tmp_path, 'direct')
    storage = workflow.storage
    storage.register_component_topology(workflow.topology.snapshot())
    identities = {(node, job): storage.read_job_instance_id(node, job)
                  for node in ('A', 'B') for job in (1, 2)}
    for node in ('A', 'B'):
        (storage.node_input_dir(node) / 'user.txt').write_text(node, encoding='utf-8')
    _close(storage)
    (tmp_path / 'src' / 'graph.py').write_text(_graph([('A', 'B')]), encoding='utf-8')
    assert cli.main(['graph', '--update']) == 0
    _close_finished_project(tmp_path)
    capsys.readouterr()
    rows, files = _closed_database_rows(tmp_path), _snapshot(tmp_path)

    assert cli.main([*arguments, '--plan']) == 0

    plan = capsys.readouterr().out
    assert 'uninitialized' not in plan and 'would refuse' not in plan
    assert _closed_database_rows(tmp_path) == rows
    assert _snapshot(tmp_path) == files

    assert cli.main(arguments) == 0
    capsys.readouterr()
    storage = FileStorage(tmp_path)
    try:
        for node in ('A', 'B'):
            assert (storage.node_input_dir(node) / 'user.txt').read_text(encoding='utf-8') == node
            for job in (1, 2):
                assert storage.read_job_instance_id(node, job) == identities[node, job]
                processed = node == 'A' and job in completed
                assert storage.get_job_status(node, job) == ('done' if processed else 'queued')
                owner = storage.read_job_current_owner(node, job)
                assert (owner is not None) == processed
                if owner is not None:
                    assert owner['component'] == ('A',)
        assert storage.get_component_state(('A',))['lifecycle'] == (
            'done' if len(completed) == 2 else 'sampled'
        )
        assert storage.get_component_state(('B',))['lifecycle'] == 'queued'
        assert storage.get_component_state(('B',))['alignment_generation'] == 0
        assert storage.get_component_reservation(('A',)) is None
    finally:
        _close(storage)


def test_reintroduced_retired_component_does_not_revive_its_old_result(
    tmp_path, monkeypatch, capsys,
):
    _establish_old_membership(tmp_path, monkeypatch, capsys, 'split')
    assert cli.main(['reset', 'A', '--yes']) == 0
    storage = FileStorage(tmp_path)
    owners = _owner_rows(storage)
    results = [tuple(row) for row in storage.db_connection().execute(
        'SELECT * FROM component_successful_results ORDER BY component_key, shape_id, alignment_generation',
    )]
    floor = max(storage.get_component_state(component)['alignment_generation']
                for component in (('A', 'B'), ('A',), ('B',)))
    identities = {(node, job): storage.read_job_instance_id(node, job)
                  for node in ('A', 'B') for job in storage.list_job_ids(node)}
    _close(storage)
    original, _ = _membership_edges('split')
    (tmp_path / 'src' / 'graph.py').write_text(_graph(original), encoding='utf-8')
    assert cli.main(['graph', '--update']) == 0
    _close_finished_project(tmp_path)
    capsys.readouterr()
    rows, files = _closed_database_rows(tmp_path), _snapshot(tmp_path)

    assert cli.main(['resume', 'A', '--plan']) == 0

    plan = capsys.readouterr().out
    assert '{A, B}: queued' in plan
    assert 'would refuse' not in plan
    assert _closed_database_rows(tmp_path) == rows
    assert _snapshot(tmp_path) == files

    assert cli.main(['resume', 'A']) == 0
    capsys.readouterr()
    storage = FileStorage(tmp_path)
    try:
        state = storage.get_component_state(('A', 'B'))
        assert state['lifecycle'] == 'done' and not state['misaligned']
        assert state['alignment_generation'] > floor
        current = _owner_rows(storage)
        assert {key: current[key] for key in owners} == owners
        after_results = [tuple(row) for row in storage.db_connection().execute(
            'SELECT * FROM component_successful_results ORDER BY component_key, shape_id, alignment_generation',
        )]
        assert all(row in after_results for row in results)
        for (node, job), instance in identities.items():
            assert storage.read_job_instance_id(node, job) == instance
            assert storage.get_job_status(node, job) == 'done'
            owner = storage.read_job_current_owner(node, job)
            assert owner['execution_id'] not in owners
            assert owner['component'] == ('A', 'B')
            assert owner['alignment_generation'] == state['alignment_generation']
    finally:
        _close(storage)
