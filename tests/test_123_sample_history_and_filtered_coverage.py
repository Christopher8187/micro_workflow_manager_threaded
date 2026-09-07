"""Native sampling history and filtered full-coverage regressions."""

from __future__ import annotations

import copy
import json

import pytest

from micro_workflow_manager import cli
from micro_workflow_manager.storage import FileStorage
from micro_workflow_manager.storage.sample_history import read_sample_admission_history
from tests.test_036_hoeflein_scheduling import make_project
from tests.test_090_component_session_settlement import _close
from tests.test_117_execution_sampling import _job_snapshot
from tests.test_120_native_sample_admission import _admit, _plan, native_selection


def _filtered_project(tmp_path, monkeypatch):
    make_project(
        tmp_path,
        monkeypatch,
        edges="EDGES = [('A', 'A')]",
        runner='direct',
        files={'A': '''
            from micro_workflow_manager import NodeRouter
            router = NodeRouter('A', runner='direct')
            router.create_job(params={'role': 'old-failure'})
            router.create_job(params={'role': 'selected-success'})
            @router.task
            def run(ctx, role):
                if role == 'old-failure':
                    raise ValueError('expected old failure')
                return role
        '''},
    )
    assert cli.main(['run', 'A', 'job', '1', '--runner', 'direct']) == 1
    storage = FileStorage(tmp_path)
    assert storage.get_job_status('A', 1) == 'failed'
    assert storage.get_job_status('A', 2) == 'queued'
    return storage


def _latest_sample(storage):
    sessions = [session for session in storage.list_execution_sessions()
                if session['command'] == 'run sample']
    session, = sessions
    return session, session['details']['selection']


def test_status_filtered_full_coverage_ignores_unchanged_older_ineligible_job(
    tmp_path, monkeypatch, capsys,
):
    storage = _filtered_project(tmp_path, monkeypatch)
    old = _job_snapshot(storage, 'A', 1)
    capsys.readouterr()
    try:
        assert cli.main([
            'run', 'A', 'sample', '100%', '--status', 'queued',
            '--seed', 'filtered-full', '--runner', 'direct',
        ]) == 0
        output = capsys.readouterr().out.lower()
        assert 'full coverage' in output
        assert _job_snapshot(storage, 'A', 1) == old
        assert storage.get_job_status('A', 2) == 'done'
        state = storage.get_component_state(('A',))
        assert (state['lifecycle'], state['stability'], state['misaligned']) == (
            'done', 'stable', False,
        )
        session, selection = _latest_sample(storage)
        assert session['selected_jobs'] == [('A', 2)]
        member = selection['members']['A']
        assert [(job['job_id'], job['status']) for job in member['starting_jobs']] == [
            (1, 'failed'), (2, 'queued'),
        ]
        assert [job['job_id'] for job in member['population']] == [2]
        assert selection['full_starting_coverage'] is True
    finally:
        _close(storage)


@pytest.mark.parametrize('arrival', ['new', 'replacement'])
def test_full_starting_population_stays_sampled_when_newer_work_is_unprocessed(
    tmp_path, monkeypatch, capsys, arrival,
):
    from micro_workflow_manager.cli import run_selected

    storage = _filtered_project(tmp_path, monkeypatch)
    original = run_selected.prepare_selected_addresses

    def add_after_preparation(root, workflow, addresses, **kwargs):
        result = original(root, workflow, addresses, **kwargs)
        if arrival == 'replacement':
            workflow.storage.delete_job('A', 1)
            job_id = 1
        else:
            job_id = 3
        workflow.add_job(None, 'A', job_id=job_id, role='late-unprocessed')
        return result

    capsys.readouterr()
    try:
        with monkeypatch.context() as patch:
            patch.setattr(run_selected, 'prepare_selected_addresses', add_after_preparation)
            assert cli.main([
                'run', 'A', 'sample', '100%', '--status', 'queued',
                '--seed', 'late-work', '--runner', 'direct',
            ]) == 0
        late_id = 1 if arrival == 'replacement' else 3
        assert storage.get_job_status('A', 2) == 'done'
        assert storage.get_job_status('A', late_id) == 'queued'
        assert storage.read_job_current_owner('A', late_id) is None
        assert storage.get_component_state(('A',))['lifecycle'] == 'sampled'
        _session, selection = _latest_sample(storage)
        assert selection['full_starting_coverage'] is True
        assert selection['members']['A']['selected_job_ids'] == [2]
    finally:
        _close(storage)


@pytest.mark.parametrize('damage', [
    'coverage', 'baseline-status', 'baseline-instance', 'baseline-digest',
    'population-status', 'selected-ids', 'shape', 'session-root',
    'boolean-count', 'boolean-selector', 'boolean-job-id',
])
def test_sample_history_reader_refuses_inconsistent_persisted_claim(native_selection, damage):
    storage, snapshot = native_selection
    admitted = _admit(
        storage, snapshot,
        lambda connection: _plan(storage, snapshot, connection, ('A=100%', 'B=100%')),
    )
    session_id = admitted['session_id']
    details = copy.deepcopy(admitted['details'])
    selection = details['selection']
    member = selection['members']['A']
    if damage == 'coverage':
        selection['full_starting_coverage'] = False
    elif damage == 'baseline-status':
        member['starting_jobs'][0]['status'] = 'done'
    elif damage == 'baseline-instance':
        member['starting_jobs'][0]['job_instance_id'] = '0' * 32
    elif damage == 'baseline-digest':
        member['starting_jobs_digest'] = 'sha256:' + '0' * 64
    elif damage == 'population-status':
        member['population'][0]['status'] = 'done'
    elif damage == 'selected-ids':
        member['selected_job_ids'] = member['selected_job_ids'][:-1]
    elif damage == 'boolean-count':
        member['population_count'] = True
    elif damage == 'boolean-selector':
        member['selector']['value'] = True
    elif damage == 'boolean-job-id':
        member['selected_job_ids'][0] = True
    elif damage == 'shape':
        selection['shape'] = '[]'
    elif damage == 'session-root':
        storage.db_connection().execute(
            "UPDATE session_jobs SET job_instance_id=? WHERE session_id=? AND position=0",
            ('f' * 32, session_id),
        )
    if damage != 'session-root':
        storage.db_connection().execute(
            'UPDATE execution_sessions SET details_json=? WHERE session_id=?',
            (json.dumps(details), session_id),
        )

    with pytest.raises(RuntimeError, match='sample|Sample'):
        read_sample_admission_history(
            storage, storage.db_connection(), session_id,
            component=('A', 'B'), expected_shape=snapshot.shape_json,
        )


def test_status_excluded_changes_do_not_change_guarded_replay_population_digest(
    tmp_path, monkeypatch, capsys,
):
    from micro_workflow_manager.storage.component_definitions import component_snapshot_from_shape
    from micro_workflow_manager.storage.sample_planning import plan_sample

    storage = _filtered_project(tmp_path, monkeypatch)

    def observe():
        state = storage.get_component_state(('A',))
        snapshot = component_snapshot_from_shape(state['shape_json'])
        return plan_sample(
            storage.db_connection(), storage.project_dir, snapshot, 'A', ('100%',),
            seed='filtered-replay', statuses=('queued',), expected_population=None,
        )

    try:
        first = observe()
        first_failed = first.members[0].starting_jobs[0]
        assert first_failed['status'] == 'failed'
        assert type(first_failed['generation']) is int and first_failed['generation'] >= 0
        assert [job['job_id'] for job in first.members[0].population] == [2]

        capsys.readouterr()
        storage.set_job_status('A', 1, 'cancelled', reason='excluded status change')
        storage.db_mutation_barrier()
        capsys.readouterr()
        second = observe()
        second_failed = second.members[0].starting_jobs[0]
        assert second_failed['status'] == 'cancelled'
        assert second_failed['generation'] == first_failed['generation']
        assert second_failed['job_instance_id'] == first_failed['job_instance_id']
        assert second.members[0].starting_jobs_digest != first.members[0].starting_jobs_digest
        assert second.members[0].population == first.members[0].population
        assert second.combined_digest == first.combined_digest
        assert cli.main([
            'run', 'A', 'sample', '100%', '--status', 'queued', '--seed', 'filtered-replay',
            '--expect-population', first.combined_digest, '--plan',
        ]) == 0
        capsys.readouterr()

        storage.set_job_status('A', 1, 'queued', reason='job enters sample status filter')
        storage.db_mutation_barrier()
        capsys.readouterr()
        entered = observe()
        assert [job['job_id'] for job in entered.members[0].population] == [1, 2]
        assert entered.combined_digest != first.combined_digest
        assert cli.main([
            'run', 'A', 'sample', '100%', '--status', 'queued', '--seed', 'filtered-replay',
            '--expect-population', first.combined_digest, '--plan',
        ]) == 1
        assert 'changed' in capsys.readouterr().err.lower()
    finally:
        _close(storage)