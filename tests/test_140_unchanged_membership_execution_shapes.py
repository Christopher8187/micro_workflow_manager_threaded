"""New executions retain their actual shape after an unrelated graph edit."""

from micro_workflow_manager import cli
from micro_workflow_manager.cli.project import load_workflow
from micro_workflow_manager.storage import FileStorage
from tests.test_036_hoeflein_scheduling import make_project
from tests.test_064_read_only_previews import _close_without_sidecars, _snapshot
from tests.test_090_component_session_settlement import _close
from tests.test_117_execution_sampling import _job_snapshot
from tests.test_133_readonly_reset_live_refusal import _closed_database_rows
from tests.test_137_between_run_membership import _graph, _owner_rows, _close_finished_project


TASKS = {
    "A": """
from micro_workflow_manager import NodeRouter
router = NodeRouter('A', runner='direct')
router.create_job(number=4)
@router.task
def run(ctx):
    return f'A/{ctx.job_id}'
""",
    **{
        node: f"""
from micro_workflow_manager import NodeRouter
router = NodeRouter({node!r}, runner='direct')
@router.task
def run(ctx):
    return {node!r}
"""
        for node in ('B', 'U', 'Z', 'W')
    },
}


def _change_unrelated_region(root, storage, capsys):
    _close_without_sidecars(storage, root)
    (root / 'src' / 'graph.py').write_text(
        _graph([('A', 'B'), ('U', 'W')]) + '\n', encoding='utf-8',
    )
    assert cli.main(['graph', '--update']) == 0
    _close_finished_project(root)
    capsys.readouterr()


def _owner_shape(storage, owner):
    row = storage.db_connection().execute(
        'SELECT shape_json FROM graph_shapes WHERE shape_id=?',
        (owner['shape_id'],),
    ).fetchone()
    assert row is not None
    return row['shape_json']


def test_sampled_resume_after_unrelated_edit_preserves_old_and_records_new_shapes(
    tmp_path, monkeypatch, capsys,
):
    make_project(
        tmp_path, monkeypatch,
        edges=_graph([('A', 'B'), ('U', 'Z')]), files=TASKS, runner='direct',
    )
    assert cli.main([
        'run', 'A', 'sample', '50%', '--seed', 'unrelated-membership',
        '--runner', 'direct',
    ]) == 0
    capsys.readouterr()
    storage = FileStorage(tmp_path)
    sampled = storage.get_component_state(('A',))
    assert (sampled['lifecycle'], sampled['stability']) == ('sampled', 'stable')
    done = [job for job in range(1, 5) if storage.get_job_status('A', job) == 'done']
    queued = [job for job in range(1, 5) if storage.get_job_status('A', job) == 'queued']
    assert len(done) == len(queued) == 2
    retained_jobs = {job: _job_snapshot(storage, 'A', job) for job in done}
    retained_owners = {job: storage.read_job_current_owner('A', job) for job in done}
    retained_rows = _owner_rows(storage)
    old_shape = sampled['shape_json']
    assert all(_owner_shape(storage, owner) == old_shape for owner in retained_owners.values())
    _change_unrelated_region(tmp_path, storage, capsys)
    rows, files = _closed_database_rows(tmp_path), _snapshot(tmp_path)

    assert cli.main(['resume', 'A', '--plan']) == 0
    assert 'would refuse:' not in capsys.readouterr().out
    assert _closed_database_rows(tmp_path) == rows
    assert _snapshot(tmp_path) == files
    assert cli.main(['resume', 'A', '--runner', 'direct']) == 0
    capsys.readouterr()

    workflow = load_workflow(tmp_path, 'direct')
    storage = workflow.storage
    try:
        current_shape = workflow.topology.snapshot().shape_json
        assert current_shape != old_shape
        state = storage.get_component_state(('A',))
        assert (state['lifecycle'], state['stability'], state['instability_origin']) == (
            'done', 'stable', None,
        )
        assert not state['misaligned']
        assert state['alignment_generation'] == sampled['alignment_generation']
        assert {job: _job_snapshot(storage, 'A', job) for job in done} == retained_jobs
        assert {job: storage.read_job_current_owner('A', job) for job in done} == retained_owners
        current_rows = _owner_rows(storage)
        assert {key: current_rows[key] for key in retained_rows} == retained_rows
        for job in queued:
            assert storage.get_job_status('A', job) == 'done'
            owner = storage.read_job_current_owner('A', job)
            assert owner['component'] == ('A',)
            assert _owner_shape(storage, owner) == current_shape
        sessions = storage.list_execution_sessions()
        resumed = [session for session in sessions if session['command'] == 'resume']
        assert len(resumed) == 1
        assert resumed[0]['selected_components'] == [('A',)]
        assert (resumed[0]['status'], resumed[0]['outcome']) == ('terminal', 'done')
        assert storage.get_component_reservation(('A',)) is None
    finally:
        _close(storage)


def test_selected_execution_after_unrelated_edit_keeps_other_successful_owners(
    tmp_path, monkeypatch, capsys,
):
    make_project(
        tmp_path, monkeypatch,
        edges=_graph([('A', 'B'), ('U', 'Z')]), files=TASKS, runner='direct',
    )
    assert cli.main(['run', 'A', '--runner', 'direct']) == 0
    capsys.readouterr()
    storage = FileStorage(tmp_path)
    before = storage.get_component_state(('A',))
    retained_jobs = {job: _job_snapshot(storage, 'A', job) for job in (2, 3, 4)}
    old_owner = storage.read_job_current_owner('A', 1)
    retained_rows = _owner_rows(storage)
    assert _owner_shape(storage, old_owner) == before['shape_json']
    _change_unrelated_region(tmp_path, storage, capsys)

    assert cli.main(['run', 'A', 'job', '1', '--runner', 'direct']) == 0
    capsys.readouterr()

    workflow = load_workflow(tmp_path, 'direct')
    storage = workflow.storage
    try:
        current_shape = workflow.topology.snapshot().shape_json
        assert current_shape != before['shape_json']
        assert {job: _job_snapshot(storage, 'A', job) for job in (2, 3, 4)} == retained_jobs
        current_rows = _owner_rows(storage)
        assert {key: current_rows[key] for key in retained_rows} == retained_rows
        owner = storage.read_job_current_owner('A', 1)
        assert owner['execution_id'] != old_owner['execution_id']
        assert owner['job_instance_id'] == old_owner['job_instance_id']
        assert owner['generation'] == old_owner['generation']
        assert _owner_shape(storage, owner) == current_shape
        state = storage.get_component_state(('A',))
        assert (state['lifecycle'], state['stability'], state['misaligned']) == ('done', 'stable', False)
        assert state['alignment_generation'] == before['alignment_generation']
        assert storage.get_component_reservation(('A',)) is None
    finally:
        _close(storage)
