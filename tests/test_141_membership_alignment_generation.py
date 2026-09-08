"""Changed membership cannot reuse a raw receiver's retained cause generation."""

from micro_workflow_manager import cli
from micro_workflow_manager.cli.project import load_workflow
from micro_workflow_manager.storage import FileStorage
from tests.test_036_hoeflein_scheduling import make_project
from tests.test_064_read_only_previews import _close_without_sidecars
from tests.test_090_component_session_settlement import _close
from tests.test_137_between_run_membership import _graph, _owner_rows


TASKS = {
    'P': """
from micro_workflow_manager import NodeRouter
router = NodeRouter('P', runner='direct')
router.create_job(number=1)
@router.task
def run(ctx):
    ctx.node('A').write_input('source.txt', 'parent input')
    return 'P'
""",
    **{
        node: f"""
from micro_workflow_manager import NodeRouter
router = NodeRouter({node!r}, runner='direct')
router.create_job(number=1)
@router.task
def run(ctx):
    return {node!r}
"""
        for node in ('A', 'B')
    },
}


def test_split_repair_advances_beyond_retained_receiver_cause_generation(
    tmp_path, monkeypatch, capsys,
):
    make_project(
        tmp_path, monkeypatch,
        edges=_graph([('P', 'A'), ('A', 'B'), ('B', 'A')]),
        files=TASKS, runner='direct',
    )
    assert cli.main(['run', 'P', '--runner', 'direct']) == 0
    assert cli.main(['run', 'A', '--runner', 'direct']) == 0
    assert cli.main(['run', 'P', 'job', '1', '--runner', 'direct']) == 0
    capsys.readouterr()
    storage = FileStorage(tmp_path)
    previous_state = storage.get_component_state(('A', 'B'))
    assert previous_state['lifecycle'] == 'done' and previous_state['misaligned']
    previous_causes = storage.read_component_misalignment_causes(('A', 'B'))
    assert len(previous_causes) == 1
    assert previous_causes[0]['receiver_node'] == 'A'
    previous_rows = [dict(row) for row in storage.db_connection().execute(
        'SELECT * FROM component_misalignment_causes ORDER BY receiver_node, alignment_generation',
    )]
    previous_owners = _owner_rows(storage)
    _close_without_sidecars(storage, tmp_path)
    (tmp_path / 'src' / 'graph.py').write_text(
        _graph([('P', 'A'), ('A', 'B')]) + '\n', encoding='utf-8',
    )
    assert cli.main(['graph', '--update']) == 0

    assert cli.main(['reset', 'A', '--yes']) == 0
    storage = FileStorage(tmp_path)
    try:
        reset_state = storage.get_component_state(('A',))
        assert (reset_state['lifecycle'], reset_state['misaligned']) == ('queued', False)
        assert reset_state['alignment_generation'] == previous_state['alignment_generation'] + 1
    finally:
        _close(storage)
    assert cli.main(['resume', 'A', '--runner', 'direct']) == 0
    capsys.readouterr()
    workflow = load_workflow(tmp_path, 'direct')
    storage = workflow.storage
    try:
        repaired = storage.get_component_state(('A',))
        assert (repaired['lifecycle'], repaired['misaligned']) == ('done', False)
        assert repaired['alignment_generation'] == reset_state['alignment_generation']
        assert storage.read_component_misalignment_causes(('A',)) == []
    finally:
        _close(storage)

    assert cli.main(['run', 'P', 'job', '1', '--runner', 'direct']) == 0
    capsys.readouterr()
    storage = FileStorage(tmp_path)
    try:
        current = storage.get_component_state(('A',))
        assert current == dict(repaired, misaligned=True)
        causes = storage.read_component_misalignment_causes(('A',))
        assert len(causes) == 1
        assert causes[0]['receiver_node'] == 'A'
        assert causes[0]['alignment_generation'] == repaired['alignment_generation']
        historical = [dict(row) for row in storage.db_connection().execute(
            'SELECT * FROM component_misalignment_causes WHERE alignment_generation=? '
            'ORDER BY receiver_node, alignment_generation',
            (previous_state['alignment_generation'],),
        )]
        assert historical == previous_rows
        owners = _owner_rows(storage)
        assert {key: owners[key] for key in previous_owners} == previous_owners
        assert (storage.node_input_dir('A') / 'P' / 'source.txt').read_text() == 'parent input'
    finally:
        _close(storage)
