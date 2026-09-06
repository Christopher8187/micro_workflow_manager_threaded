from __future__ import annotations

import pytest

from micro_workflow_manager import cli
from micro_workflow_manager.cli.project import load_workflow
from micro_workflow_manager.storage import FileStorage
from tests.test_036_hoeflein_scheduling import make_project
from tests.test_090_component_session_settlement import _close, _rows


def _node_files(root):
    return {path.relative_to(root).as_posix(): path.read_bytes()
            for path in sorted((root / 'node').rglob('*')) if path.is_file()}


@pytest.mark.parametrize('command', ['run', 'runfrom'])
@pytest.mark.parametrize('parent_done,raw_status', [
    pytest.param(False, 'queued', id='blocked-control'),
    pytest.param(True, 'done', id='ready-control'),
    pytest.param(False, 'done', id='raw-done-native-queued'),
    pytest.param(True, 'queued', id='raw-queued-native-done'),
])
def test_ordinary_cli_start_uses_native_parent_readiness(
    tmp_path, monkeypatch, capsys, command, parent_done, raw_status,
):
    make_project(
        tmp_path, monkeypatch, edges="EDGES = [('P', 'A'), ('A', 'B')]",
        files={name: f'''
            from micro_workflow_manager import NodeRouter
            router = NodeRouter('{name}')
            router.create_job(number=1)
            @router.task
            def run(ctx):
                ctx.write_output('ran.txt', '{name}')
                return '{name}'
        ''' for name in ('P', 'A', 'B')},
    )
    workflow = load_workflow(tmp_path)
    try:
        workflow.storage.register_component_topology(workflow.topology.snapshot())
    finally:
        _close(workflow.storage)
    if parent_done:
        assert cli.main(['run', 'P']) == 0
    storage = FileStorage(tmp_path)
    try:
        storage.set_node_status('P', raw_status)
        retained = tmp_path / 'node' / 'A' / 'output' / 'retained.txt'
        retained.parent.mkdir(parents=True, exist_ok=True)
        retained.write_text('preserve if admission refuses', encoding='utf-8')
        storage.db_mutation_barrier()
        before, files = _rows(storage), _node_files(tmp_path)
        parent_state = storage.get_component_state(('P',))
        parent_owner = storage.read_job_current_owner('P', 1)
        parent_events = storage.read_job_events('P', 1)
        capsys.readouterr()

        assert cli.main([command, 'A']) == (0 if parent_done else 1)

        output = capsys.readouterr().out
        assert storage.get_component_state(('P',)) == parent_state
        assert storage.read_job_current_owner('P', 1) == parent_owner
        assert storage.read_job_events('P', 1) == parent_events
        assert {name: data for name, data in _node_files(tmp_path).items()
                if name.startswith('node/P/')} == {
                    name: data for name, data in files.items() if name.startswith('node/P/')}
        if not parent_done:
            assert f'Cannot {command} A:' in output
            assert _rows(storage) == before
            assert _node_files(tmp_path) == files
            return
        selected = [('A',)] if command == 'run' else [('A',), ('B',)]
        sessions = storage.list_execution_sessions()
        assert len(sessions) == len(before['execution_sessions']) + 1
        current = next(session for session in sessions
                       if session['session_id'] not in {row['session_id'] for row in before['execution_sessions']})
        assert current['selected_components'] == selected
        assert (current['status'], current['outcome']) == ('terminal', 'done')
        assert storage.get_node_status('P') == raw_status
        for node in ('A', 'B'):
            ran = (node,) in selected
            assert storage.get_component_state((node,))['lifecycle'] == ('done' if ran else 'queued')
            assert storage.get_job_status(node, 1) == ('done' if ran else 'queued')
            assert (tmp_path / 'node' / node / 'output' / 'ran.txt').exists() is ran
            assert storage.get_component_reservation((node,)) is None
        assert storage.get_component_reservation(('P',)) is None
    finally:
        _close(storage)


@pytest.mark.parametrize('command', ['run', 'runfrom'])
def test_ordinary_cli_missing_parent_state_refuses_without_preparation(tmp_path, monkeypatch, capsys, command):
    make_project(
        tmp_path, monkeypatch, edges="EDGES = [('P', 'A')]",
        files={name: f'''
            from micro_workflow_manager import NodeRouter
            router = NodeRouter('{name}')
            router.create_job(number=1)
            @router.task
            def run(ctx):
                ctx.write_output('ran.txt', '{name}')
        ''' for name in ('P', 'A')},
    )
    storage = FileStorage(tmp_path)
    try:
        assert storage.get_component_state(('P',)) is None
        retained = tmp_path / 'node' / 'A' / 'output' / 'retained.txt'
        retained.parent.mkdir(parents=True, exist_ok=True)
        retained.write_text('retained before the first execution', encoding='utf-8')
        storage.db_mutation_barrier()
        before, files = _rows(storage), _node_files(tmp_path)
        capsys.readouterr()
        assert cli.main([command, 'A']) == 1
        assert _rows(storage) == before
        assert _node_files(tmp_path) == files
        assert f'Cannot {command} A:' in capsys.readouterr().out
    finally:
        _close(storage)


@pytest.mark.parametrize('runner', ['direct', 'threaded', 'api', 'process'])
@pytest.mark.parametrize('command,cyclic,child_job', [
    pytest.param('run', False, False, id='empty-singleton'),
    pytest.param('run', True, False, id='empty-hoeflein-component'),
    pytest.param('runfrom', False, True, id='empty-parent-before-child'),
])
def test_ordinary_cli_completes_empty_selected_components(
    tmp_path, monkeypatch, command, cyclic, child_job, runner,
):
    edges = [('A', 'B'), ('B', 'A')] if cyclic else [('A', 'B')]
    make_project(
        tmp_path, monkeypatch, edges=f'EDGES = {edges!r}', runner=runner,
        files={name: f'''
            from micro_workflow_manager import NodeRouter
            router = NodeRouter('{name}')
            {'router.create_job(number=1)' if name == 'B' and child_job else ''}
            @router.task
            def run(ctx):
                assert ctx.system.storage.get_component_state(('A',))['lifecycle'] == 'done'
                ctx.write_output('ran.txt', '{name}')
                return '{name}'
        ''' for name in ('A', 'B')},
    )
    assert cli.main([command, 'A']) == 0
    storage = FileStorage(tmp_path)
    try:
        selected = [('A', 'B')] if cyclic else [('A',), ('B',)] if child_job else [('A',)]
        session, = storage.list_execution_sessions()
        assert session['selected_components'] == selected
        assert (session['status'], session['outcome']) == ('terminal', 'done')
        for component in selected:
            state = storage.get_component_state(component)
            assert (state['lifecycle'], state['stability'], state['instability_origin']) == ('done', 'stable', None)
            assert storage.get_component_reservation(component) is None
        assert storage.list_job_ids('A') == []
        assert (tmp_path / 'node' / 'B' / 'output' / 'ran.txt').exists() is child_job
        if child_job:
            assert storage.get_job_status('B', 1) == 'done'
        elif not cyclic:
            assert storage.get_component_state(('B',))['lifecycle'] == 'queued'
    finally:
        _close(storage)


@pytest.mark.parametrize('runner', ['direct', 'threaded', 'api', 'process'])
@pytest.mark.parametrize('boundary', ['refuse', 'refuseafter'])
def test_empty_component_obeys_cli_stop_boundary(tmp_path, monkeypatch, boundary, runner):
    make_project(
        tmp_path, monkeypatch, edges="EDGES = [('A', 'B')]", runner=runner,
        files={name: f'''
            from micro_workflow_manager import NodeRouter
            router = NodeRouter('{name}')
            {'router.create_job(number=1)' if name == 'B' else ''}
            @router.task
            def run(ctx):
                ctx.write_output('ran.txt', '{name}')
        ''' for name in ('A', 'B')},
    )
    assert cli.main(['runfrom', 'A', boundary, 'A']) == 0
    storage = FileStorage(tmp_path)
    try:
        assert storage.get_component_state(('A',))['lifecycle'] == ('done' if boundary == 'refuseafter' else 'queued')
        assert storage.get_component_state(('B',))['lifecycle'] == 'queued'
        assert storage.get_job_status('B', 1) == 'queued'
        assert storage.read_job_current_owner('B', 1) is None
        assert not (tmp_path / 'node' / 'B' / 'output' / 'ran.txt').exists()
        session, = storage.list_execution_sessions()
        assert (session['status'], session['outcome']) == ('terminal', 'done')
        assert session['selected_components'] == [('A',), ('B',)]
        assert storage.get_component_reservation(('A',)) is None
        assert storage.get_component_reservation(('B',)) is None
    finally:
        _close(storage)


@pytest.mark.parametrize('runner', ['direct', 'threaded', 'api', 'process'])
@pytest.mark.parametrize('boundary', ['refuse', 'refuseafter'])
def test_cli_stop_boundary_uses_native_component_completion(tmp_path, monkeypatch, runner, boundary):
    make_project(
        tmp_path, monkeypatch, edges="EDGES = [('A', 'B'), ('B', 'C')]", runner=runner,
        files={name: f'''
            from micro_workflow_manager import NodeRouter
            router = NodeRouter('{name}')
            router.create_job(number=1)
            @router.task
            def run(ctx):
                if '{name}' == 'A':
                    ctx.system.storage.set_node_status('B', 'done')
                ctx.write_output('ran.txt', '{name}')
        ''' for name in ('A', 'B', 'C')},
    )
    assert cli.main(['runfrom', 'A', boundary, 'B']) == 0
    storage = FileStorage(tmp_path)
    try:
        for node in ('A', 'B', 'C'):
            ran = node == 'A' or (node == 'B' and boundary == 'refuseafter')
            expected = 'done' if ran else 'queued'
            assert storage.get_component_state((node,))['lifecycle'] == expected
            assert storage.get_job_status(node, 1) == expected
            assert (tmp_path / 'node' / node / 'output' / 'ran.txt').exists() is ran
            assert storage.get_component_reservation((node,)) is None
        session, = storage.list_execution_sessions()
        assert (session['status'], session['outcome']) == ('terminal', 'done')
    finally:
        _close(storage)


@pytest.mark.parametrize('runner', ['direct', 'threaded', 'api', 'process'])
@pytest.mark.parametrize('parent_done', [False, True])
def test_runfrom_distant_merge_preserves_unselected_native_parent(tmp_path, monkeypatch, runner, parent_done):
    make_project(
        tmp_path, monkeypatch, edges="EDGES = [('A', 'C'), ('X', 'C')]", runner=runner,
        files={name: f'''
            from micro_workflow_manager import NodeRouter
            router = NodeRouter('{name}')
            router.create_job(number=1)
            @router.task
            def run(ctx):
                ctx.write_output('ran.txt', '{name}')
        ''' for name in ('A', 'X', 'C')},
    )
    workflow = load_workflow(tmp_path)
    try:
        workflow.storage.register_component_topology(workflow.topology.snapshot())
    finally:
        _close(workflow.storage)
    if parent_done:
        assert cli.main(['run', 'X']) == 0
    storage = FileStorage(tmp_path)
    try:
        raw_status = 'queued' if parent_done else 'done'
        storage.set_node_status('X', raw_status)
        storage.db_mutation_barrier()
        parent = storage.get_component_state(('X',))
        owner = storage.read_job_current_owner('X', 1)
        events = storage.read_job_events('X', 1)
        files = _node_files(tmp_path)
        sessions_before = {session['session_id'] for session in storage.list_execution_sessions()}

        assert cli.main(['runfrom', 'A']) == (0 if parent_done else 1)

        assert storage.get_component_state(('X',)) == parent
        assert storage.get_node_status('X') == raw_status
        assert storage.get_job_status('X', 1) == ('done' if parent_done else 'queued')
        assert storage.read_job_current_owner('X', 1) == owner
        assert storage.read_job_events('X', 1) == events
        assert {name: data for name, data in _node_files(tmp_path).items() if name.startswith('node/X/')} == {
            name: data for name, data in files.items() if name.startswith('node/X/')}
        assert storage.get_component_state(('A',))['lifecycle'] == 'done'
        assert storage.get_job_status('A', 1) == 'done'
        assert storage.get_component_state(('C',))['lifecycle'] == ('done' if parent_done else 'queued')
        assert storage.get_job_status('C', 1) == ('done' if parent_done else 'queued')
        assert (tmp_path / 'node' / 'C' / 'output' / 'ran.txt').exists() is parent_done
        for node in ('A', 'X', 'C'):
            assert storage.get_component_reservation((node,)) is None
        current, = [session for session in storage.list_execution_sessions()
                    if session['session_id'] not in sessions_before]
        assert current['selected_components'] == [('A',), ('C',)]
        assert (current['status'], current['outcome']) == ('terminal', 'done' if parent_done else 'blocked')
        if not parent_done:
            assert storage.read_job_current_owner('C', 1) is None
    finally:
        _close(storage)
