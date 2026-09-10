from __future__ import annotations

import json

import pytest

from micro_workflow_manager import MicroWorkflow
from micro_workflow_manager.cli.engine import build_engine_snapshot
from micro_workflow_manager.project_format import new_project_config


@pytest.mark.parametrize('reverse', [False, True])
def test_runtime_and_engine_keep_the_same_autostart_components_and_quotient(tmp_path, reverse):
    edges = [('Before', 'A'), ('A', 'B'), ('B', 'C'), ('B', 'Side'), ('Outside', 'C')]
    if reverse:
        edges.reverse()
    workflow = MicroWorkflow(tmp_path / 'runtime')
    workflow.graph(edges)
    workflow.set_autostart_edges([('A', 'B')])

    project = tmp_path / 'engine'
    behavior = project / 'src' / 'node_behavior'
    behavior.mkdir(parents=True)
    metadata = project / '.mwf'
    metadata.mkdir()
    config = new_project_config()
    config.update({
        'edges': [list(edge) for edge in edges], 'graph_path': 'src/graph.py',
    })
    (metadata / 'project.json').write_text(json.dumps(config), encoding='utf-8')
    (behavior / 'A.py').write_text(
        'def publish(ctx):\n    ctx.node("B").add(autostart=True)\n', encoding='utf-8',
    )
    before = {p.relative_to(project): p.read_bytes() for p in project.rglob('*') if p.is_file()}

    snapshot = build_engine_snapshot(project)

    expected = {('Before',), ('A', 'B'), ('C',), ('Side',), ('Outside',)}
    assert {tuple(node['members']) for node in snapshot['nodes']} == expected
    assert {workflow.component_key(c) for c in workflow.hoeflein_components()} == expected
    rendered = {node['id']: tuple(node['members']) for node in snapshot['nodes']}
    expected_edges = {
        (('Before',), ('A', 'B')), (('A', 'B'), ('C',)),
        (('A', 'B'), ('Side',)), (('Outside',), ('C',)),
    }
    assert {(rendered[e['source']], rendered[e['target']]) for e in snapshot['edges']} == expected_edges
    assert set(workflow.component_dag().edges) == expected_edges
    assert workflow.component_predecessors({'A', 'B'}) == {'Before'}
    assert workflow.component_predecessor_components({'C'}) == {('A', 'B'), ('Outside',)}
    assert workflow.component_interval('Before', 'C') == [('Before',), ('A', 'B')]
    assert {p.relative_to(project): p.read_bytes() for p in project.rglob('*') if p.is_file()} == before


def test_topology_queries_reflect_later_autostart_registration_and_replacement(tmp_path):
    workflow = MicroWorkflow(tmp_path)
    workflow.graph([('A', 'B'), ('B', 'C')])
    assert workflow.component_for('A') == {'A'}
    assert workflow.component_descendants({'A'}) == [('B',), ('C',)]

    workflow.register_autostart_edge('A', 'B')
    assert workflow.component_for('A') == {'A', 'B'}
    assert workflow.component_interval('B', 'C') == [('A', 'B')]
    assert workflow.execution_components(['B']) == [('A', 'B')]

    workflow.set_autostart_edges([('B', 'C')])
    assert workflow.component_for('A') == {'A'}
    assert workflow.component_for('C') == {'B', 'C'}
    assert workflow.component_interval('A', 'C') == [('A',)]


@pytest.mark.parametrize(
    'method,autostart,import_line,constructor,expected',
    [
        pytest.param(
            'add_job', True,
            'from micro_workflow_manager import NodeInputFileSystem',
            'NodeInputFileSystem', {('A', 'B'), ('C',)}, id='single-true',
        ),
        pytest.param(
            'add_jobs', True,
            'from micro_workflow_manager import NodeInputFileSystem as InputTarget',
            'InputTarget', {('A', 'B'), ('C',)}, id='batch-true',
        ),
        pytest.param(
            'add_job', False,
            'from micro_workflow_manager import NodeInputFileSystem',
            'NodeInputFileSystem', {('A',), ('B',), ('C',)}, id='single-false',
        ),
    ],
)
def test_engine_reads_literal_node_input_autostart_without_importing(
    tmp_path, method, autostart, import_line, constructor, expected,
):
    project = tmp_path / 'engine'
    behavior = project / 'src' / 'node_behavior'
    behavior.mkdir(parents=True)
    metadata = project / '.mwf'
    metadata.mkdir()
    config = new_project_config()
    config.update({
        'edges': [['A', 'B'], ['B', 'C']], 'graph_path': 'src/graph.py',
    })
    (metadata / 'project.json').write_text(json.dumps(config), encoding='utf-8')
    positional = '' if method == 'add_job' else ', [{}]'
    call = f'target.{method}(ctx{positional}, autostart={autostart!r})'
    (behavior / 'A.py').write_text(
        'raise AssertionError("engine snapshot imported user code")\n'
        f'{import_line}\n'
        f'target = {constructor}("B")\n'
        'def publish(ctx):\n'
        f'    {call}\n',
        encoding='utf-8',
    )
    before = {p.relative_to(project): p.read_bytes() for p in project.rglob('*') if p.is_file()}

    snapshot = build_engine_snapshot(project)

    assert {tuple(node['members']) for node in snapshot['nodes']} == expected
    assert {p.relative_to(project): p.read_bytes() for p in project.rglob('*') if p.is_file()} == before


@pytest.mark.parametrize(
    'source',
    [
        pytest.param(
            'class NodeInputFileSystem:\n'
            '    def __init__(self, node):\n'
            '        self.node = node\n'
            'target = NodeInputFileSystem("B")\n',
            id='local-lookalike',
        ),
        pytest.param(
            'from micro_workflow_manager import NodeInputFileSystem\n'
            'NodeInputFileSystem = object\n'
            'target = NodeInputFileSystem("B")\n',
            id='constructor-rebound',
        ),
        pytest.param(
            'from micro_workflow_manager import NodeInputFileSystem\n'
            'def replace_constructor():\n'
            '    global NodeInputFileSystem\n'
            '    NodeInputFileSystem = object\n'
            'replace_constructor()\n'
            'target = NodeInputFileSystem("B")\n',
            id='global-constructor-rebound',
        ),
        pytest.param(
            'from micro_workflow_manager import NodeInputFileSystem\n'
            'target = NodeInputFileSystem("B")\n'
            'target = object()\n',
            id='handle-rebound',
        ),
        pytest.param(
            'from micro_workflow_manager import NodeInputFileSystem\n'
            'target = NodeInputFileSystem("B")\n'
            'from other import *\n',
            id='wildcard-import',
        ),
        pytest.param(
            'from micro_workflow_manager import NodeInputFileSystem\n'
            'target = NodeInputFileSystem(node_name="B")\n',
            id='keyword-construction',
        ),
        pytest.param(
            'import micro_workflow_manager as mwf\n'
            'target = mwf.NodeInputFileSystem("B")\n',
            id='module-attribute',
        ),
        pytest.param(
            'from micro_workflow_manager import NodeInputFileSystem\n'
            'NodeInputFileSystem("B").add_job(ctx, autostart=True)\n',
            id='temporary-constructor',
        ),
    ],
)
def test_engine_ignores_unverified_node_input_bindings_without_importing(tmp_path, source):
    project = tmp_path / 'engine'
    behavior = project / 'src' / 'node_behavior'
    behavior.mkdir(parents=True)
    metadata = project / '.mwf'
    metadata.mkdir()
    config = new_project_config()
    config.update({
        'edges': [['A', 'B'], ['B', 'C']], 'graph_path': 'src/graph.py',
    })
    (metadata / 'project.json').write_text(json.dumps(config), encoding='utf-8')
    (behavior / 'A.py').write_text(
        'raise AssertionError("engine snapshot imported user code")\n'
        f'{source}'
        'def publish(ctx):\n'
        '    target.add_job(ctx, autostart=True)\n',
        encoding='utf-8',
    )
    before = {p.relative_to(project): p.read_bytes() for p in project.rglob('*') if p.is_file()}

    snapshot = build_engine_snapshot(project)

    assert {tuple(node['members']) for node in snapshot['nodes']} == {('A',), ('B',), ('C',)}
    assert {p.relative_to(project): p.read_bytes() for p in project.rglob('*') if p.is_file()} == before
