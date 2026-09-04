from __future__ import annotations

import json

import pytest

from micro_workflow_manager import MicroWorkflow
from micro_workflow_manager.cli.engine import build_engine_snapshot


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
    (project / '.mwf').write_text(json.dumps({
        'edges': edges, 'graph_path': 'src/graph.py',
    }), encoding='utf-8')
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
