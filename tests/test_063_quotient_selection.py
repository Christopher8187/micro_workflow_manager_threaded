from __future__ import annotations

import pytest

from micro_workflow_manager import MicroWorkflow
from micro_workflow_manager.errors import InvalidGraphError


def test_component_descendants_preserve_directed_branches_and_autostart_members(tmp_path):
    workflow = MicroWorkflow(tmp_path)
    workflow.graph([('A', 'B'), ('B', 'C'), ('A', 'D'), ('D', 'C'), ('X', 'C')])
    workflow.set_autostart_edges([('A', 'B')])

    assert workflow.component_for('B') == {'A', 'B'}
    assert workflow.component_descendants({'A', 'B'}) == [('D',), ('C',)]
    assert workflow.component_descendants({'C'}) == []


def test_between_selects_both_directed_routes_and_excludes_end_and_side_branch(tmp_path):
    workflow = MicroWorkflow(tmp_path)
    workflow.graph([
        ('A', 'B'), ('B', 'D'), ('A', 'C'), ('C', 'D'),
        ('A', 'Side'), ('Outside', 'C'), ('D', 'After'),
    ])

    assert workflow.component_interval('A', 'D') == [('A',), ('B',), ('C',)]


@pytest.mark.parametrize('start,end', [('A', 'B'), ('D', 'A'), ('Side', 'D'), ('A', 'Outside')])
def test_between_requires_a_strict_directed_descendant_component(tmp_path, start, end):
    workflow = MicroWorkflow(tmp_path)
    workflow.graph([('A', 'B'), ('B', 'D'), ('A', 'Side'), ('Outside', 'D')])
    workflow.set_autostart_edges([('A', 'B')])

    with pytest.raises(InvalidGraphError, match='strict directed descendant'):
        workflow.component_interval(start, end)


def test_between_expands_members_excludes_entire_end_component_and_changes_no_state(tmp_path):
    workflow = MicroWorkflow(tmp_path)
    workflow.graph([('A', 'B'), ('B', 'C'), ('C', 'D'), ('D', 'E'), ('E', 'D')])
    workflow.set_autostart_edges([('A', 'B')])
    before = {p.relative_to(tmp_path): p.read_bytes() for p in tmp_path.rglob('*') if p.is_file()}

    assert workflow.component_interval('B', 'E') == [('A', 'B'), ('C',)]
    assert workflow.component_interval('A', 'D') == [('A', 'B'), ('C',)]
    assert {p.relative_to(tmp_path): p.read_bytes() for p in tmp_path.rglob('*') if p.is_file()} == before


def test_between_order_does_not_depend_on_edge_declaration_order(tmp_path):
    edges = [('A', 'C'), ('C', 'D'), ('A', 'B'), ('B', 'D')]
    for number, declaration in enumerate([edges, list(reversed(edges))]):
        workflow = MicroWorkflow(tmp_path / str(number))
        workflow.graph(declaration)

        assert workflow.component_interval('A', 'D') == [('A',), ('B',), ('C',)]


def test_between_handles_many_overlapping_paths_as_one_selection(tmp_path):
    workflow = MicroWorkflow(tmp_path)
    # Thirty two-node layers describe 2**30 paths with only 62 raw nodes.
    layers = [[f'L{i:02d}a', f'L{i:02d}b'] for i in range(30)]
    groups = [['Start'], *layers, ['End']]
    workflow.graph([(a, b) for left, right in zip(groups, groups[1:]) for a in left for b in right])

    selected = workflow.component_interval('Start', 'End')

    assert selected[0] == ('Start',)
    assert len(selected) == 61
    assert set(selected) == {('Start',)} | {(name,) for layer in layers for name in layer}


@pytest.mark.parametrize('start,end', [('Missing', 'B'), ('A', 'Missing')])
def test_between_reports_unknown_endpoint_before_traversing_graph(tmp_path, start, end):
    workflow = MicroWorkflow(tmp_path)
    workflow.graph([('A', 'B')])

    with pytest.raises(InvalidGraphError, match="Unknown.*Missing"):
        workflow.component_interval(start, end)
