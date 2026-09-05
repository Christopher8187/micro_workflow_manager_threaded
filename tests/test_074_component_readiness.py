from __future__ import annotations

import pytest


def test_done_stable_parents_allow_a_stable_successful_result():
    from micro_workflow_manager.component_readiness import calculate_component_readiness

    parents = [('done', 'stable', None), ('done', 'stable', None)]

    assert calculate_component_readiness(parents) == ('stable', None, False)


@pytest.mark.parametrize('interrupt_origin', [None, 'int-new'])
def test_parentless_component_is_ordinarily_ready(interrupt_origin):
    from micro_workflow_manager.component_readiness import calculate_component_readiness

    assert calculate_component_readiness(
        [], interrupt_start_origin=interrupt_origin,
    ) == ('stable', None, False)


def test_done_unstable_parents_preserve_their_exact_shared_origin():
    from micro_workflow_manager.component_readiness import calculate_component_readiness

    parents = [('done', 'unstable', 'int-17'), ('done', 'unstable', 'int-17')]

    assert calculate_component_readiness(parents) == ('unstable', 'int-17', False)
    assert calculate_component_readiness(reversed(parents)) == ('unstable', 'int-17', False)


@pytest.mark.parametrize('incomplete', [
    ('queued', None, None),
    ('running', None, None),
    ('running', 'unstable', 'int-17'),
    ('failed', None, None),
    ('sampled', 'stable', None),
    ('sampled', 'unstable', 'int-17'),
])
def test_incomplete_parent_blocks_even_with_compatible_lineage(incomplete):
    from micro_workflow_manager.component_readiness import calculate_component_readiness

    parents = [('done', 'stable', None), incomplete]

    assert calculate_component_readiness(parents) is None
    assert calculate_component_readiness(reversed(parents)) is None


@pytest.mark.parametrize('parents', [
    [('queued', None, None)],
    [('running', None, None)],
    [('failed', None, None)],
    [('sampled', 'unstable', 'int-17')],
    [('done', 'stable', None), ('done', 'unstable', 'int-17')],
    [('done', 'unstable', 'int-17'), ('done', 'unstable', 'int-18')],
])
def test_authorized_interrupt_start_replaces_blocked_lineage_with_its_new_origin(parents):
    from micro_workflow_manager.component_readiness import calculate_component_readiness

    assert calculate_component_readiness(
        parents, interrupt_start_origin='int-new',
    ) == ('unstable', 'int-new', True)


@pytest.mark.parametrize('parents, expected', [
    ([('done', 'stable', None)], ('stable', None, False)),
    ([('done', 'unstable', 'int-17'), ('done', 'unstable', 'int-17')],
     ('unstable', 'int-17', False)),
])
def test_interrupt_start_preserves_ordinary_readiness_lineage(parents, expected):
    from micro_workflow_manager.component_readiness import calculate_component_readiness

    assert calculate_component_readiness(
        parents, interrupt_start_origin='int-new',
    ) == expected


@pytest.mark.parametrize('parents', [
    [('done', 'stable', None), ('done', 'unstable', 'int-17')],
    [('done', 'unstable', 'int-17'), ('done', 'unstable', 'int-18')],
    [('done', 'unstable', 'int-17'), ('done', 'unstable', 'int-17'),
     ('done', 'unstable', 'int-18')],
])
def test_successful_parents_with_conflicting_lineage_block_ordinary_work(parents):
    from micro_workflow_manager.component_readiness import calculate_component_readiness

    assert calculate_component_readiness(parents) is None
    assert calculate_component_readiness(reversed(parents)) is None


@pytest.mark.parametrize('lifecycle', ['waiting', 'skipped', 'cancelled'])
@pytest.mark.parametrize('invalid_first', [True, False])
@pytest.mark.parametrize('interrupt_origin', [None, 'int-new'])
def test_unknown_component_lifecycle_is_invalid_even_when_another_parent_blocks(
    lifecycle, invalid_first, interrupt_origin,
):
    from micro_workflow_manager.component_readiness import calculate_component_readiness

    parents = [(lifecycle, None, None), ('queued', None, None)]
    if not invalid_first:
        parents.reverse()

    with pytest.raises(ValueError, match='unknown component lifecycle'):
        calculate_component_readiness(parents, interrupt_start_origin=interrupt_origin)


@pytest.mark.parametrize('invalid_done', [
    ('done', 'stable', 'int-17'),
    ('done', 'unstable', None),
    ('done', 'unstable', ''),
    ('done', 'unstable', 17),
    ('done', 'unstable', True),
    ('done', None, None),
    ('done', 'sampled', None),
])
@pytest.mark.parametrize('interrupt_origin', [None, 'int-new'])
def test_impossible_done_results_are_invalid_before_readiness_or_override(
    invalid_done, interrupt_origin,
):
    from micro_workflow_manager.component_readiness import calculate_component_readiness

    for parents in ([invalid_done], [('queued', None, None), invalid_done],
                    [invalid_done, ('queued', None, None)]):
        with pytest.raises(ValueError, match='invalid done component result'):
            calculate_component_readiness(parents, interrupt_start_origin=interrupt_origin)


@pytest.mark.parametrize('invalid_origin', ['', 17, True])
@pytest.mark.parametrize('parents', [[('queued', None, None)], [('done', 'stable', None)]])
def test_interrupt_start_origin_must_be_a_nonempty_exact_string(invalid_origin, parents):
    from micro_workflow_manager.component_readiness import calculate_component_readiness

    with pytest.raises(ValueError, match='invalid interrupt start origin'):
        calculate_component_readiness(parents, interrupt_start_origin=invalid_origin)


def test_readiness_consumes_an_iterable_without_changing_observations_or_exact_origin():
    from micro_workflow_manager.component_readiness import calculate_component_readiness

    parents = [('done', 'unstable', ' exact origin: alpha '),
               ('done', 'unstable', ' exact origin: alpha ')]
    before = parents.copy()

    assert calculate_component_readiness(iter(parents)) == (
        'unstable', ' exact origin: alpha ', False,
    )
    assert parents == before
    assert calculate_component_readiness(
        iter([('sampled', 'stable', None)]),
        interrupt_start_origin=' exact origin: beta ',
    ) == ('unstable', ' exact origin: beta ', True)
