from __future__ import annotations

import pytest


def test_shorthand_count_selects_only_the_named_component_member():
    from micro_workflow_manager.sample_selection import parse_sample_selectors

    selectors = parse_sample_selectors('X', ('Z', 'X', 'Y'), ['30'])

    assert selectors == {'X': ('count', 30), 'Y': ('count', 0), 'Z': ('count', 0)}


def test_shorthand_percentage_selects_only_the_named_component_member():
    from micro_workflow_manager.sample_selection import parse_sample_selectors

    selectors = parse_sample_selectors('Y', ('X', 'Y', 'Z'), ['10%'])

    assert selectors == {'X': ('percentage', 0), 'Y': ('percentage', 10), 'Z': ('percentage', 0)}


def test_named_counts_select_several_members_and_leave_omitted_members_at_zero():
    from micro_workflow_manager.sample_selection import parse_sample_selectors

    selectors = parse_sample_selectors('X', ('X', 'Y', 'Z'), ['X=30', 'Y=10'])

    assert selectors == {'X': ('count', 30), 'Y': ('count', 10), 'Z': ('count', 0)}


def test_named_percentages_keep_independent_member_values():
    from micro_workflow_manager.sample_selection import parse_sample_selectors

    selectors = parse_sample_selectors('X', ('X', 'Y', 'Z'), ['X=10%', 'Y=25%'])

    assert selectors == {'X': ('percentage', 10), 'Y': ('percentage', 25), 'Z': ('percentage', 0)}


@pytest.mark.parametrize('tokens,kind', [
    (['0'], 'count'), (['0%'], 'percentage'),
    (['X=0', 'Y=0'], 'count'), (['X=0%', 'Y=0%'], 'percentage'),
])
def test_explicit_zero_is_a_valid_selector_for_every_form(tokens, kind):
    from micro_workflow_manager.sample_selection import parse_sample_selectors

    assert parse_sample_selectors('X', ('X', 'Y'), tokens) == {'X': (kind, 0), 'Y': (kind, 0)}


@pytest.mark.parametrize('start_node,tokens', [('X', ['Outside=1']), ('Outside', ['1'])])
def test_selectors_cannot_name_nodes_outside_the_starting_component(start_node, tokens):
    from micro_workflow_manager.sample_selection import parse_sample_selectors

    with pytest.raises(ValueError, match='component'):
        parse_sample_selectors(start_node, ('X', 'Y'), tokens)


@pytest.mark.parametrize('tokens', [
    [], ['1', '2'], ['X=1', 'X=2'], ['X=10%', 'Y=25'],
])
def test_incomplete_duplicate_or_mixed_requests_are_refused(tokens):
    from micro_workflow_manager.sample_selection import parse_sample_selectors

    with pytest.raises(ValueError):
        parse_sample_selectors('X', ('X', 'Y'), tokens)


@pytest.mark.parametrize('token', ['+3', '003', '3'])
def test_existing_integer_spellings_keep_their_count(token):
    from micro_workflow_manager.sample_selection import parse_sample_selectors

    assert parse_sample_selectors('X', ('X',), [token]) == {'X': ('count', 3)}


@pytest.mark.parametrize('token', ['2.5%', 'abc', '10%%'])
def test_non_integer_selector_values_are_refused(token):
    from micro_workflow_manager.sample_selection import parse_sample_selectors

    with pytest.raises(ValueError):
        parse_sample_selectors('X', ('X',), [token])


@pytest.mark.parametrize('token', ['-1', '-1%', '101%'])
def test_negative_selectors_and_percentages_above_full_coverage_are_refused(token):
    from micro_workflow_manager.sample_selection import parse_sample_selectors

    with pytest.raises(ValueError):
        parse_sample_selectors('X', ('X',), [token])


@pytest.mark.parametrize('percentage,population,expected', [
    (0, 50, 0), (1, 1, 1), (10, 11, 2), (25, 4, 1), (10, 0, 0),
])
def test_percentage_count_uses_the_specification_ceiling_examples(percentage, population, expected):
    from micro_workflow_manager.sample_selection import sample_count

    assert sample_count(('percentage', percentage), population) == expected


@pytest.mark.parametrize('requested,population', [(0, 0), (0, 8), (3, 8), (8, 8)])
def test_count_selectors_keep_the_requested_number(requested, population):
    from micro_workflow_manager.sample_selection import sample_count

    assert sample_count(('count', requested), population) == requested


@pytest.mark.parametrize('requested,population', [(5, 4), (1, 0)])
def test_a_count_cannot_exceed_the_filtered_population(requested, population):
    from micro_workflow_manager.sample_selection import sample_count

    with pytest.raises(ValueError, match='population'):
        sample_count(('count', requested), population)


def test_status_filtering_precedes_percentage_count_calculation():
    from types import SimpleNamespace
    from micro_workflow_manager.sample_selection import filter_sample_population, sample_count

    population = tuple(SimpleNamespace(job_id=job_id, status='failed' if job_id <= 4 else 'done')
                       for job_id in range(1, 12))
    all_jobs = filter_sample_population(population)
    failed_jobs = filter_sample_population(population, ('failed',))

    assert all_jobs == list(population)
    assert [job.job_id for job in failed_jobs] == [1, 2, 3, 4]
    assert sample_count(('percentage', 25), len(all_jobs)) == 3
    assert sample_count(('percentage', 25), len(failed_jobs)) == 1


@pytest.mark.parametrize('selector,population', [
    (('count', -1), 4), (('percentage', -1), 4), (('percentage', 101), 4),
    (('count', 1.5), 4), (('percentage', 1.5), 4), (('count', True), 4),
    (('count', 0), -1), (('percentage', 25), 1.5), (('percentage', 25), -1),
])
def test_count_calculation_refuses_invalid_direct_inputs(selector, population):
    from micro_workflow_manager.sample_selection import sample_count

    with pytest.raises(ValueError):
        sample_count(selector, population)


def test_named_request_reports_a_missing_assignment():
    from micro_workflow_manager.sample_selection import parse_sample_selectors

    with pytest.raises(ValueError, match='assignment'):
        parse_sample_selectors('X', ('X', 'Y'), ['X=1', '2'])


@pytest.mark.parametrize('seed,node,offset,expected', [
    ('acceptance', 'X', 0, (3, 4, 5)), ('acceptance', 'Y', 0, (1, 4, 10)),
    ('other', 'X', 0, (1, 6, 7)), ('other', 'Y', 0, (8, 11, 12)),
    ('acceptance', 'X', 100, (103, 104, 105)),
])
def test_lowest_ranked_selection_matches_independent_v1_examples(seed, node, offset, expected):
    from micro_workflow_manager.sample_selection import select_sample_ids

    # Fixed results were calculated independently with .NET SHA-256. Identity
    # bytes come from the caller and can differ from the returned job identifier.
    candidates = {job_id + offset: str(job_id).encode('ascii') for job_id in range(12, 0, -1)}

    assert select_sample_ids(node, candidates, count=3, seed=seed) == expected


@pytest.mark.parametrize('count,seed', [(-1, 'seed'), (True, 'seed'), (3, 'seed'), (1, ''), (1, 'a\0b')])
def test_ranking_refuses_invalid_counts_or_ambiguous_seeds(count, seed):
    from micro_workflow_manager.sample_selection import select_sample_ids

    with pytest.raises(ValueError):
        select_sample_ids('X', {1: b'one', 2: b'two'}, count=count, seed=seed)


def test_ranking_refuses_duplicate_candidate_identities():
    from micro_workflow_manager.sample_selection import select_sample_ids

    with pytest.raises(ValueError, match='identity'):
        select_sample_ids('X', {1: b'same', 2: b'same'}, count=1, seed='seed')


@pytest.mark.parametrize('node', ['', 'X\0Y'])
def test_ranking_refuses_an_ambiguous_node_identity(node):
    from micro_workflow_manager.sample_selection import select_sample_ids

    with pytest.raises(ValueError, match='node'):
        select_sample_ids(node, {1: b'one'}, count=1, seed='seed')


@pytest.mark.parametrize('population,selected,expected', [
    ({'X': 0, 'Y': 4, 'Z': 0}, {'X': 0, 'Y': 4, 'Z': 0}, True),
    ({'X': 0, 'Y': 4, 'Z': 0}, {'X': 0, 'Y': 3, 'Z': 0}, False),
    ({'X': 0, 'Y': 0, 'Z': 0}, {'X': 0, 'Y': 0, 'Z': 0}, False),
])
def test_starting_coverage_requires_some_work_and_every_nonempty_member(population, selected, expected):
    from micro_workflow_manager.sample_selection import starting_population_is_fully_selected

    assert starting_population_is_fully_selected(population, selected) is expected


@pytest.mark.parametrize('population,selected', [
    ({'X': 1, 'Y': 0}, {'X': 1}), ({'X': 1}, {'X': 1, 'Y': 0}),
    ({'X': -1}, {'X': -1}), ({'X': 1}, {'X': 2}),
    ({'X': 1.5}, {'X': 1}), ({'X': 1}, {'X': True}),
])
def test_starting_coverage_refuses_mismatched_members_or_invalid_counts(population, selected):
    from micro_workflow_manager.sample_selection import starting_population_is_fully_selected

    with pytest.raises(ValueError):
        starting_population_is_fully_selected(population, selected)


def test_named_percentage_example_calculates_each_member_independently():
    from micro_workflow_manager.sample_selection import parse_sample_selectors, sample_count

    selectors = parse_sample_selectors('X', ('X', 'Y', 'Z'), ['X=10%', 'Y=25%'])
    populations = {'X': 11, 'Y': 4, 'Z': 100}

    assert {node: sample_count(selector, populations[node])
            for node, selector in selectors.items()} == {'X': 2, 'Y': 1, 'Z': 0}


def test_percentage_rounding_keeps_precision_above_floating_point_integer_range():
    from micro_workflow_manager.sample_selection import sample_count

    assert sample_count(('percentage', 1), 1_000_000_000_000_000_001) == 10_000_000_000_000_001
    assert sample_count(('percentage', 100), 11) == 11


def test_repeated_samples_can_overlap_and_do_not_change_the_population():
    from micro_workflow_manager.sample_selection import select_sample_ids

    candidates = {job_id: str(job_id).encode('ascii') for job_id in range(1, 13)}
    before = candidates.copy()

    assert select_sample_ids('X', candidates, count=3, seed='acceptance') == (3, 4, 5)
    assert select_sample_ids('X', dict(reversed(tuple(candidates.items()))),
                             count=3, seed='acceptance') == (3, 4, 5)
    assert select_sample_ids('X', candidates, count=3, seed='acceptance') == (3, 4, 5)
    assert candidates == before


@pytest.mark.parametrize('candidates,count,expected', [
    ({}, 0, ()), ({5: b'five', 2: b'two'}, 0, ()),
    ({5: b'five', 2: b'two'}, 2, (2, 5)),
])
def test_zero_and_full_selection_preserve_population_boundaries(candidates, count, expected):
    from micro_workflow_manager.sample_selection import select_sample_ids

    assert select_sample_ids('X', candidates, count=count, seed='seed') == expected


def test_no_matching_status_leaves_no_starting_work():
    from types import SimpleNamespace
    from micro_workflow_manager.sample_selection import filter_sample_population, sample_count

    candidates = [SimpleNamespace(job_id=1, status='done')]
    assert filter_sample_population(candidates, ('failed',)) == []
    assert sample_count(('percentage', 100), 0) == 0


@pytest.mark.parametrize('population,selected,expected', [
    ({}, {}, False), ({'X': 2, 'Y': 3}, {'Y': 3, 'X': 2}, True),
    ({'X': 2, 'Y': 3}, {'Y': 3, 'X': 0}, False),
])
def test_starting_coverage_includes_all_populated_members(population, selected, expected):
    from micro_workflow_manager.sample_selection import starting_population_is_fully_selected

    assert starting_population_is_fully_selected(population, selected) is expected
