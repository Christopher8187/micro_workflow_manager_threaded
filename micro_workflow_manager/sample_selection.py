from __future__ import annotations

from collections.abc import Collection, Iterable, Mapping, Sequence
from hashlib import sha256
from heapq import nsmallest
from typing import Literal


SampleSelector = tuple[Literal['count', 'percentage'], int]


def parse_sample_selectors(
    start_node: str, component_members: Sequence[str], tokens: Sequence[str],
) -> dict[str, SampleSelector]:
    """Calculate one selector per member from string request tokens.

    The caller supplies the exact component as a reusable sequence of unique
    raw-node names. This calculation does not discover or validate graph state.
    """
    if start_node not in component_members:
        raise ValueError('Sample start node is outside the starting component')
    if not tokens or ('=' not in tokens[0] and len(tokens) != 1):
        raise ValueError('Use one shorthand selector or named component-member assignments')
    if '=' in tokens[0] and any('=' not in token for token in tokens):
        raise ValueError('Use a named assignment for every requested member')
    assignments = [(start_node, tokens[0])] if '=' not in tokens[0] else [
        token.rsplit('=', 1) for token in tokens]
    kind: Literal['count', 'percentage'] = 'percentage' if assignments[0][1].endswith('%') else 'count'
    selectors = {member: (kind, 0) for member in component_members}
    assigned = set()
    for member, token in assignments:
        if member not in selectors:
            raise ValueError('Sample assignment is outside the starting component: ' + member)
        if member in assigned:
            raise ValueError('Duplicate sample assignment: ' + member)
        assigned.add(member)
        if token.endswith('%') != (kind == 'percentage'):
            raise ValueError('Use one selector kind throughout a sample request')
        value = int(token[:-1] if kind == 'percentage' else token)
        if value < 0 or (kind == 'percentage' and value > 100):
            raise ValueError('Sample counts must be nonnegative and percentages must be between 0 and 100')
        selectors[member] = (kind, value)
    return selectors


def sample_count(selector: SampleSelector, population_size: int) -> int:
    """Calculate an exact sample count from an already filtered population."""
    kind, value = selector
    if type(population_size) is not int or population_size < 0:
        raise ValueError('Sample population must be a nonnegative integer')
    if type(value) is not int or value < 0 or (kind == 'percentage' and value > 100):
        raise ValueError('Sample counts must be nonnegative integers and percentages must be between 0 and 100')
    if kind == 'count':
        if value > population_size:
            raise ValueError(f'Sample count {value} exceeds filtered population {population_size}')
        return value
    if kind == 'percentage':
        return (value * population_size + 99) // 100
    raise ValueError('Unsupported sample selector kind: ' + kind)


def filter_sample_population(candidates: Iterable, statuses: Collection[str] = ()) -> list:
    """Retain candidates with a string ``status`` matching the supplied filter.

    The caller normalizes the reusable status collection. An empty collection
    means no filter. Returned candidates retain their input order and values.
    """
    return [candidate for candidate in candidates if not statuses or candidate.status in statuses]


def select_sample_ids(
    node: str, candidates: Mapping[int, bytes], *, count: int, seed: str,
) -> tuple[int, ...]:
    """Select v1 ranks using identity bytes supplied independently of job IDs."""
    sample_count(('count', count), len(candidates))
    if not seed or '\0' in seed:
        raise ValueError('Sample seed must be nonempty and contain no NUL characters')
    if not node or '\0' in node:
        raise ValueError('Sample node must be nonempty and contain no NUL characters')
    if len(set(candidates.values())) != len(candidates):
        raise ValueError('Each sample candidate must have distinct identity bytes')
    prefix = b'mwf.sample.v1\0' + seed.encode('utf-8') + b'\0' + node.encode('utf-8') + b'\0'
    selected = nsmallest(count, candidates,
                        key=lambda job_id: (sha256(prefix + candidates[job_id]).digest(), job_id))
    return tuple(sorted(selected))


def starting_population_is_fully_selected(
    population_counts: Mapping[str, int], selected_counts: Mapping[str, int],
) -> bool:
    """Check valid counts for the same members without claiming execution success."""
    if population_counts.keys() != selected_counts.keys():
        raise ValueError('Starting population and selection must name the same members')
    for node, count in population_counts.items():
        sample_count(('count', selected_counts[node]), count)
    return any(selected_counts.values()) and all(
        selected_counts[node] == count for node, count in population_counts.items())
