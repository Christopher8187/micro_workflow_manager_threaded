"""Pure readiness decisions from direct-parent component observations."""

from __future__ import annotations

from collections.abc import Iterable


def calculate_component_readiness(
    parent_observations: Iterable[tuple[str, str | None, str | None]],
    *,
    interrupt_start_origin: str | None = None,
) -> tuple[str, str | None, bool] | None:
    """Return compatible success lineage, or None when ordinary work is blocked.

    Each parent supplies lifecycle, stability, and exact instability origin.
    A ready result supplies stability, origin, and whether the override was used.
    The caller must authorize any interrupt override for the named start only.
    This calculation neither observes storage nor publishes a lifecycle result.
    """
    if interrupt_start_origin is not None and (
        not isinstance(interrupt_start_origin, str) or not interrupt_start_origin
    ):
        raise ValueError('invalid interrupt start origin')
    blocked_result = (
        ('unstable', interrupt_start_origin, True)
        if interrupt_start_origin is not None else None
    )
    result = None
    blocked = False
    for lifecycle, stability, origin in parent_observations:
        if lifecycle not in ('queued', 'running', 'sampled', 'done', 'failed'):
            raise ValueError('unknown component lifecycle')
        if lifecycle != 'done':
            blocked = True
            continue
        if not (
            (stability == 'stable' and origin is None)
            or (stability == 'unstable' and isinstance(origin, str) and origin != '')
        ):
            raise ValueError('invalid done component result')
        parent_result = stability, origin, False
        if result is not None and result != parent_result:
            blocked = True
        result = parent_result
    if blocked:
        return blocked_result
    return result if result is not None else ('stable', None, False)
