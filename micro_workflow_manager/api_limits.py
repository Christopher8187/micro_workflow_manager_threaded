from __future__ import annotations


def allocate_api_capacity(
    requested: dict[str, int],
    configured_total: int | None,
) -> dict[str, int]:
    """Allocate an aggregate API admission budget over active nodes.

    Requests are weights as well as hard upper bounds. Every active node keeps
    one slot, and capacity released by a completed node is redistributed across
    the nodes that are still running.
    """
    checked = {name: max(1, int(value)) for name, value in requested.items()}
    if not checked:
        return {}

    requested_total = sum(checked.values())
    if configured_total is None or configured_total >= requested_total:
        return checked

    target = max(len(checked), int(configured_total))
    shares = {
        name: max(1, (target * value) // requested_total)
        for name, value in checked.items()
    }
    used = sum(shares.values())
    remainders = sorted(
        checked,
        key=lambda name: (
            -((target * checked[name]) % requested_total),
            name,
        ),
    )
    while used < target:
        changed = False
        for name in remainders:
            if shares[name] >= checked[name]:
                continue
            shares[name] += 1
            used += 1
            changed = True
            if used == target:
                break
        if not changed:
            break
    while used > target:
        changed = False
        for name in reversed(remainders):
            if shares[name] <= 1:
                continue
            shares[name] -= 1
            used -= 1
            changed = True
            if used == target:
                break
        if not changed:
            break
    return shares
