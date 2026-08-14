from __future__ import annotations

from threading import Event


def wait_for_live_component_release(
    ready_event: Event | None,
    start_event: Event | None,
    stop_event: Event | None,
) -> None:
    """Attach every live member before sibling component work is released."""
    if ready_event is not None:
        ready_event.set()
    if start_event is None:
        return
    while not start_event.wait(0.05):
        if stop_event is not None and stop_event.is_set():
            return
