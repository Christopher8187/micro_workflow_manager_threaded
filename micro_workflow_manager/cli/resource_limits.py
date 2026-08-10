from __future__ import annotations

import os

TARGET_NOFILE = 65_536


def raise_open_file_limit(target: int = TARGET_NOFILE) -> int | None:
    """Best-effort raise of this process' soft RLIMIT_NOFILE on POSIX.

    The function never lowers an existing limit. If the hard limit is below the
    requested target and cannot itself be raised, the soft limit is raised as
    far as the current hard limit permits. Windows has no RLIMIT_NOFILE and is
    intentionally left unchanged.
    """
    if type(target) is not int or target < 1:
        raise ValueError("target must be an integer >= 1")
    if os.name == "nt":
        return None
    try:
        import resource
    except ImportError:
        return None

    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    if soft == resource.RLIM_INFINITY or soft >= target:
        return int(soft) if soft != resource.RLIM_INFINITY else target

    desired = target
    if hard != resource.RLIM_INFINITY and hard < desired:
        # Raising the hard limit may succeed for privileged/service contexts.
        try:
            resource.setrlimit(resource.RLIMIT_NOFILE, (desired, desired))
            return desired
        except (OSError, ValueError, PermissionError):
            desired = int(hard)

    if desired > soft:
        resource.setrlimit(resource.RLIMIT_NOFILE, (desired, hard))
    new_soft, _ = resource.getrlimit(resource.RLIMIT_NOFILE)
    return int(new_soft) if new_soft != resource.RLIM_INFINITY else target
