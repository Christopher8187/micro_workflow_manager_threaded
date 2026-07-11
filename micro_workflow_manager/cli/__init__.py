"""Command-line interface package for micro-workflow-manager."""

from __future__ import annotations

import sys
from importlib import import_module


def _dispatch_main(argv: list[str] | None = None) -> int:
    resolved = list(sys.argv[1:] if argv is None else argv)

    # Keep the active-job control path intentionally light. Importing the full
    # workflow CLI loads graph scheduling and networkx before it can touch the
    # job. The dedicated restart parser only needs file storage and writes the
    # execution fence first.
    if resolved and resolved[0] == "restart":
        restart_module = import_module(".restart", __name__)
        return restart_module.restart_cli(resolved[1:])

    full_module = import_module(".main", __name__)
    # Importing a child module named ``main`` makes Python assign that module to
    # this package's ``main`` attribute. Restore the longstanding callable API
    # used by ``from micro_workflow_manager import cli; cli.main([...])``.
    globals()["main"] = _dispatch_main
    return full_module.main(resolved)


main = _dispatch_main

__all__ = ["main"]
