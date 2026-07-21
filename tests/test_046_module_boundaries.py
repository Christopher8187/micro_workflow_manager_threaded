from __future__ import annotations

from pathlib import Path


def test_only_cohesive_fiber_runtime_exceeds_500_lines():
    package_root = Path(__file__).parents[1] / "micro_workflow_manager"
    oversized = {
        path.relative_to(package_root).as_posix()
        for path in package_root.rglob("*.py")
        if len(path.read_text(encoding="utf-8").splitlines()) > 500
    }
    assert oversized == {"fibers.py"}
