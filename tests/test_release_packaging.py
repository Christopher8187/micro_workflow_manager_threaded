from __future__ import annotations

import shutil
import tarfile
from pathlib import Path

import setuptools.build_meta as build_backend


def test_source_distribution_includes_project_guidance_and_excludes_build_state(
    tmp_path, monkeypatch
):
    root = Path(__file__).parents[1]
    source = tmp_path / "source"
    shutil.copytree(
        root,
        source,
        ignore=shutil.ignore_patterns(
            ".git",
            ".venv",
            ".pytest_cache",
            "__pycache__",
            "build",
            "dist",
            "release-dist*",
            "*.egg-info",
            "*.pyc",
            "*.pyo",
            "PROTOTYPE*.html",
        ),
    )

    prototype = source / "examples" / "PROTOTYPE-sentinel.html"
    prototype.write_text("must not ship", encoding="utf-8")
    cache = source / "micro_workflow_manager" / "__pycache__"
    cache.mkdir()
    (cache / "sentinel.pyc").write_bytes(b"must not ship")

    output = tmp_path / "dist"
    output.mkdir()
    monkeypatch.chdir(source)
    archive_name = build_backend.build_sdist(str(output))

    with tarfile.open(output / archive_name, "r:gz") as archive:
        members = {member.name for member in archive.getmembers()}

    archive_root = archive_name.removesuffix(".tar.gz")
    expected = {
        Path("AGENTS.md"),
        Path("CONTEXT.md"),
        Path("MANIFEST.in"),
        Path("README.md"),
        *(path.relative_to(root) for path in (root / ".agents").rglob("*.md")),
        *(path.relative_to(root) for path in (root / "docs").rglob("*.md")),
        *(
            path.relative_to(root)
            for path in (root / "examples").rglob("*")
            if path.suffix in {".md", ".py"}
        ),
        *(
            path.relative_to(root)
            for path in (root / "benchmarks").rglob("*")
            if path.suffix in {".md", ".py", ".json", ".jsonl"}
        ),
        *(
            path.relative_to(root)
            for path in (root / "tests").rglob("*")
            if path.suffix in {".md", ".py"}
        ),
    }
    expected_members = {f"{archive_root}/{path.as_posix()}" for path in expected}

    assert expected_members <= members
    assert not any("PROTOTYPE" in member for member in members)
    assert not any("__pycache__" in member for member in members)
    assert not any(member.endswith((".pyc", ".pyo")) for member in members)
