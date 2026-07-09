from __future__ import annotations

import json
from pathlib import Path
from typing import Any

VSCODE_EXCLUDES = {
    "**/*.egg-info": True,
    "**/__pycache__": True,
    "**/.pytest_cache": True,
    "**/.mwf_locks": True,
    "**/.mwf_run.json": True,
}

GITIGNORE_SECTION_START = "# >>> micro-workflow-manager generated state >>>"
GITIGNORE_SECTION_END = "# <<< micro-workflow-manager generated state <<<"
GITIGNORE_ENTRIES = [
    GITIGNORE_SECTION_START,
    "# Runtime state and scheduler locks",
    ".mwf_locks/",
    ".mwf_run.json",
    "",
    "# Node runtime folders. Keep direct input/output files, but ignore nested directories.",
    "node/*/input/*/",
    "node/*/jobs/**",
    "node/*/output/*/",
    "node/*/queued/**",
    "",
    "# Rebuildable node metadata and caches",
    "node/*/node_state.json",
    "node/*/job_index.json",
    "node/*/job_index.dirty",
    "node/*/default_jobs.json",
    "node/*/schema.json",
    "",
    "# Python/editor/cache files",
    "__pycache__/",
    "*.py[cod]",
    ".pytest_cache/",
    ".mypy_cache/",
    ".ruff_cache/",
    ".coverage",
    "htmlcov/",
    "*.egg-info/",
    ".venv/",
    "venv/",
    ".env",
    ".env.*",
    ".DS_Store",
    GITIGNORE_SECTION_END,
]


def ensure_project_sidecars(root: Path):
    """Create or update editor/git hygiene files for an mwf project."""

    ensure_vscode_settings(root)
    ensure_gitignore(root)


def ensure_vscode_settings(root: Path):
    vscode_dir = root / ".vscode"
    vscode_dir.mkdir(parents=True, exist_ok=True)
    settings_path = vscode_dir / "settings.json"

    settings: dict[str, Any] = {}
    if settings_path.exists():
        settings = json.loads(settings_path.read_text(encoding="utf-8") or "{}")
        if not isinstance(settings, dict):
            raise ValueError(f"Expected VS Code settings object: {settings_path}")

    for key in ["files.exclude", "search.exclude"]:
        current = settings.get(key)
        if not isinstance(current, dict):
            current = {}
        current.update(VSCODE_EXCLUDES)
        settings[key] = current

    settings_path.write_text(
        json.dumps(settings, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


def ensure_gitignore(root: Path):
    path = root / ".gitignore"
    section = "\n".join(GITIGNORE_ENTRIES) + "\n"

    if not path.exists():
        path.write_text(section, encoding="utf-8")
        return

    text = path.read_text(encoding="utf-8")
    start = text.find(GITIGNORE_SECTION_START)
    end = text.find(GITIGNORE_SECTION_END)

    if start != -1 and end != -1 and end >= start:
        end += len(GITIGNORE_SECTION_END)
        new_text = text[:start].rstrip() + "\n\n" + section + text[end:].lstrip("\n")
    else:
        prefix = text.rstrip()
        new_text = (prefix + "\n\n" if prefix else "") + section

    path.write_text(new_text, encoding="utf-8")
