from __future__ import annotations

import json
from pathlib import Path
from typing import Any

VSCODE_EXCLUDES = {
    "**/*.egg-info": True,
    "**/__pycache__": True,
    "**/.pytest_cache": True,
    "**/.mwf": True,
}


MATERIAL_FOLDER_ASSOCIATIONS = {
    "clipboard": "archive",
    "node_behavior": "flow",
    "utils": "tools",
    "input": "input",
    "output": "export",
    "jobs": "tasks",
    "queued": "queue",
    "idempotency": "keys",
    "files": "resource",
}

MATERIAL_FILE_ASSOCIATIONS = {
    ".mwfignore": "routing",
    "graph.py": "routing",
}


GITIGNORE_SECTION_START = "# >>> micro-workflow-manager generated state >>>"
GITIGNORE_SECTION_END = "# <<< micro-workflow-manager generated state <<<"
GITIGNORE_ENTRIES = [
    GITIGNORE_SECTION_START,
    "# Consolidated MWF runtime, locks, local deployment archives, and server setup",
    ".mwf/",
    "",
    "# Node runtime folders. Keep direct input/output files, but ignore nested directories.",
    "node/*/input/*/",
    "node/*/jobs/**",
    "node/*/output/*/",
    "node/*/queued/**",       # legacy 0.3.3 queue markers
    "node/*/idempotency/**",  # legacy 0.3.3 idempotency records
    "",
    "# Generated schema and legacy framework metadata",
    "node/*/schema.json",
    "node/*/node_state.json",
    "node/*/job_index.json",
    "node/*/job_index.dirty",
    "node/*/default_jobs.json",
    "",
    "# Clipboard node runtime folders. Keep direct input/output files, but ignore nested directories.",
    "clipboard/*/input/*/",
    "clipboard/*/jobs/**",
    "clipboard/*/output/*/",
    "clipboard/*/queued/**",       # legacy 0.3.3 queue markers
    "clipboard/*/idempotency/**",  # legacy 0.3.3 idempotency records
    "",
    "# Clipboard schema, legacy metadata, and cold SQLite state snapshot",
    "clipboard/*/schema.json",
    "clipboard/*/node_state.json",
    "clipboard/*/job_index.json",
    "clipboard/*/job_index.dirty",
    "clipboard/*/default_jobs.json",
    "clipboard/*/.mwf-node-state.sqlite3",
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


def ensure_vscode_settings(
    root: Path,
    *,
    node_names: set[str] | None = None,
    previous_node_names: set[str] | None = None,
):
    vscode_dir = root / ".vscode"
    vscode_dir.mkdir(parents=True, exist_ok=True)
    settings_path = vscode_dir / "settings.json"

    settings: dict[str, Any] = {}
    if settings_path.exists():
        settings = json.loads(settings_path.read_text(encoding="utf-8") or "{}")
        if not isinstance(settings, dict):
            raise ValueError(f"Expected VS Code settings object: {settings_path}")

    settings["workbench.iconTheme"] = "material-icon-theme"

    file_associations = settings.get("material-icon-theme.files.associations")
    if not isinstance(file_associations, dict):
        file_associations = {}
    file_associations.update(MATERIAL_FILE_ASSOCIATIONS)
    settings["material-icon-theme.files.associations"] = file_associations

    folder_associations = settings.get("material-icon-theme.folders.associations")
    if not isinstance(folder_associations, dict):
        folder_associations = {}
    folder_associations.update(MATERIAL_FOLDER_ASSOCIATIONS)
    # 0.3.4 deliberately leaves the top-level node folder on the theme's
    # native icon. Remove the old generated association during upgrades.
    folder_associations.pop("node", None)

    # Material Icon Theme associations are exact folder names rather than path
    # wildcards. Map graph nodes and every direct node/clipboard child by name.
    # This also covers saved clipboard snapshots that are no longer in the
    # current graph without inventing path-specific wildcard settings.
    associated_node_names = set(node_names or set())
    for parent_name in ("node", "clipboard"):
        parent = root / parent_name
        if parent.is_dir():
            associated_node_names.update(
                child.name
                for child in parent.iterdir()
                if child.is_dir() and not child.name.startswith(".")
            )
    for old_name in previous_node_names or set():
        if old_name not in associated_node_names and folder_associations.get(old_name) == "flow":
            folder_associations.pop(old_name, None)
    for node_name in associated_node_names:
        folder_associations[node_name] = "flow"
    settings["material-icon-theme.folders.associations"] = folder_associations

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
