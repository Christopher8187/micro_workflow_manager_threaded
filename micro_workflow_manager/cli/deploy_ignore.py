from __future__ import annotations

import fnmatch
from dataclasses import dataclass
from pathlib import Path

DEFAULT_MWFIGNORE = """# MWF deployment ignore rules
# Review this file before the first deployment. Rules follow the familiar
# .gitignore/.dockerignore style; later rules override earlier rules and !
# re-includes a path.

# Version-control and editor metadata
.git/
.github/
.gitignore
.gitattributes
.vscode/
.idea/

# MWF runtime/deployment state and local node clipboard
.mwf/
clipboard/

# Python environments, caches, and build output
.venv/
venv/
__pycache__/
*.py[cod]
.pytest_cache/
.mypy_cache/
.ruff_cache/
.coverage
htmlcov/
*.egg-info/
build/
dist/

# Local secrets and operating-system clutter
.env
.env.*
.DS_Store
Thumbs.db
"""


@dataclass(frozen=True)
class IgnoreRule:
    pattern: str
    negated: bool
    directory_only: bool
    anchored: bool

    def matches(self, relative_path: str, *, is_dir: bool) -> bool:
        path = relative_path.strip("/")
        if not path:
            return False

        pattern = self.pattern.strip("/")
        if not pattern:
            return False

        if self.anchored or "/" in pattern:
            matched = fnmatch.fnmatchcase(path, pattern)
            if self.directory_only and not self.negated:
                matched = matched or _directory_prefix_match(path, pattern)
            return matched and (is_dir or not self.directory_only or not self.negated)

        parts = path.split("/")
        if self.directory_only:
            if self.negated:
                # Re-including a directory makes traversal possible, but does
                # not implicitly re-include every file below it. Use !dir/**
                # when the contents should also be deployed.
                return is_dir and fnmatch.fnmatchcase(parts[-1], pattern)
            # A positive directory rule such as .git/ ignores that directory
            # wherever it occurs and every descendant below it.
            return any(fnmatch.fnmatchcase(part, pattern) for part in parts[:-1] + ([parts[-1]] if is_dir else []))
        return any(fnmatch.fnmatchcase(part, pattern) for part in parts)


def _directory_prefix_match(path: str, pattern: str) -> bool:
    parts = path.split("/")
    for length in range(1, len(parts) + 1):
        prefix = "/".join(parts[:length])
        if fnmatch.fnmatchcase(prefix, pattern):
            return True
    return False


class MWFIgnore:
    def __init__(self, rules: list[IgnoreRule]):
        self.rules = rules

    @classmethod
    def from_text(cls, text: str) -> "MWFIgnore":
        rules: list[IgnoreRule] = []
        for raw_line in text.splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            negated = line.startswith("!")
            if negated:
                line = line[1:]
            if not line:
                continue
            line = line.replace("\\", "/")
            anchored = line.startswith("/")
            directory_only = line.endswith("/")
            rules.append(
                IgnoreRule(
                    pattern=line.strip("/"),
                    negated=negated,
                    directory_only=directory_only,
                    anchored=anchored,
                )
            )
        return cls(rules)

    @classmethod
    def from_file(cls, path: Path) -> "MWFIgnore":
        return cls.from_text(path.read_text(encoding="utf-8"))

    def is_ignored(self, relative_path: str | Path, *, is_dir: bool = False) -> bool:
        path = Path(relative_path).as_posix()
        while path.startswith("./"):
            path = path[2:]
        path = path.lstrip("/")
        ignored = False
        for rule in self.rules:
            if rule.matches(path, is_dir=is_dir):
                ignored = not rule.negated
        return ignored


def ensure_mwfignore(root: Path) -> tuple[Path, bool]:
    path = root / ".mwfignore"
    created = False
    if not path.exists():
        path.write_text(DEFAULT_MWFIGNORE, encoding="utf-8")
        created = True
    elif not path.is_file():
        raise RuntimeError(f"Expected .mwfignore to be a file: {path}")
    return path, created
