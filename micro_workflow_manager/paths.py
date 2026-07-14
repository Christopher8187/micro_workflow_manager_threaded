from __future__ import annotations

from pathlib import Path

MWF_DIR_NAME = ".mwf"
MWF_CONFIG_NAME = "project.json"
MWF_RUN_NAME = "run.json"
MWF_THREADS_NAME = "threads.json"
MWF_LOCKS_NAME = "locks"
MWF_DEPLOY_NAME = "deploy"

LEGACY_CONFIG_NAME = ".mwf"
LEGACY_RUN_NAME = ".mwf_run.json"
LEGACY_THREADS_NAME = ".mwf_threads.json"
LEGACY_LOCKS_NAME = ".mwf_locks"


def mwf_dir(root: Path) -> Path:
    return root / MWF_DIR_NAME


def config_file(root: Path) -> Path:
    return mwf_dir(root) / MWF_CONFIG_NAME


def run_file(root: Path) -> Path:
    return mwf_dir(root) / MWF_RUN_NAME


def threads_file(root: Path) -> Path:
    return mwf_dir(root) / MWF_THREADS_NAME


def locks_dir(root: Path) -> Path:
    return mwf_dir(root) / MWF_LOCKS_NAME


def deploy_dir(root: Path) -> Path:
    return mwf_dir(root) / MWF_DEPLOY_NAME
