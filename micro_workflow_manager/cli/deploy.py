from __future__ import annotations

from pathlib import Path

from .deploy_archive import (
    DeploymentStats,
    _build_staging,
    _format_bytes,
    _included_node_entries,
    _zip_selected_tree,
    _zip_tree,
    deploy_local,
    local_archive_path,
)
from .deploy_config import (
    ServerConfig,
    _SetupArgs,
    _choice,
    _confirm,
    _expanded_path,
    _find_putty_tool,
    _optional_text,
    _positive_port,
    _prompt,
    _prompt_choice,
    _required_text,
    _resolve_command,
    deploy_setup,
    read_server_config,
    server_config_path,
    validate_server_config,
)
from .deploy_remote import (
    _display_arg,
    _extract_remote,
    _remote_extraction_script,
    _run_visible,
    _upload_archive,
    deploy_remote,
)


def deploy_command(root: Path, args) -> int:
    action = args.deploy_command
    if action == "setup":
        return deploy_setup(root, args)
    if action == "local":
        return deploy_local(root)
    if action == "remote":
        return deploy_remote(root, args)
    raise RuntimeError("Use: mwf deploy setup|local|remote")


__all__ = [
    "DeploymentStats",
    "ServerConfig",
    "deploy_command",
    "deploy_local",
    "deploy_remote",
    "deploy_setup",
    "local_archive_path",
    "read_server_config",
    "server_config_path",
    "validate_server_config",
]
