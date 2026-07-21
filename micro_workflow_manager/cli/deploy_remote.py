from __future__ import annotations

import base64
import json
import shlex
import subprocess
import uuid
from pathlib import Path

from .deploy_archive import _format_bytes, deploy_local, local_archive_path
from .deploy_config import (
    ServerConfig,
    _SetupArgs,
    _confirm,
    _expanded_path,
    _prompt,
    _resolve_command,
    deploy_setup,
    read_server_config,
    validate_server_config,
)


def _upload_archive(config: ServerConfig, archive: Path, remote_temp: str) -> None:
    target = f"{config.user}@{config.host}:{remote_temp}"
    if config.tool == "putty":
        command = [_resolve_command(config.pscp_path) or str(config.pscp_path), "-P", str(config.port)]
        if config.key_path:
            command.extend(["-i", _expanded_path(config.key_path)])
        command.extend([str(archive), target])
    else:
        command = ["scp", "-P", str(config.port)]
        if config.key_path:
            command.extend(["-i", _expanded_path(config.key_path)])
        command.extend([str(archive), target])
    _run_visible(command, "archive upload")

def _extract_remote(config: ServerConfig, remote_temp: str, remote_path: str) -> None:
    script = _remote_extraction_script(remote_temp, remote_path)
    encoded = base64.b64encode(script.encode("utf-8")).decode("ascii")
    bootstrap = f"import base64;exec(base64.b64decode('{encoded}'))"
    remote_command = f"{config.python_command} -c {shlex.quote(bootstrap)}"
    destination = f"{config.user}@{config.host}"
    if config.tool == "putty":
        command = [_resolve_command(config.plink_path) or str(config.plink_path), "-P", str(config.port)]
        if config.key_path:
            command.extend(["-i", _expanded_path(config.key_path)])
        command.extend([destination, remote_command])
    else:
        command = ["ssh", "-p", str(config.port)]
        if config.key_path:
            command.extend(["-i", _expanded_path(config.key_path)])
        command.extend([destination, remote_command])
    _run_visible(command, "remote extraction")

def _remote_extraction_script(remote_temp: str, remote_path: str) -> str:
    payload = base64.b64encode(json.dumps({"archive": remote_temp, "target": remote_path}).encode("utf-8")).decode("ascii")
    return f'''
import base64, json, shutil, zipfile
from pathlib import Path
cfg = json.loads(base64.b64decode({payload!r}).decode("utf-8"))
archive = Path(cfg["archive"]).expanduser()
target = Path(cfg["target"]).expanduser()
target.mkdir(parents=True, exist_ok=True)

def safe_extract(handle, destination):
    base = destination.resolve()
    for member in handle.infolist():
        candidate = (destination / member.filename).resolve()
        candidate.relative_to(base)
    handle.extractall(destination)

print("[mwf remote] extracting main deployment archive", flush=True)
with zipfile.ZipFile(archive, "r") as handle:
    safe_extract(handle, target)
node_dir = target / "node"
if node_dir.is_dir():
    archives = sorted(node_dir.glob("*.zip"))
    for index, node_archive in enumerate(archives, 1):
        node_target = node_dir / node_archive.stem
        node_target.mkdir(parents=True, exist_ok=True)
        print(f"[mwf remote] extracting node {{index}}/{{len(archives)}}: {{node_archive.stem}}", flush=True)
        with zipfile.ZipFile(node_archive, "r") as handle:
            safe_extract(handle, node_target)
        empty_marker = node_target / ".mwf-empty"
        if empty_marker.exists():
            empty_marker.unlink()
        node_archive.unlink()
archive.unlink(missing_ok=True)
print(f"[mwf remote] deployment ready at {{target}}", flush=True)
'''.strip()

def _run_visible(command: list[str], label: str) -> None:
    print("  command: " + " ".join(_display_arg(item) for item in command))
    try:
        result = subprocess.run(command, check=False)
    except OSError as error:
        raise RuntimeError(f"Could not start {label}: {error}") from error
    if result.returncode != 0:
        raise RuntimeError(f"{label} failed with exit code {result.returncode}")

def _display_arg(value: str) -> str:
    if " " in value or "\t" in value:
        return f'"{value}"'
    return value

def deploy_remote(root: Path, args) -> int:
    archive = local_archive_path(root)
    if not archive.is_file():
        print("No local deployment archive exists.")
        if not _confirm("Build it now with 'mwf deploy local'?", default=True):
            print("Remote deployment cancelled. Run 'mwf deploy local' first.")
            return 1
        deploy_local(root)
    else:
        print(f"Found local deployment: {archive} ({_format_bytes(archive.stat().st_size)})")
        if not args.yes and not _confirm("Deploy this existing local archive to the server?", default=True):
            if not _confirm("Build a fresh local deployment first?", default=True):
                print("Remote deployment cancelled.")
                return 1
            deploy_local(root)
            archive = local_archive_path(root)
            if not _confirm("Deploy the freshly built archive now?", default=True):
                print("Remote deployment cancelled.")
                return 1

    config = read_server_config(root, required=False)
    if config is None:
        print("No deployment server setup exists.")
        if not _confirm("Run 'mwf deploy setup' now?", default=True):
            print("Remote deployment cancelled. Run 'mwf deploy setup' first.")
            return 1
        setup_args = _SetupArgs()
        deploy_setup(root, setup_args)
        config = read_server_config(root, required=True)

    assert config is not None
    validate_server_config(config, require_tools=True)
    remote_path = args.path or _prompt("Destination path on the server", None)
    if not remote_path.strip():
        raise RuntimeError("Remote destination path cannot be empty")

    remote_temp = f"/tmp/mwf-deployment-{uuid.uuid4().hex}.zip"
    print("Remote deployment plan")
    print(f"  server: {config.user}@{config.host}:{config.port}")
    print(f"  remote path: {remote_path}")
    print(f"  transfer method: {config.tool}")
    print(f"  local archive: {archive}")
    print("Uploading compressed deployment...")
    _upload_archive(config, archive, remote_temp)
    print("Upload complete. Extracting project files and node archives on the server...")
    _extract_remote(config, remote_temp, remote_path)
    print("Remote deployment complete")
    print(f"  deployed to: {config.user}@{config.host}:{remote_path}")
    print("  existing files with matching paths were overwritten; unrelated remote files were left in place")
    return 0
