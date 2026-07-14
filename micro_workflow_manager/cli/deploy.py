from __future__ import annotations

import base64
import json
import os
import shlex
import shutil
import subprocess
import sys
import uuid
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from micro_workflow_manager.paths import deploy_dir

from .deploy_ignore import MWFIgnore, ensure_mwfignore


@dataclass(frozen=True)
class DeploymentStats:
    copied_files: int = 0
    copied_bytes: int = 0
    node_archives: int = 0
    ignored_paths: int = 0
    skipped_links: int = 0


@dataclass(frozen=True)
class ServerConfig:
    host: str
    user: str
    port: int
    auth: str
    tool: str
    key_path: str | None = None
    pscp_path: str | None = None
    plink_path: str | None = None
    python_command: str = "python3"

    @classmethod
    def from_dict(cls, data: dict) -> "ServerConfig":
        if not isinstance(data, dict):
            raise RuntimeError("Deployment server configuration must be a JSON object")
        return cls(
            host=_required_text(data, "host"),
            user=_required_text(data, "user"),
            port=_positive_port(data.get("port", 22)),
            auth=_choice(data.get("auth", "key"), "auth", {"password", "key"}),
            tool=_choice(data.get("tool", "openssh"), "tool", {"putty", "openssh"}),
            key_path=_optional_text(data.get("key_path")),
            pscp_path=_optional_text(data.get("pscp_path")),
            plink_path=_optional_text(data.get("plink_path")),
            python_command=_optional_text(data.get("python_command")) or "python3",
        )

    def as_dict(self) -> dict:
        return {
            "host": self.host,
            "user": self.user,
            "port": self.port,
            "auth": self.auth,
            "tool": self.tool,
            "key_path": self.key_path,
            "pscp_path": self.pscp_path,
            "plink_path": self.plink_path,
            "python_command": self.python_command,
        }


def deploy_command(root: Path, args) -> int:
    action = args.deploy_command
    if action == "setup":
        return deploy_setup(root, args)
    if action == "local":
        return deploy_local(root)
    if action == "remote":
        return deploy_remote(root, args)
    raise RuntimeError("Use: mwf deploy setup|local|remote")


def deploy_setup(root: Path, args) -> int:
    ignore_path, created = ensure_mwfignore(root)
    if created:
        print(f"Created {ignore_path}")
    print("Review .mwfignore before deploying; it decides exactly which project files leave this computer.")

    existing = read_server_config(root, required=False)
    host = args.host or _prompt("Server host or IP", existing.host if existing else None)
    user = args.user or _prompt("Server user", existing.user if existing else None)
    port = args.port or (existing.port if existing else 22)
    auth = args.auth or _prompt_choice(
        "Authentication",
        {"password", "key"},
        existing.auth if existing else "key",
    )

    key_path = args.key
    if auth == "key" and key_path is None:
        default_key = existing.key_path if existing else None
        key_path = _prompt("Private key path (blank to use ssh-agent/default keys)", default_key, allow_blank=True) or None

    tool = args.tool
    if tool is None:
        if auth == "password" or (key_path and key_path.lower().endswith(".ppk")):
            tool = "putty"
        else:
            tool = existing.tool if existing else "openssh"

    pscp_path = args.pscp or (existing.pscp_path if existing else None)
    plink_path = args.plink or (existing.plink_path if existing else None)
    if tool == "putty":
        pscp_path = pscp_path or _find_putty_tool("pscp") or "pscp"
        plink_path = plink_path or _find_putty_tool("plink") or "plink"

    python_command = args.python_command or (existing.python_command if existing else "python3")
    config = ServerConfig(
        host=host.strip(),
        user=user.strip(),
        port=_positive_port(port),
        auth=auth,
        tool=tool,
        key_path=key_path,
        pscp_path=pscp_path,
        plink_path=plink_path,
        python_command=python_command,
    )
    validate_server_config(config, require_tools=False)
    path = server_config_path(root)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(config.as_dict(), indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    print(f"Saved deployment server setup: {path}")
    print(f"  destination: {config.user}@{config.host}:{config.port}")
    print(f"  authentication: {config.auth} via {config.tool}")
    if config.auth == "password":
        print("  password: not stored; PuTTY will ask for it during upload and remote extraction")
    if config.tool == "putty" and (_resolve_command(config.pscp_path) is None or _resolve_command(config.plink_path) is None):
        print("  WARNING: pscp/plink were not found yet. Install PuTTY or update setup with --pscp and --plink.")
    print("Next: run 'mwf deploy local', review the output, then run 'mwf deploy remote'.")
    return 0


def deploy_local(root: Path) -> int:
    ignore_path, created = ensure_mwfignore(root)
    if created:
        print(f"Created {ignore_path}")
        print("IMPORTANT: review .mwfignore before sharing or deploying this project.")
    else:
        print(f"Using ignore rules from {ignore_path}")

    ignore = MWFIgnore.from_file(ignore_path)
    deploy_root = deploy_dir(root)
    final_local = deploy_root / "local"
    building = deploy_root / f".local-building-{uuid.uuid4().hex}"
    staging = building / "staging"
    archive = building / "deployment.zip"

    print("Preparing a fresh local deployment; the previous local deployment will be replaced.")
    if building.exists():
        shutil.rmtree(building)
    staging.mkdir(parents=True, exist_ok=True)

    stats = _build_staging(root, staging, ignore)
    print("Creating the final deployment archive...")
    _zip_tree(staging, archive)
    archive_size = archive.stat().st_size
    shutil.rmtree(staging)

    deploy_root.mkdir(parents=True, exist_ok=True)
    if final_local.exists():
        print(f"Removing previous local deployment: {final_local}")
        shutil.rmtree(final_local)
    building.replace(final_local)
    final_archive = final_local / "deployment.zip"

    print("Local deployment complete")
    print(f"  archive: {final_archive}")
    print(f"  copied files: {stats.copied_files}")
    print(f"  copied size before compression: {_format_bytes(stats.copied_bytes)}")
    print(f"  node subfolder archives: {stats.node_archives}")
    print(f"  ignored paths: {stats.ignored_paths}")
    if stats.skipped_links:
        print(f"  skipped symbolic links: {stats.skipped_links}")
    print(f"  final archive size: {_format_bytes(archive_size)}")
    print("The next local deployment will overwrite this archive to avoid accumulating large copies.")
    return 0


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


def _build_staging(root: Path, staging: Path, ignore: MWFIgnore) -> DeploymentStats:
    copied_files = 0
    copied_bytes = 0
    node_archives = 0
    ignored_paths = 0
    skipped_links = 0
    node_root = root / "node"

    def account_file(source: Path, destination: Path) -> None:
        nonlocal copied_files, copied_bytes
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination)
        copied_files += 1
        copied_bytes += source.stat().st_size
        if copied_files <= 10 or copied_files % 100 == 0:
            print(f"  copied {copied_files} file(s): {source.relative_to(root).as_posix()}")

    # Copy all non-node content while respecting ignore rules. The .mwf deploy
    # directory is always skipped independently to prevent recursive copying even
    # if a user accidentally re-includes it.
    for source in root.rglob("*"):
        relative = source.relative_to(root)
        relative_text = relative.as_posix()
        if relative.parts and relative.parts[0] == ".mwf":
            ignored_paths += 1
            continue
        if relative.parts and relative.parts[0] == "node":
            continue
        if source.is_symlink():
            skipped_links += 1
            continue
        is_dir = source.is_dir()
        if ignore.is_ignored(relative_text, is_dir=is_dir):
            ignored_paths += 1
            continue
        destination = staging / relative
        if is_dir:
            destination.mkdir(parents=True, exist_ok=True)
        elif source.is_file():
            account_file(source, destination)

    # Each direct node child is independently compressed. This dramatically
    # reduces the number of small files copied to a remote filesystem.
    if node_root.is_dir() and not ignore.is_ignored("node", is_dir=True):
        target_node = staging / "node"
        target_node.mkdir(parents=True, exist_ok=True)
        for child in sorted(node_root.iterdir(), key=lambda item: item.name.lower()):
            relative_child = child.relative_to(root).as_posix()
            if child.is_symlink():
                skipped_links += 1
                continue
            if ignore.is_ignored(relative_child, is_dir=child.is_dir()):
                ignored_paths += 1
                continue
            if child.is_file():
                account_file(child, target_node / child.name)
                continue
            if not child.is_dir():
                continue

            included, ignored_count, link_count = _included_node_entries(root, child, ignore)
            ignored_paths += ignored_count
            skipped_links += link_count
            archive_path = target_node / f"{child.name}.zip"
            if ignored_count == 0 and link_count == 0:
                print(f"  zipping node/{child.name} directly (no ignored content inside)")
            else:
                print(f"  zipping filtered node/{child.name} ({ignored_count} ignored path(s))")
            _zip_selected_tree(child, archive_path, included)
            node_archives += 1
            copied_files += sum(1 for path in included if path.is_file())
            copied_bytes += sum(path.stat().st_size for path in included if path.is_file())

    return DeploymentStats(copied_files, copied_bytes, node_archives, ignored_paths, skipped_links)


def _included_node_entries(root: Path, node_dir: Path, ignore: MWFIgnore) -> tuple[list[Path], int, int]:
    included: list[Path] = []
    ignored = 0
    links = 0
    for path in node_dir.rglob("*"):
        if path.is_symlink():
            links += 1
            continue
        relative = path.relative_to(root).as_posix()
        if ignore.is_ignored(relative, is_dir=path.is_dir()):
            ignored += 1
            continue
        included.append(path)
    return included, ignored, links


def _zip_selected_tree(base: Path, archive: Path, entries: Iterable[Path]) -> None:
    archive.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as handle:
        entries = list(entries)
        if not entries:
            # Preserve an empty node folder on extraction.
            handle.writestr(".mwf-empty", "")
            return
        for path in entries:
            arcname = path.relative_to(base).as_posix()
            if path.is_dir():
                handle.writestr(arcname.rstrip("/") + "/", "")
            elif path.is_file():
                handle.write(path, arcname)


def _zip_tree(base: Path, archive: Path) -> None:
    with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as handle:
        for path in sorted(base.rglob("*")):
            arcname = path.relative_to(base).as_posix()
            if path.is_dir():
                handle.writestr(arcname.rstrip("/") + "/", "")
            elif path.is_file():
                handle.write(path, arcname)


def local_archive_path(root: Path) -> Path:
    return deploy_dir(root) / "local" / "deployment.zip"


def server_config_path(root: Path) -> Path:
    return deploy_dir(root) / "server.json"


def read_server_config(root: Path, *, required: bool) -> ServerConfig | None:
    path = server_config_path(root)
    if not path.exists():
        if required:
            raise RuntimeError("No deployment server setup exists. Run: mwf deploy setup")
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise RuntimeError(f"Invalid deployment server setup at {path}: {error}") from error
    return ServerConfig.from_dict(data)


def validate_server_config(config: ServerConfig, *, require_tools: bool) -> None:
    if not config.host.strip() or not config.user.strip():
        raise RuntimeError("Server host and user are required")
    _positive_port(config.port)
    if config.auth == "password" and config.tool != "putty":
        raise RuntimeError("Password-based deployment must use PuTTY (pscp and plink)")
    if config.auth == "key" and config.key_path:
        key = Path(os.path.expandvars(os.path.expanduser(config.key_path)))
        if not key.is_file():
            raise RuntimeError(f"Private key does not exist: {key}")
    if not require_tools:
        return
    if config.tool == "putty":
        if _resolve_command(config.pscp_path) is None:
            raise RuntimeError("PuTTY pscp was not found. Run mwf deploy setup --pscp <path-to-pscp.exe>")
        if _resolve_command(config.plink_path) is None:
            raise RuntimeError("PuTTY plink was not found. Run mwf deploy setup --plink <path-to-plink.exe>")
    else:
        if shutil.which("scp") is None or shutil.which("ssh") is None:
            raise RuntimeError("OpenSSH scp/ssh were not found on PATH")


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


def _find_putty_tool(name: str) -> str | None:
    found = shutil.which(name)
    if found:
        return found
    if os.name != "nt":
        return None
    executable = f"{name}.exe"
    for base in [os.environ.get("ProgramFiles"), os.environ.get("ProgramFiles(x86)"), r"C:\Program Files", r"C:\Program Files (x86)"]:
        if not base:
            continue
        candidate = Path(base) / "PuTTY" / executable
        if candidate.is_file():
            return str(candidate)
    return None


def _resolve_command(value: str | None) -> str | None:
    if not value:
        return None
    expanded = _expanded_path(value)
    path = Path(expanded)
    if path.is_file():
        return str(path)
    return shutil.which(expanded)


def _expanded_path(value: str) -> str:
    return os.path.expandvars(os.path.expanduser(value))


def _prompt(label: str, default: str | None, *, allow_blank: bool = False) -> str:
    suffix = f" [{default}]" if default else ""
    while True:
        value = input(f"{label}{suffix}: ").strip()
        if value:
            return value
        if default is not None:
            return default
        if allow_blank:
            return ""
        print("A value is required.")


def _prompt_choice(label: str, choices: set[str], default: str) -> str:
    while True:
        value = input(f"{label} ({'/'.join(sorted(choices))}) [{default}]: ").strip().lower() or default
        if value in choices:
            return value
        print("Choose one of: " + ", ".join(sorted(choices)))


def _confirm(question: str, *, default: bool) -> bool:
    prompt = "[Y/n]" if default else "[y/N]"
    value = input(f"{question} {prompt} ").strip().lower()
    if not value:
        return default
    return value in {"y", "yes"}


def _required_text(data: dict, key: str) -> str:
    value = data.get(key)
    if not isinstance(value, str) or not value.strip():
        raise RuntimeError(f"Deployment server configuration requires {key!r}")
    return value.strip()


def _optional_text(value) -> str | None:
    return value.strip() if isinstance(value, str) and value.strip() else None


def _choice(value, label: str, choices: set[str]) -> str:
    if not isinstance(value, str) or value not in choices:
        raise RuntimeError(f"Deployment {label} must be one of: {', '.join(sorted(choices))}")
    return value


def _positive_port(value) -> int:
    try:
        port = int(value)
    except (TypeError, ValueError) as error:
        raise RuntimeError(f"Invalid SSH port: {value!r}") from error
    if not 1 <= port <= 65535:
        raise RuntimeError("SSH port must be between 1 and 65535")
    return port


def _format_bytes(value: int) -> str:
    number = float(value)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if number < 1024 or unit == "TiB":
            return f"{number:.1f} {unit}" if unit != "B" else f"{int(number)} B"
        number /= 1024
    return f"{number:.1f} TiB"


class _SetupArgs:
    host = None
    user = None
    port = None
    auth = None
    tool = None
    key = None
    pscp = None
    plink = None
    python_command = None
