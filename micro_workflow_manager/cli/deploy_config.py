from __future__ import annotations

import json
import os
import shutil
from dataclasses import dataclass
from pathlib import Path

from micro_workflow_manager.paths import deploy_dir

from .deploy_ignore import ensure_mwfignore


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

def deploy_setup(root: Path, args) -> int:
    ignore_path, created = ensure_mwfignore(root)
    if created:
        print(f"Created {ignore_path}")
    print("Review .mwfignore before deploying; it decides exactly which project files leave this computer.")

    existing = read_server_config(root, required=False)
    host = args.host or _prompt("Server host or IP", existing.host if existing else None)
    user = args.user or _prompt("Server user", existing.user if existing else None)
    if args.port is not None:
        port = args.port
    else:
        port = _prompt("SSH port", str(existing.port if existing else 22))
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
