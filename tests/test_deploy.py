from __future__ import annotations

import json
import subprocess
import zipfile
from pathlib import Path
from types import SimpleNamespace

from micro_workflow_manager import cli
from micro_workflow_manager.cli.deploy import _remote_extraction_script
from micro_workflow_manager.cli.deploy_ignore import MWFIgnore


def _init(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    assert cli.main(["init"]) == 0


def test_legacy_root_state_is_consolidated_into_mwf_directory(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".mwf").write_text(
        json.dumps({"version": 2, "schema_version": 1, "graph_path": None, "runner": "threaded", "edges": []}),
        encoding="utf-8",
    )
    (tmp_path / ".mwf_run.json").write_text('{"status":"done"}', encoding="utf-8")
    (tmp_path / ".mwf_threads.json").write_text('{"overrides":{"A":2}}', encoding="utf-8")
    (tmp_path / ".mwf_locks").mkdir()
    (tmp_path / ".mwf_locks" / "A.lock").write_text("0", encoding="utf-8")

    assert cli.main(["init"]) == 0
    out = capsys.readouterr().out
    assert "Migrated legacy runtime state" in out
    assert (tmp_path / ".mwf" / "project.json").is_file()
    assert (tmp_path / ".mwf" / "run.json").is_file()
    assert (tmp_path / ".mwf" / "threads.json").is_file()
    assert (tmp_path / ".mwf" / "locks" / "A.lock").is_file()
    assert not (tmp_path / ".mwf_run.json").exists()
    assert not (tmp_path / ".mwf_threads.json").exists()
    assert not (tmp_path / ".mwf_locks").exists()


def test_deploy_setup_creates_ignore_and_stores_no_password(tmp_path, monkeypatch, capsys):
    _init(tmp_path, monkeypatch)
    capsys.readouterr()

    assert cli.main([
        "deploy", "setup",
        "--host", "server.example",
        "--user", "chris",
        "--port", "2222",
        "--auth", "password",
        "--tool", "putty",
        "--pscp", "pscp",
        "--plink", "plink",
    ]) == 0
    output = capsys.readouterr().out
    assert "Review .mwfignore" in output
    assert (tmp_path / ".mwfignore").is_file()
    config = json.loads((tmp_path / ".mwf" / "deploy" / "server.json").read_text(encoding="utf-8"))
    assert config["host"] == "server.example"
    assert config["user"] == "chris"
    assert config["port"] == 2222
    assert config["auth"] == "password"
    assert "password" not in config


def test_mwfignore_supports_allowlist_negation():
    rules = MWFIgnore.from_text(
        """*
!.mwfignore
!README.md
!src/
!src/**
!node/
!node/start/
!node/start/input/
!node/start/input/**
"""
    )
    assert rules.is_ignored("secret.txt")
    assert not rules.is_ignored("README.md")
    assert not rules.is_ignored("src/node_behavior/A.py")
    assert not rules.is_ignored("node/start/input/book.pdf")
    assert rules.is_ignored("node/start/output/result.txt")
    assert rules.is_ignored("node/other/input/file.txt")


def test_deploy_local_filters_files_zips_each_node_and_overwrites_previous(tmp_path, monkeypatch, capsys):
    _init(tmp_path, monkeypatch)
    capsys.readouterr()
    (tmp_path / ".mwfignore").write_text(
        """.git/
.vscode/
.mwf/
*.tmp
.env
""",
        encoding="utf-8",
    )
    (tmp_path / "src").mkdir()
    (tmp_path / "src" / "main.py").write_text("print('v1')", encoding="utf-8")
    (tmp_path / ".env").write_text("SECRET=x", encoding="utf-8")
    (tmp_path / ".git").mkdir()
    (tmp_path / ".git" / "config").write_text("x", encoding="utf-8")
    for node in ("A", "B"):
        (tmp_path / "node" / node / "input").mkdir(parents=True)
        (tmp_path / "node" / node / "input" / "keep.txt").write_text(node, encoding="utf-8")
    (tmp_path / "node" / "A" / "input" / "skip.tmp").write_text("skip", encoding="utf-8")

    assert cli.main(["deploy", "local"]) == 0
    first_output = capsys.readouterr().out
    assert "zipping filtered node/A" in first_output
    assert "zipping node/B directly" in first_output
    archive = tmp_path / ".mwf" / "deploy" / "local" / "deployment.zip"
    assert archive.is_file()

    with zipfile.ZipFile(archive) as outer:
        names = set(outer.namelist())
        assert "src/main.py" in names
        assert ".env" not in names
        assert ".git/config" not in names
        assert "node/A.zip" in names
        assert "node/B.zip" in names
        outer.extract("node/A.zip", tmp_path / "inspect")
    with zipfile.ZipFile(tmp_path / "inspect" / "node" / "A.zip") as node_zip:
        assert "input/keep.txt" in node_zip.namelist()
        assert "input/skip.tmp" not in node_zip.namelist()

    (tmp_path / "src" / "main.py").write_text("print('v2')", encoding="utf-8")
    (tmp_path / "src" / "obsolete.py").write_text("obsolete", encoding="utf-8")
    assert cli.main(["deploy", "local"]) == 0
    capsys.readouterr()
    (tmp_path / "src" / "obsolete.py").unlink()
    assert cli.main(["deploy", "local"]) == 0
    capsys.readouterr()
    with zipfile.ZipFile(archive) as outer:
        assert "src/obsolete.py" not in outer.namelist()
        assert outer.read("src/main.py") == b"print('v2')"


def test_remote_extraction_script_expands_nested_node_archives(tmp_path):
    node_zip = tmp_path / "A.zip"
    with zipfile.ZipFile(node_zip, "w") as handle:
        handle.writestr("input/value.txt", "hello")
    outer = tmp_path / "deployment.zip"
    with zipfile.ZipFile(outer, "w") as handle:
        handle.writestr("src/main.py", "print('ok')")
        handle.write(node_zip, "node/A.zip")
    target = tmp_path / "remote target"

    exec(_remote_extraction_script(str(outer), str(target)), {})
    assert (target / "src" / "main.py").read_text(encoding="utf-8") == "print('ok')"
    assert (target / "node" / "A" / "input" / "value.txt").read_text(encoding="utf-8") == "hello"
    assert not (target / "node" / "A.zip").exists()
    assert not outer.exists()


def test_deploy_remote_uses_putty_for_password_setup(tmp_path, monkeypatch, capsys):
    _init(tmp_path, monkeypatch)
    capsys.readouterr()
    local = tmp_path / ".mwf" / "deploy" / "local"
    local.mkdir(parents=True)
    (local / "deployment.zip").write_bytes(b"zip")
    tools = tmp_path / "tools"
    tools.mkdir()
    pscp = tools / "pscp.exe"
    plink = tools / "plink.exe"
    pscp.write_text("", encoding="utf-8")
    plink.write_text("", encoding="utf-8")
    server = tmp_path / ".mwf" / "deploy" / "server.json"
    server.write_text(
        json.dumps({
            "host": "server.example",
            "user": "chris",
            "port": 2222,
            "auth": "password",
            "tool": "putty",
            "key_path": None,
            "pscp_path": str(pscp),
            "plink_path": str(plink),
            "python_command": "python3",
        }),
        encoding="utf-8",
    )
    commands: list[list[str]] = []

    def fake_run(command, check=False):
        commands.append(list(command))
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(subprocess, "run", fake_run)
    assert cli.main(["deploy", "remote", "--path", "/srv/example", "--yes"]) == 0
    output = capsys.readouterr().out
    assert "Remote deployment complete" in output
    assert len(commands) == 2
    assert commands[0][0] == str(pscp)
    assert commands[0][1:3] == ["-P", "2222"]
    assert commands[1][0] == str(plink)
    assert "/srv/example" not in " ".join(commands[0])
    assert "python3" in commands[1][-1]
