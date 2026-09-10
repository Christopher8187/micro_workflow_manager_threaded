"""Interrupt preflight refuses router exports it cannot determine without imports."""

from __future__ import annotations

from pathlib import Path

import pytest

from micro_workflow_manager import cli
from tests.test_064_read_only_previews import _snapshot
from tests.test_133_readonly_reset_live_refusal import _closed_database_rows
from tests.test_168_interrupt_preflight_integration import (
    _make_stale_interrupt_project,
    _running_session_row,
    _write_router,
)
from tests.test_149_interrupt_declarations import _make_interrupt_project


def _indeterminate_router_source(root: Path, form: str) -> str:
    marker = root / "user-router-imported.txt"
    prefix = f"""
from pathlib import Path
Path({str(marker)!r}).write_text('imported', encoding='utf-8')
from micro_workflow_manager import NodeRouter
"""
    if form == "unused-interrupt-router":
        declarations = """
unused = NodeRouter('I', runner='direct', interrupt=True)
router = NodeRouter('I', runner='direct', interrupt=False)
"""
    else:
        if form == "dynamic-export":
            declarations = """
interrupt_router = NodeRouter('I', runner='direct', interrupt=True)
ordinary_router = NodeRouter('I', runner='direct', interrupt=False)
router = interrupt_router if False else ordinary_router
"""
        elif form == "rebound-constructor-name":
            declarations = """
class FakeRouter:
    def task(self, function):
        return function
def NodeRouter(*args, **kwargs):
    return FakeRouter()
router = NodeRouter('I', runner='direct', interrupt=True)
"""
        elif form == "unrelated-attribute-owner":
            declarations = """
class FakeRouter:
    def task(self, function):
        return function
class LocalNamespace:
    @staticmethod
    def NodeRouter(*args, **kwargs):
        return FakeRouter()
local = LocalNamespace()
router = local.NodeRouter('I', runner='direct', interrupt=True)
"""
        elif form in {"setattr-interrupt", "delattr-interrupt"}:
            mutation = (
                "setattr(router, 'interrupt', False)"
                if form == "setattr-interrupt"
                else "delattr(router, 'interrupt')"
            )
            declarations = f"""
router = NodeRouter('I', runner='direct', interrupt=True)
{mutation}
"""
        elif form == "setattr-named-expression-alias":
            declarations = """
router = NodeRouter('I', runner='direct', interrupt=True)
setattr((saved := router), 'interrupt', False)
"""
        elif form == "setattr-container-alias":
            declarations = """
router = NodeRouter('I', runner='direct', interrupt=True)
holder = [router]
setattr(holder[0], 'interrupt', False)
"""
        elif form in {
            "decorated-function-router-alias",
            "decorated-class-router-alias",
        }:
            definition = (
                "def saved():\n    return None"
                if form == "decorated-function-router-alias"
                else "class saved:\n    pass"
            )
            declarations = f"""
router = NodeRouter('I', runner='direct', interrupt=True)
def as_router(value):
    return router
@as_router
{definition}
setattr(saved, 'interrupt', False)
"""
        elif form == "wildcard-constructor-rebind":
            declarations = """
from _interrupt_shadow import *
router = NodeRouter('I', runner='direct', interrupt=True)
"""
        else:
            assert form == "assignment-expression-rebind"
            declarations = """
class FakeRouter:
    def task(self, function):
        return function
def fake_router(*args, **kwargs):
    return FakeRouter()
ignored = (NodeRouter := fake_router)
router = NodeRouter('I', runner='direct', interrupt=True)
"""
    return prefix + declarations + """
@router.task
def run(ctx):
    ctx.write_output('ran.txt', 'I')
    return 'I'
"""


def _write_wildcard_shadow(root: Path) -> None:
    path = root / "src" / "node_behavior" / "_interrupt_shadow.py"
    path.write_text(
        """
__all__ = ['NodeRouter']
class FakeRouter:
    def task(self, function):
        return function
def NodeRouter(*args, **kwargs):
    return FakeRouter()
""".strip() + "\n",
        encoding="utf-8",
    )


def _supported_import_source(root: Path, form: str) -> str:
    marker = root / "user-router-imported.txt"
    if form == "module-alias":
        imported = "import micro_workflow_manager as mwf"
        declaration = "router = mwf.NodeRouter('I', runner='direct', interrupt=True)"
    else:
        assert form == "router-class-alias"
        imported = (
            "from micro_workflow_manager.router import "
            "NodeRouter as RoutedNode"
        )
        declaration = "router = RoutedNode('I', runner='direct', interrupt=True)"
    return f"""
from pathlib import Path
Path({str(marker)!r}).write_text('imported', encoding='utf-8')
{imported}
{declaration}
@router.task
def run(ctx):
    ctx.write_output('ran.txt', 'I')
    return 'I'
"""


def _unrelated_interrupt_attribute_source(root: Path) -> str:
    marker = root / "user-router-imported.txt"
    return f"""
from pathlib import Path
Path({str(marker)!r}).write_text('imported', encoding='utf-8')
from micro_workflow_manager import NodeRouter
def unrelated_function():
    return None
setattr(unrelated_function, 'interrupt', False)
router = NodeRouter('I', runner='direct', interrupt=True)
@router.task
def run(ctx):
    ctx.write_output('ran.txt', 'I')
    return 'I'
"""


@pytest.mark.parametrize("form", ["module-alias", "router-class-alias"])
def test_exact_public_node_router_imports_are_accepted_without_importing_on_plan(
    tmp_path, monkeypatch, capsys, form,
):
    _make_interrupt_project(
        tmp_path, monkeypatch,
        edges=[("I", "I")], jobs={"I"},
    )
    _write_router(tmp_path, "I", _supported_import_source(tmp_path, form))
    before_rows = _closed_database_rows(tmp_path)
    before_files = _snapshot(tmp_path)
    marker = tmp_path / "user-router-imported.txt"
    capsys.readouterr()

    assert cli.main([
        "run", "I", "--plan", "--interrupt-policy", "run-all",
    ]) == 0

    output = capsys.readouterr().out.lower()
    assert "interrupt component {i}" in output
    assert "canonical replay key: i" in output
    assert _closed_database_rows(tmp_path) == before_rows
    assert _snapshot(tmp_path) == before_files
    assert not marker.exists()


def test_statically_unrelated_interrupt_attribute_mutation_is_accepted(
    tmp_path, monkeypatch, capsys,
):
    _make_interrupt_project(
        tmp_path, monkeypatch,
        edges=[("I", "I")], jobs={"I"},
    )
    source_text = _unrelated_interrupt_attribute_source(tmp_path)
    source = _write_router(tmp_path, "I", source_text)
    before_rows = _closed_database_rows(tmp_path)
    before_files = _snapshot(tmp_path)
    marker = tmp_path / "user-router-imported.txt"
    capsys.readouterr()

    assert cli.main([
        "run", "I", "--plan", "--interrupt-policy", "run-all",
    ]) == 0

    output = capsys.readouterr().out.lower()
    assert "interrupt component {i}" in output
    assert "canonical replay key: i" in output
    assert _closed_database_rows(tmp_path) == before_rows
    assert _snapshot(tmp_path) == before_files
    assert not marker.exists()
    assert source.read_text(encoding="utf-8") == source_text.strip() + "\n"


@pytest.mark.parametrize(
    "form",
    [
        "unused-interrupt-router",
        "dynamic-export",
        "rebound-constructor-name",
        "unrelated-attribute-owner",
        "setattr-interrupt",
        "delattr-interrupt",
        "setattr-named-expression-alias",
        "setattr-container-alias",
        "decorated-function-router-alias",
        "decorated-class-router-alias",
        "wildcard-constructor-rebind",
        "assignment-expression-rebind",
    ],
)
def test_indeterminate_router_exports_refuse_before_stale_recovery_or_import(
    tmp_path, monkeypatch, capsys, form,
):
    _, _, marker = _make_stale_interrupt_project(tmp_path, monkeypatch)
    if form == "wildcard-constructor-rebind":
        _write_wildcard_shadow(tmp_path)
    source = _write_router(tmp_path, "I", _indeterminate_router_source(tmp_path, form))
    before_rows = _closed_database_rows(tmp_path)
    before_files = _snapshot(tmp_path)
    stale_before = _running_session_row(tmp_path, "stale-A")
    capsys.readouterr()

    assert cli.main([
        "run", "I", "--runner", "direct",
        "--interrupt-policy", "run-all",
    ]) == 1

    captured = capsys.readouterr()
    output = (captured.out + captured.err).lower()
    assert "interrupt" in output
    assert "i.py" in output
    assert "static" in output or "export" in output
    assert _closed_database_rows(tmp_path) == before_rows
    assert _snapshot(tmp_path) == before_files
    assert _running_session_row(tmp_path, "stale-A") == stale_before
    assert not marker.exists()
    assert not (tmp_path / "node" / "I" / "output" / "ran.txt").exists()
    assert source.read_text(encoding="utf-8") == _indeterminate_router_source(
        tmp_path, form,
    ).strip() + "\n"
