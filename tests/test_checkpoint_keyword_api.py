from __future__ import annotations

import inspect
import json
import time
from pathlib import Path

import pytest

from micro_workflow_manager import JobContext, MicroWorkflow, NodeRouter, __version__
from micro_workflow_manager.errors import JobFailedError


def test_released_checkpoint_signature_supports_keywords():
    signature = inspect.signature(JobContext.checkpoint)
    assert list(signature.parameters) == [
        "self", "name", "timeout", "progress", "detail"
    ]
    assert signature.parameters["name"].default is None
    for name in ("timeout", "progress", "detail"):
        assert signature.parameters[name].kind is inspect.Parameter.KEYWORD_ONLY
        assert signature.parameters[name].default is None
    assert __version__ == "0.2.5"


def test_checkpoint_keywords_persist_for_inspect(tmp_path: Path):
    workflow = MicroWorkflow(project_dir=tmp_path, runner="direct")

    @workflow.task("A", timeout=2)
    def run(ctx):
        ctx.checkpoint(
            name="halfway",
            timeout=1,
            progress=0.5,
            detail="one of two sections",
        )
        return "ok"

    workflow.start("A")
    assert workflow.run_job("A", 1, ignore_readiness=True) == "ok"

    runtime = json.loads((tmp_path / "node" / "A" / "jobs" / "1" / "runtime.json").read_text())
    assert runtime["checkpoint_name"] == "halfway"
    assert runtime["checkpoint_timeout_seconds"] == 1.0
    assert runtime["progress"] == 0.5
    assert runtime["progress_detail"] == "one of two sections"
    assert runtime["state"] == "completed"


def test_dynamic_checkpoint_timeout_fails_and_reaches_fallback(tmp_path: Path):
    workflow = MicroWorkflow(project_dir=tmp_path, runner="direct")
    calls: list[str] = []

    @workflow.task("A", timeout=2)
    def run(ctx):
        calls.append("main")
        ctx.checkpoint("blocking section", timeout=0.05, progress=0.25)
        time.sleep(0.2)
        return "late"

    @workflow.fallback("A", name="recover", timeout=1)
    def recover(ctx, error=None):
        calls.append("fallback")
        ctx.checkpoint("recovered", timeout=0.5, progress=1.0, detail=str(error))
        return "recovered"

    workflow.start("A")
    assert workflow.run_job("A", 1, ignore_readiness=True) == "recovered"
    assert calls == ["main", "fallback"]


def test_checkpoint_keyword_validation(tmp_path: Path):
    workflow = MicroWorkflow(project_dir=tmp_path, runner="direct")

    @workflow.task("A", timeout=1)
    def run(ctx):
        ctx.checkpoint("bad", timeout=0.5, progress=2)

    workflow.start("A")
    with pytest.raises(JobFailedError):
        workflow.run_job("A", 1, ignore_readiness=True)


def test_router_mount_preserves_total_timeout_and_keyword_checkpoint(tmp_path: Path):
    workflow = MicroWorkflow(project_dir=tmp_path, runner="direct")
    router = NodeRouter("A")

    @router.task(timeout=2)
    def run(ctx):
        ctx.checkpoint("inside", timeout=1, progress=0.1, detail="ready")
        return 7

    router.mount_to(workflow)
    workflow.start("A")
    assert workflow.run_job("A", 1, ignore_readiness=True) == 7
