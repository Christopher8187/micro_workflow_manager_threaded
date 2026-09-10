from __future__ import annotations

import json
import os
import time
from datetime import datetime
from pathlib import Path

import pytest

from micro_workflow_manager import MicroWorkflow, NodeRouter, cli
from micro_workflow_manager.models import DONE, FAILED, QUEUED, RUNNING
from micro_workflow_manager.storage import FileStorage


def write_project(tmp_path: Path, monkeypatch, *, graph="EDGES = [('A', 'B')]\n") -> Path:
    monkeypatch.chdir(tmp_path)
    behavior = tmp_path / "src" / "node_behavior"
    behavior.mkdir(parents=True)
    (tmp_path / "src" / "graph.py").write_text(graph, encoding="utf-8")
    (behavior / "A.py").write_text(
        '''from micro_workflow_manager import NodeRouter
router = NodeRouter("A")
router.create_job(number=1)
@router.task
def run(ctx):
    return 1
''',
        encoding="utf-8",
    )
    (behavior / "B.py").write_text(
        '''from micro_workflow_manager import NodeRouter
router = NodeRouter("B")
@router.task
def run(ctx, value=1):
    return value * 2
''',
        encoding="utf-8",
    )
    assert cli.main(["init"]) == 0
    assert cli.main(["graph", "src/graph.py", "--runner", "direct"]) == 0
    return behavior


def test_graph_path_is_stored_with_slashes_and_accepts_backslashes(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    behavior = tmp_path / "src" / "node_behavior"
    behavior.mkdir(parents=True)
    (tmp_path / "src" / "graph.py").write_text("EDGES = [('A', 'B')]\n", encoding="utf-8")
    router_source = (
        'from micro_workflow_manager import NodeRouter\n'
        'router = NodeRouter("{name}")\n'
        '@router.task\n'
        'def run(ctx):\n'
        '    return None\n'
    )
    for name in ("A", "B"):
        (behavior / f"{name}.py").write_text(router_source.format(name=name), encoding="utf-8")
    assert cli.main(["init"]) == 0
    # A Windows-style command path must also work when this test runs on Linux.
    assert cli.main(["graph", "src\\graph.py", "--runner", "direct"]) == 0
    config_path = tmp_path / ".mwf" / "project.json"
    config = json.loads(config_path.read_text(encoding="utf-8"))
    assert config["graph_path"] == "src/graph.py"

    config["graph_path"] = "src\\graph.py"
    config_path.write_text(json.dumps(config), encoding="utf-8")
    assert cli.main(["monitor", "--once"]) == 0
    capsys.readouterr()

    assert cli.main(["graph", "--update"]) == 0
    config = json.loads(config_path.read_text(encoding="utf-8"))
    assert config["graph_path"] == "src/graph.py"


def test_doctor_detects_missing_router_without_mutating_project(tmp_path, monkeypatch, capsys):
    behavior = write_project(tmp_path, monkeypatch)
    capsys.readouterr()
    before = (tmp_path / ".mwf" / "project.json").read_bytes()

    assert cli.main(["doctor"]) == 0
    assert "Healthy" in capsys.readouterr().out

    (behavior / "B.py").unlink()
    assert cli.main(["doctor"]) == 1
    out = capsys.readouterr().out
    assert "without node_behavior files: B" in out
    assert (tmp_path / ".mwf" / "project.json").read_bytes() == before



def test_doctor_reports_malformed_payload_and_continues(tmp_path, monkeypatch, capsys):
    write_project(tmp_path, monkeypatch)
    capsys.readouterr()
    payload = tmp_path / "node" / "A" / "jobs" / "1" / "input.json"
    payload.write_text("{not json", encoding="utf-8")

    assert cli.main(["doctor"]) == 1
    out = capsys.readouterr().out
    assert "malformed JSON" in out
    assert str(payload) in out


def test_events_and_inspect_show_job_history(tmp_path, monkeypatch, capsys):
    write_project(tmp_path, monkeypatch)
    capsys.readouterr()
    assert cli.main(["run", "A"]) == 0
    capsys.readouterr()

    rows = FileStorage(tmp_path).read_job_events("A", 1)
    names = [row["event"] for row in rows]
    # A fresh CLI run clears the pre-run journal by default. The new execution
    # history starts with its reset/queued event rather than retaining the
    # original job-creation record.
    assert "created" not in names
    assert "queued" in names
    assert "started" in names
    assert "done" in names

    assert cli.main(["inspect", "A", "job", "1"]) == 0
    out = capsys.readouterr().out
    assert "Job A/1" in out
    assert "Events:" in out
    assert "started" in out
    assert "done" in out


def test_recover_requeues_only_abandoned_running_jobs(tmp_path, monkeypatch, capsys):
    from tests.test_064_read_only_previews import _initialize_native_project
    from tests.test_146_native_applied_recovery import _create_active_scope, _scope_rows

    workflow = _initialize_native_project(tmp_path, monkeypatch, edges=[('A', 'B')])
    storage = workflow.storage
    shape = workflow.topology.graph_shape()
    abandoned = _create_active_scope(storage, shape, 'A', 'abandoned-A')
    _create_active_scope(storage, shape, 'B', 'live-B', live=True)
    live_before = _scope_rows(storage, 'B', 'live-B')
    capsys.readouterr()
    try:
        assert cli.main(['recover']) == 0
        assert storage.get_job_status('A', 1) == QUEUED
        session = storage.get_execution_session('abandoned-A')
        assert session['status'] == 'terminal' and session['outcome'] == 'failed'
        assert storage.read_job_control('A', 1)['generation'] == abandoned['generation'] + 1
        assert _scope_rows(storage, 'B', 'live-B') == live_before
        assert not (tmp_path / '.mwf' / 'run.json').exists()
    finally:
        storage.close_database_connections()


def test_timeout_moves_to_fallback_and_blocks_late_context_write(tmp_path):
    workflow = MicroWorkflow(project_dir=tmp_path, runner="direct")
    workflow.graph([("A", "B")])

    @workflow.task("A", timeout=0.03)
    def slow(ctx):
        time.sleep(0.08)
        ctx.write_output("late.txt", "must not commit")
        return "late"

    @workflow.fallback("A", name="quick")
    def quick(ctx, error=None):
        return "fallback"

    @workflow.task("B")
    def b(ctx):
        return None

    job = workflow.start("A")
    assert workflow.run_job("A", job.job_id, ignore_readiness=True) == "fallback"
    time.sleep(0.1)
    assert not (tmp_path / "node" / "A" / "output" / "late.txt").exists()
    events = workflow.storage.read_job_events("A", job.job_id)
    assert any(event.get("event") == "timeout" for event in events)
    assert any(event.get("event") == "fallback_started" for event in events)




def test_all_failed_fallbacks_report_the_terminal_error(tmp_path):
    workflow = MicroWorkflow(project_dir=tmp_path, runner="direct")
    workflow.graph([("A", "B")])

    @workflow.task("A")
    def primary(ctx):
        raise ValueError("primary validation error")

    @workflow.fallback("A", name="final")
    def final(ctx, error=None):
        raise TimeoutError("terminal apply timeout")

    @workflow.task("B")
    def b(ctx):
        return None

    job = workflow.start("A")
    with pytest.raises(Exception) as raised:
        workflow.run_job("A", job.job_id, ignore_readiness=True)

    assert isinstance(raised.value.__cause__, TimeoutError)
    assert "terminal apply timeout" in str(raised.value.__cause__)
    output = workflow.storage.read_json(workflow.storage.output_file("A", job.job_id))
    assert "terminal apply timeout" in output["error"]
    assert "primary validation error" not in output["error"]


def test_initial_attempt_receives_empty_fresh_failure_history(tmp_path):
    workflow = MicroWorkflow(project_dir=tmp_path, runner="direct")
    workflow.graph([])
    observed = {}

    @workflow.task("A")
    def primary(ctx, errors):
        ctx_errors = ctx.errors
        assert ctx_errors == []
        assert errors == []
        assert ctx_errors is not errors

        ctx_errors.append(ValueError("local ctx mutation"))
        errors.append(ValueError("local parameter mutation"))
        observed["fresh_ctx_errors"] = ctx.errors
        return "ok"

    job = workflow.start("A")
    assert workflow.run_job("A", job.job_id, ignore_readiness=True) == "ok"
    assert observed["fresh_ctx_errors"] == []


def test_main_retry_receives_ordered_failure_history(tmp_path):
    workflow = MicroWorkflow(project_dir=tmp_path, runner="direct")
    workflow.graph([])
    first_error = ValueError("first attempt failed")
    attempts = []

    @workflow.task("A", retries=1)
    def primary(ctx, errors):
        ctx_errors = ctx.errors
        attempts.append(ctx.attempt)

        if ctx.attempt == 1:
            assert ctx.error is None
            assert ctx_errors == errors == []
            raise first_error

        assert ctx.error is first_error
        assert ctx_errors == [first_error]
        assert errors == [first_error]
        assert ctx_errors[0] is first_error
        assert errors[0] is first_error
        assert ctx_errors is not errors
        return "recovered"

    job = workflow.start("A")
    assert workflow.run_job("A", job.job_id, ignore_readiness=True) == "recovered"
    assert attempts == [1, 2]


def test_repeated_attempt_failure_enters_ordered_history(tmp_path):
    workflow = MicroWorkflow(project_dir=tmp_path, runner="direct")
    workflow.graph([])
    repeated_error = ValueError("second repetition failed")
    observations = []

    @workflow.task("A", repeats=2, retries=1)
    def primary(ctx, errors):
        ctx_errors = ctx.errors
        observations.append((ctx.attempt, ctx.repeat_index))

        if ctx.attempt == 1 and ctx.repeat_index == 1:
            assert ctx.error is None
            assert ctx_errors == errors == []
            return "first repetition"

        if ctx.attempt == 1 and ctx.repeat_index == 2:
            assert ctx.error is None
            assert ctx_errors == errors == []
            raise repeated_error

        assert ctx.error is repeated_error
        assert ctx_errors == [repeated_error]
        assert errors == [repeated_error]
        assert ctx_errors[0] is repeated_error
        assert errors[0] is repeated_error
        assert ctx_errors is not errors
        return f"recovered-{ctx.repeat_index}"

    job = workflow.start("A")
    assert workflow.run_job("A", job.job_id, ignore_readiness=True) == [
        "recovered-1",
        "recovered-2",
    ]
    assert observations == [(1, 1), (1, 2), (2, 1), (2, 2)]


def test_multiple_fallbacks_receive_ordered_failure_history(tmp_path):
    workflow = MicroWorkflow(project_dir=tmp_path, runner="direct")
    workflow.graph([])
    main_error = ValueError("main failed")
    first_fallback_error = RuntimeError("first fallback failed")
    calls = []

    @workflow.task("A")
    def primary(ctx, errors):
        calls.append("main")
        assert ctx.error is None
        assert ctx.errors == errors == []
        raise main_error

    @workflow.fallback("A", name="first")
    def first(ctx, errors):
        ctx_errors = ctx.errors
        calls.append("first")
        assert ctx.error is main_error
        assert ctx_errors == [main_error]
        assert errors == [main_error]
        assert ctx_errors[0] is main_error
        assert errors[0] is main_error
        assert ctx_errors is not errors
        raise first_fallback_error

    @workflow.fallback("A", name="second")
    def second(ctx, errors):
        ctx_errors = ctx.errors
        calls.append("second")
        assert ctx.error is first_fallback_error
        assert ctx_errors == [main_error, first_fallback_error]
        assert errors == [main_error, first_fallback_error]
        assert ctx_errors[0] is main_error
        assert ctx_errors[1] is first_fallback_error
        assert errors[0] is main_error
        assert errors[1] is first_fallback_error
        assert ctx_errors is not errors
        return "recovered"

    job = workflow.start("A")
    assert workflow.run_job("A", job.job_id, ignore_readiness=True) == "recovered"
    assert calls == ["main", "first", "second"]


def test_fallback_retry_receives_ordered_failure_history(tmp_path):
    workflow = MicroWorkflow(project_dir=tmp_path, runner="direct")
    workflow.graph([])
    main_error = ValueError("main failed")
    fallback_attempt_error = RuntimeError("fallback attempt failed")
    observations = []

    @workflow.task("A")
    def primary(ctx, errors):
        assert ctx.error is None
        assert ctx.errors == errors == []
        raise main_error

    @workflow.fallback("A", name="retrying", retries=1)
    def retrying(ctx, errors):
        ctx_errors = ctx.errors
        observations.append(
            {
                "attempt": ctx.attempt,
                "error": ctx.error,
                "ctx_errors": ctx_errors,
                "errors": errors,
                "same_list": ctx_errors is errors,
            }
        )
        if ctx.attempt == 1:
            raise fallback_attempt_error
        return "recovered"

    job = workflow.start("A")
    assert workflow.run_job("A", job.job_id, ignore_readiness=True) == "recovered"
    assert [item["attempt"] for item in observations] == [1, 2]

    first, second = observations
    assert first["error"] is main_error
    assert first["ctx_errors"] == [main_error]
    assert first["errors"] == [main_error]
    assert first["ctx_errors"][0] is main_error
    assert first["errors"][0] is main_error
    assert first["same_list"] is False

    assert second["error"] is fallback_attempt_error
    assert second["ctx_errors"] == [main_error, fallback_attempt_error]
    assert second["errors"] == [main_error, fallback_attempt_error]
    assert second["ctx_errors"][0] is main_error
    assert second["ctx_errors"][1] is fallback_attempt_error
    assert second["errors"][0] is main_error
    assert second["errors"][1] is fallback_attempt_error
    assert second["same_list"] is False


def test_local_error_list_mutation_does_not_enter_failure_history(tmp_path):
    workflow = MicroWorkflow(project_dir=tmp_path, runner="direct")
    workflow.graph([])
    first_error = ValueError("first attempt failed")
    second_error = RuntimeError("second attempt failed")
    local_ctx_error = LookupError("local ctx mutation")
    local_parameter_error = LookupError("local parameter mutation")
    attempt_observations = []
    fallback_observations = []

    @workflow.task("A", retries=1)
    def primary(ctx, errors):
        ctx_errors = ctx.errors
        attempt_observations.append(
            {
                "attempt": ctx.attempt,
                "error": ctx.error,
                "ctx_errors": ctx_errors,
                "errors": errors,
                "same_list": ctx_errors is errors,
            }
        )
        if ctx.attempt == 1:
            raise first_error

        ctx_errors.append(local_ctx_error)
        errors.append(local_parameter_error)
        raise second_error

    @workflow.fallback("A", name="recover")
    def recover(ctx, errors):
        ctx_errors = ctx.errors
        fallback_observations.append(
            {
                "error": ctx.error,
                "ctx_errors": ctx_errors,
                "errors": errors,
                "same_list": ctx_errors is errors,
            }
        )
        return "recovered"

    job = workflow.start("A")
    assert workflow.run_job("A", job.job_id, ignore_readiness=True) == "recovered"

    first, second = attempt_observations
    assert first["attempt"] == 1
    assert first["error"] is None
    assert first["ctx_errors"] == []
    assert first["errors"] == []
    assert first["same_list"] is False

    assert second["attempt"] == 2
    assert second["error"] is first_error
    assert second["ctx_errors"] == [first_error, local_ctx_error]
    assert second["errors"] == [first_error, local_parameter_error]
    assert second["same_list"] is False

    [fallback] = fallback_observations
    assert fallback["error"] is second_error
    assert fallback["ctx_errors"] == [first_error, second_error]
    assert fallback["errors"] == [first_error, second_error]
    assert fallback["ctx_errors"][0] is first_error
    assert fallback["ctx_errors"][1] is second_error
    assert fallback["errors"][0] is first_error
    assert fallback["errors"][1] is second_error
    assert local_ctx_error not in fallback["ctx_errors"]
    assert local_parameter_error not in fallback["errors"]
    assert fallback["same_list"] is False


def test_failure_history_is_scoped_to_one_job_execution(tmp_path):
    workflow = MicroWorkflow(project_dir=tmp_path, runner="direct")
    workflow.graph([("A", "B")])
    execution_number = 0
    execution_errors = []
    a_observations = []
    b_observations = []

    @workflow.task("A", retries=1)
    def a(ctx, errors):
        nonlocal execution_number
        if ctx.attempt == 1:
            execution_number += 1
            execution_errors.append(ValueError(f"execution {execution_number} failed"))

        current_error = execution_errors[-1]
        ctx_errors = ctx.errors
        a_observations.append(
            (
                execution_number,
                ctx.attempt,
                ctx.error,
                ctx_errors,
                errors,
                ctx_errors is errors,
            )
        )
        if ctx.attempt == 1:
            raise current_error

        ctx.node("B").add(source_execution=execution_number)
        return execution_number

    @workflow.task("B")
    def b(ctx, errors, source_execution):
        ctx_errors = ctx.errors
        b_observations.append(
            (
                source_execution,
                ctx.error,
                ctx_errors,
                errors,
                ctx_errors is errors,
            )
        )
        return source_execution

    job = workflow.start("A")
    assert workflow.run_job("A", job.job_id, ignore_readiness=True) == 1
    assert workflow.run_job("B", 1, ignore_readiness=True) == 1
    assert workflow.run_job("A", job.job_id, ignore_readiness=True) == 2
    # Fresh rerunning A removes its earlier causal work in B before publishing
    # the next job. In-memory handler observations still cover both executions.
    assert workflow.storage.list_job_ids("B") == [1]
    assert workflow.run_job("B", 1, ignore_readiness=True) == 2

    assert [(item[0], item[1]) for item in a_observations] == [
        (1, 1),
        (1, 2),
        (2, 1),
        (2, 2),
    ]
    first_start, first_retry, second_start, second_retry = a_observations

    for start in (first_start, second_start):
        assert start[2] is None
        assert start[3] == start[4] == []
        assert start[5] is False

    for retry, expected_error in zip(
        (first_retry, second_retry), execution_errors, strict=True
    ):
        assert retry[2] is expected_error
        assert retry[3] == retry[4] == [expected_error]
        assert retry[3][0] is expected_error
        assert retry[4][0] is expected_error
        assert retry[5] is False

    assert [item[0] for item in b_observations] == [1, 2]
    for _, error, ctx_errors, errors, same_list in b_observations:
        assert error is None
        assert ctx_errors == errors == []
        assert same_list is False


def test_failed_task_attempts_record_ordered_durable_events(tmp_path):
    workflow = MicroWorkflow(project_dir=tmp_path, runner="direct")
    workflow.graph([])
    main_repeat_error = ValueError("main repetition failed")
    main_retry_error = RuntimeError("main retry failed")
    fallback_retry_error = LookupError("fallback retry failed")

    @workflow.task("A", repeats=2, retries=1)
    def primary(ctx):
        if ctx.attempt == 1 and ctx.repeat_index == 1:
            return "partial"
        if ctx.attempt == 1:
            raise main_repeat_error
        raise main_retry_error

    @workflow.fallback("A", name="recover", retries=1)
    def recover(ctx):
        if ctx.attempt == 1:
            raise fallback_retry_error
        return "recovered"

    job = workflow.start("A")
    assert workflow.run_job("A", job.job_id, ignore_readiness=True) == "recovered"

    events = workflow.storage.read_job_events("A", job.job_id)
    failure_events = [event for event in events if event["event"] == "task_failed"]
    assert [
        (
            event["task"],
            event["task_role"],
            event["attempt"],
            event["repeat_index"],
            event["error"],
        )
        for event in failure_events
    ] == [
        ("primary", "main", 1, 2, repr(main_repeat_error)),
        ("primary", "main", 2, 1, repr(main_retry_error)),
        ("recover", "fallback", 1, 1, repr(fallback_retry_error)),
    ]

    for event in failure_events:
        assert isinstance(event["time"], str)
        datetime.fromisoformat(event["time"])

    relevant_events = [
        event["event"]
        for event in events
        if event["event"] in {"task_failed", "retry_started", "fallback_started"}
    ]
    assert relevant_events == [
        "task_failed",
        "retry_started",
        "task_failed",
        "fallback_started",
        "task_failed",
        "retry_started",
    ]


def test_failed_task_event_survives_an_exception_with_broken_repr(tmp_path):
    class BrokenReprError(Exception):
        def __repr__(self):
            raise RuntimeError("repr exploded")

    workflow = MicroWorkflow(project_dir=tmp_path, runner="direct")
    workflow.graph([])
    workflow.active_job_restart_enabled = True
    original_error = BrokenReprError("original handler failure")

    @workflow.task("A")
    def primary(ctx):
        raise original_error

    job = workflow.start("A")
    with pytest.raises(Exception) as raised:
        workflow.run_job("A", job.job_id, ignore_readiness=True)

    assert raised.value.__cause__ is original_error
    [failure] = [
        event
        for event in workflow.storage.read_job_events("A", job.job_id)
        if event["event"] == "task_failed"
    ]
    assert failure["error"] == "BrokenReprError('original handler failure')"
    output = workflow.storage.read_json(workflow.storage.output_file("A", job.job_id))
    assert output["error"] == "BrokenReprError('original handler failure')"


def test_context_sleep_and_cancellation_alias(tmp_path):
    workflow = MicroWorkflow(project_dir=tmp_path, runner="direct")
    workflow.graph([("A", "B")])

    @workflow.task("A")
    def a(ctx):
        ctx.raise_if_cancelled()
        ctx.sleep(0.01)
        assert not ctx.is_cancelled()
        return "ok"

    @workflow.task("B")
    def b(ctx):
        return None

    assert workflow.run_one("A") == "ok"


def test_job_context_has_no_public_transaction_helper():
    from micro_workflow_manager.context import JobContext

    assert not hasattr(JobContext, "transaction")


def test_explicit_cross_node_idempotency_keys_reuse_precomputed_children(tmp_path):
    workflow = MicroWorkflow(project_dir=tmp_path, runner="direct")
    workflow.graph([("A", "B"), ("A", "C")])

    @workflow.task("A")
    def a(ctx):
        children = [
            ("B", {"value": 1}, "a-to-b:1"),
            ("C", {"value": 2}, "a-to-c:2"),
        ]
        return [
            ctx.node(node).add(idempotency_key=key, **params).job_id
            for node, params, key in children
        ]

    @workflow.task("B")
    def b(ctx, value):
        return value

    @workflow.task("C")
    def c(ctx, value):
        return value

    parent = workflow.start("A")
    assert workflow.run_job("A", parent.job_id, ignore_readiness=True) == [1, 1]
    workflow.storage.request_job_restart("A", parent.job_id, reason="repeat parent")
    assert workflow.run_job("A", parent.job_id, ignore_readiness=True) == [1, 1]
    assert workflow.storage.list_job_ids("B") == [1]
    assert workflow.storage.list_job_ids("C") == [1]


def test_resumefrom_preserves_done_jobs_and_continues_failed_descendant(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    behavior = tmp_path / "src" / "node_behavior"
    behavior.mkdir(parents=True)
    (tmp_path / "src" / "graph.py").write_text("EDGES = [('A', 'B')]\n", encoding="utf-8")
    (behavior / "A.py").write_text(
        '''from micro_workflow_manager import NodeRouter
router = NodeRouter("A")
router.create_job(number=1)
@router.task
def run(ctx):
    ctx.node("B").add(value=5)
    return "A done"
''', encoding="utf-8")
    (behavior / "B.py").write_text(
        '''from pathlib import Path
from micro_workflow_manager import NodeRouter
router = NodeRouter("B")
@router.task
def run(ctx, value):
    marker = Path(ctx.system.storage.project_dir) / "failed_once.txt"
    if not marker.exists():
        marker.write_text("yes", encoding="utf-8")
        raise RuntimeError("first failure")
    return value * 2
''', encoding="utf-8")
    assert cli.main(["init"]) == 0
    assert cli.main(["graph", "src/graph.py", "--runner", "direct"]) == 0
    capsys.readouterr()

    assert cli.main(["runfrom", "A"]) == 1
    capsys.readouterr()
    storage = FileStorage(tmp_path)
    assert storage.get_job_status("A", 1) == DONE
    a_events_before = sum(
        event.get("event") == "started" for event in storage.read_job_events("A", 1)
    )

    assert cli.main(["resumefrom", "A"]) == 0
    capsys.readouterr()
    a_events_after = sum(
        event.get("event") == "started" for event in storage.read_job_events("A", 1)
    )
    assert a_events_after == a_events_before
    assert storage.get_job_status("B", 1) == DONE


def test_describe_is_longer_than_help_and_uses_abstract_examples(capsys):
    with pytest.raises(SystemExit) as exit_info:
        cli.main(["run", "--help"])
    assert exit_info.value.code == 0
    help_text = capsys.readouterr().out
    assert cli.main(["--describe", "run"]) == 0
    describe = capsys.readouterr().out
    assert "Run deliberately starts fresh work" in describe
    assert "Run deliberately starts fresh work" not in help_text
    assert "random integer" in describe
    assert "explode" not in describe.lower()


def test_resume_command_retries_failed_job_without_rerunning_done_job(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    behavior = tmp_path / "src" / "node_behavior"
    behavior.mkdir(parents=True)
    (tmp_path / "src" / "graph.py").write_text("EDGES = [('A', 'B')]\n", encoding="utf-8")
    (behavior / "A.py").write_text(
        '''from pathlib import Path
from micro_workflow_manager import NodeRouter
router = NodeRouter("A")
router.create_job(number=2)
@router.task
def run(ctx):
    root = Path(ctx.system.storage.project_dir)
    if ctx.job_id == 2 and not (root / "allow_two.txt").exists():
        raise RuntimeError("job two fails once")
    count = root / f"count_{ctx.job_id}.txt"
    value = int(count.read_text() if count.exists() else "0") + 1
    count.write_text(str(value), encoding="utf-8")
    return ctx.job_id
''', encoding="utf-8")
    (behavior / "B.py").write_text(
        '''from micro_workflow_manager import NodeRouter
router = NodeRouter("B")
@router.task
def run(ctx):
    return None
''', encoding="utf-8")
    assert cli.main(["init"]) == 0
    assert cli.main(["graph", "src/graph.py", "--runner", "direct"]) == 0
    capsys.readouterr()

    assert cli.main(["run", "A"]) == 1
    capsys.readouterr()
    assert (tmp_path / "count_1.txt").read_text() == "1"
    (tmp_path / "allow_two.txt").write_text("yes", encoding="utf-8")

    assert cli.main(["resume", "A"]) == 0
    capsys.readouterr()
    assert (tmp_path / "count_1.txt").read_text() == "1"
    assert (tmp_path / "count_2.txt").read_text() == "1"


def test_node_router_timeout_is_written_to_schema(tmp_path):
    workflow = MicroWorkflow(project_dir=tmp_path, runner="direct")
    workflow.graph([("A", "B")])
    router = NodeRouter("A", timeout=1.5)

    @router.task
    def a(ctx):
        return 1

    workflow.include_router(router)

    @workflow.task("B")
    def b(ctx):
        return None

    schema = workflow.storage.read_json(workflow.storage.node_schema_file("A"))
    assert schema["timeout"] == 1.5
    assert workflow.nodes["A"].main_task.timeout == 1.5


def test_active_run_state_contains_ownership_and_heartbeat(tmp_path, monkeypatch, capsys):
    from micro_workflow_manager import __version__

    write_project(tmp_path, monkeypatch)
    capsys.readouterr()
    assert cli.main(["run", "A"]) == 0
    storage = FileStorage(tmp_path)
    sessions = storage.list_execution_sessions()
    assert len(sessions) == 1
    state = sessions[0]
    assert state["hostname"]
    assert state["pid"] == os.getpid()
    assert datetime.fromisoformat(state["started_at"]) <= datetime.fromisoformat(state["heartbeat_at"])
    assert datetime.fromisoformat(state["heartbeat_at"]) <= datetime.fromisoformat(state["finished_at"])
    assert state["details"]["mwf_version"] == __version__
    assert state["session_kind"] == "main"
    assert state["start_component"] == ("A",)
    assert state["selected_components"] == [("A",)]
    assert state["status"] == "terminal"
    assert state["outcome"] == "done"
    assert state["failures"] == []
    owner = storage.read_job_current_owner("A", 1)
    assert owner is not None and owner["session_id"] == state["session_id"]
    assert storage.get_live_main_session() is None
    assert storage.get_component_reservation(("A",)) is None
    assert not (tmp_path / ".mwf" / "run.json").exists()


def test_unsupported_project_refusal_preserves_user_data(tmp_path, monkeypatch, capsys):
    write_project(tmp_path, monkeypatch)
    capsys.readouterr()
    config_path = tmp_path / ".mwf" / "project.json"
    config = json.loads(config_path.read_text(encoding="utf-8"))
    config.pop("schema_version", None)
    config_path.write_text(json.dumps(config), encoding="utf-8")

    input_path = tmp_path / "node" / "A" / "jobs" / "1" / "input.json"
    output_path = tmp_path / "node" / "A" / "jobs" / "1" / "output.json"
    output_path.write_text('{"custom": true}', encoding="utf-8")
    input_before = input_path.read_bytes()
    output_before = output_path.read_bytes()
    before = {
        path.relative_to(tmp_path): path.read_bytes() if path.is_file() else None
        for path in tmp_path.rglob("*")
    }

    with pytest.raises(RuntimeError, match="Unsupported MWF project format.*migration.md"):
        FileStorage(tmp_path)
    assert cli.main(["run", "A"]) == 1
    assert "Unsupported MWF project format" in capsys.readouterr().err
    assert {
        path.relative_to(tmp_path): path.read_bytes() if path.is_file() else None
        for path in tmp_path.rglob("*")
    } == before
    assert "schema_version" not in json.loads(config_path.read_text(encoding="utf-8"))
    assert input_path.read_bytes() == input_before
    assert output_path.read_bytes() == output_before

def test_runfrom_plan_is_read_only(tmp_path, monkeypatch, capsys):
    behavior = write_project(tmp_path, monkeypatch)
    capsys.readouterr()
    for path in [tmp_path / "src" / "graph.py", *behavior.glob("*.py")]:
        path.write_text(
            "raise AssertionError('A run plan must not import user code')\n" + path.read_text(encoding="utf-8"),
            encoding="utf-8",
        )
    storage = FileStorage(tmp_path)
    config_path = tmp_path / ".mwf" / "project.json"
    config_before = config_path.read_bytes()
    jobs_before = storage.list_job_ids("A")
    status_before = storage.read_job_status_data("A", 1)
    tree_before = sorted(str(path.relative_to(tmp_path)) for path in (tmp_path / "node").rglob("*"))
    files_before = {
        path.relative_to(tmp_path): path.read_bytes() if path.is_file() else None
        for path in tmp_path.rglob("*")
    }

    assert cli.main(["runfrom", "A", "--plan"]) == 0
    out = capsys.readouterr().out
    assert "Plan for: mwf runfrom A" in out
    assert "planned runfrom was not applied" in out
    assert "user code was not loaded" in out
    assert "no project state was changed" in out
    assert {
        path.relative_to(tmp_path): path.read_bytes() if path.is_file() else None
        for path in tmp_path.rglob("*")
    } == files_before
    assert config_path.read_bytes() == config_before
    assert storage.list_job_ids("A") == jobs_before
    assert storage.read_job_status_data("A", 1) == status_before
    assert sorted(str(path.relative_to(tmp_path)) for path in (tmp_path / "node").rglob("*")) == tree_before
    assert not (tmp_path / ".mwf" / "run.json").exists()


def test_graph_update_dry_run_does_not_add_or_delete_nodes(tmp_path, monkeypatch, capsys):
    write_project(tmp_path, monkeypatch)
    capsys.readouterr()
    config_before = (tmp_path / ".mwf" / "project.json").read_bytes()
    (tmp_path / "src" / "graph.py").write_text("EDGES = [('A', 'C')]\n", encoding="utf-8")
    files_before = {
        path.relative_to(tmp_path): path.read_bytes() if path.is_file() else None
        for path in tmp_path.rglob("*")
    }

    assert cli.main(["graph", "--update", "--dry-run"]) == 0
    out = capsys.readouterr().out
    assert "nodes to add: C" in out
    assert "nodes to delete: B" in out
    assert "graph synchronization was not applied" in out
    assert {
        path.relative_to(tmp_path): path.read_bytes() if path.is_file() else None
        for path in tmp_path.rglob("*")
    } == files_before
    assert (tmp_path / ".mwf" / "project.json").read_bytes() == config_before
    assert (tmp_path / "node" / "B").is_dir()
    assert not (tmp_path / "node" / "C").exists()


def test_cleanup_and_recover_dry_runs_do_not_mutate(tmp_path, monkeypatch, capsys):
    from tests.test_064_read_only_previews import (
        _close_without_sidecars, _initialize_native_project, _snapshot,
    )
    from tests.test_146_native_applied_recovery import _create_active_scope

    workflow = _initialize_native_project(tmp_path, monkeypatch, edges=[('A', 'B')])
    _create_active_scope(workflow.storage, workflow.topology.graph_shape(), 'A', 'abandoned-A')
    _close_without_sidecars(workflow.storage, tmp_path)
    before = _snapshot(tmp_path)
    capsys.readouterr()

    assert cli.main(['recover', '--dry-run']) == 0
    output = capsys.readouterr().out
    assert 'session abandoned-A' in output
    assert 'would requeue abandoned execution: A/1' in output
    assert _snapshot(tmp_path) == before

    assert cli.main(['reset', 'A', '--dry-run']) == 0
    assert 'reset' in capsys.readouterr().out.lower()
    assert _snapshot(tmp_path) == before


def test_every_describe_page_extends_help_with_abstract_examples(capsys):
    from micro_workflow_manager.cli.constants import COMMAND_NAMES
    from micro_workflow_manager.cli.descriptions import (
        COMMAND_DESCRIPTIONS,
        COMMAND_HELP_DESCRIPTIONS,
    )

    forbidden = ("explode", "tagify", "attachfragment", "preexplode", "ocr_pages", "zoning")
    assert set(COMMAND_NAMES) == set(COMMAND_HELP_DESCRIPTIONS) == set(COMMAND_DESCRIPTIONS)
    for command in COMMAND_NAMES:
        assert cli.main(["--describe", command]) == 0
        text = capsys.readouterr().out
        assert "Help summary:" in text
        assert "Extended explanation:" in text
        assert "mwf " in text
        lowered = text.lower()
        for term in forbidden:
            assert term not in lowered


def test_execution_help_names_component_selection_and_descendants(capsys):
    expected = {
        "run": "Hoeflein component",
        "resume": "Hoeflein component",
        "runfrom": "quotient-DAG descendants",
        "resumefrom": "quotient-DAG descendants",
    }
    for command, phrase in expected.items():
        with pytest.raises(SystemExit) as exit_info:
            cli.main([command, "--help"])
        assert exit_info.value.code == 0
        assert phrase in capsys.readouterr().out


def test_threads_help_describes_api_total_as_an_aggregate_budget(capsys):
    with pytest.raises(SystemExit) as exit_info:
        cli.main(["threads", "--help"])
    assert exit_info.value.code == 0
    captured = capsys.readouterr()
    output = captured.out
    assert "project-wide aggregate API admission value" in " ".join(output.split())
    assert "no aggregate framework cap" not in output
    assert "Deprecated:" in output
    assert captured.err == ""

    assert cli.main(["--describe", "threads"]) == 0
    description = capsys.readouterr().out
    assert "aggregate API admission budget" in description
    assert "no workflow-wide aggregate API cap" not in description
    assert "deprecated" in description.lower()
    assert "remains functional" in description


def test_preview_and_observer_help_describes_native_state(capsys):
    for command in ('graph', 'doctor', 'run', 'resume', 'runfrom', 'resumefrom'):
        with pytest.raises(SystemExit) as exit_info:
            cli.main([command, '--help'])
        assert exit_info.value.code == 0
        output = capsys.readouterr().out
        assert 'migrate framework state' not in output

    for command in ('doctor', 'recover', 'threads', 'monitor'):
        assert cli.main(['--describe', command]) == 0
        description = capsys.readouterr().out
        assert '.mwf/run.json' not in description
        assert '.mwf/threads.json' not in description
        assert 'migrate an old runtime' not in description
        if command == 'doctor':
            assert 'without importing' in description
            assert 'changing project state' in description
        if command == 'monitor':
            assert 'Multiple interrupts remain separately visible' in description


def test_checkpoint_watchdog_refreshes_at_each_progress_checkpoint(tmp_path):
    workflow = MicroWorkflow(project_dir=tmp_path, runner="direct")
    workflow.graph([("A", "B")])

    @workflow.task("A", checkpoint_timeout=1.0)
    def a(ctx):
        started = time.monotonic()
        for _ in range(2):
            time.sleep(0.4)
            ctx.checkpoint("halfway", progress=0.5, detail="first section complete")
        time.sleep(0.4)
        return time.monotonic() - started

    @workflow.task("B")
    def b(ctx):
        return None

    elapsed = workflow.run_one("A")
    assert elapsed > 1.0
    runtime = workflow.storage.read_job_runtime("A", 1)
    assert runtime["state"] == "completed"
    assert runtime["checkpoint_name"] == "halfway"
    assert runtime["progress"] == 0.5
    assert runtime["progress_detail"] == "first section complete"
    assert not any(
        event.get("event") == "timeout"
        for event in workflow.storage.read_job_events("A", 1)
    )


def test_checkpoint_watchdog_fails_stalled_section_and_blocks_late_write(tmp_path):
    workflow = MicroWorkflow(project_dir=tmp_path, runner="direct")
    workflow.graph([("A", "B")])

    @workflow.task("A", checkpoint_timeout=0.03)
    def a(ctx):
        ctx.checkpoint("waiting for service", progress=0.25)
        time.sleep(0.08)
        ctx.write_output("late.txt", "must be fenced")
        return "late"

    @workflow.fallback("A", name="quick")
    def quick(ctx, error=None):
        return "fallback"

    @workflow.task("B")
    def b(ctx):
        return None

    assert workflow.run_one("A") == "fallback"
    time.sleep(0.1)
    assert not (tmp_path / "node" / "A" / "output" / "late.txt").exists()
    runtime = workflow.storage.read_job_runtime("A", 1)
    assert runtime["state"] == "timed_out"
    assert runtime["timeout_kind"] == "checkpoint"
    assert runtime["checkpoint_name"] == "waiting for service"
    events = workflow.storage.read_job_events("A", 1)
    timeout_events = [event for event in events if event.get("event") == "timeout"]
    assert len(timeout_events) == 1
    assert timeout_events[0]["timeout_kind"] == "checkpoint"


def test_inspect_reports_live_checkpoint_progress(tmp_path, capsys):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event
    from micro_workflow_manager.cli.inspect import inspect_job

    workflow = MicroWorkflow(project_dir=tmp_path, runner="direct")
    workflow.graph([("A", "B")])
    checkpoint_written = Event()
    release = Event()

    @workflow.task("A", checkpoint_timeout=30.0)
    def a(ctx):
        ctx.checkpoint("download", progress=0.4, detail="2 of 5 files")
        checkpoint_written.set()
        assert release.wait(30)
        ctx.checkpoint("complete", progress=1.0)
        return "ok"

    @workflow.task("B")
    def b(ctx):
        return None

    job = workflow.start("A")
    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            execution = executor.submit(workflow.run_job, 'A', job.job_id, ignore_readiness=True)
            try:
                assert checkpoint_written.wait(15)
                assert inspect_job(workflow, 'A', job.job_id) == 0
                output = capsys.readouterr().out
                assert 'checkpoint: download' in output
                assert 'progress: 40.0%' in output
                assert 'progress detail: 2 of 5 files' in output
                assert 'checkpoint deadline:' in output
            finally:
                release.set()
            assert execution.result(timeout=15) == 'ok'
    finally:
        workflow.storage.close_database_connections()


@pytest.mark.parametrize("startup_delay", [0.0, 0.6])
def test_scheduler_uses_one_central_watchdog_for_multiple_attempts(tmp_path, monkeypatch, startup_delay):
    from threading import Event, Thread, enumerate as enumerate_threads
    import micro_workflow_manager.workflow.supervisor_attempts as attempts_module
    import micro_workflow_manager.workflow.supervisor_core as core_module
    import micro_workflow_manager.workflow.supervisor_persistence as persistence_module

    # This checks shared supervision, so host scheduling must not consume deadlines.
    frozen_time = time.monotonic()
    for module in (attempts_module, core_module, persistence_module):
        monkeypatch.setattr(module, "monotonic", lambda: frozen_time)

    workflow = MicroWorkflow(project_dir=tmp_path, runner="threaded")
    workflow.graph([("A", "B")])
    workflow.nodes["A"].max_threads = 4
    all_started = Event()
    release = Event()
    started_count = {"value": 0}
    results = []
    errors = []
    from threading import Lock
    count_lock = Lock()

    @workflow.task("A", max_threads=4, checkpoint_timeout=1.0)
    def a(ctx):
        ctx.checkpoint("waiting", progress=0.5)
        with count_lock:
            started_count["value"] += 1
            if started_count["value"] == 4:
                all_started.set()
        assert release.wait(10)
        return ctx.job_id

    @workflow.task("B")
    def b(ctx):
        return None

    jobs = [workflow.start("A") for _ in range(4)]
    def run():
        try:
            time.sleep(startup_delay)
            results.extend(workflow.run_node_jobs("A", jobs, ignore_readiness=True))
        except BaseException as error:
            errors.append(error)

    runner = Thread(target=run, daemon=True)
    runner.start()
    try:
        assert all_started.wait(10)
        assert len(workflow.scheduler_supervisor._watches) == 4
        assert workflow.scheduler_supervisor._thread is not None
        assert workflow.scheduler_supervisor._thread.name == "mwf-scheduler-supervisor"
        assert not any(thread.name.startswith("mwf-timeout-") for thread in enumerate_threads())
        release.set()
        runner.join(timeout=10)
        assert not runner.is_alive()
        assert not errors, errors
        assert sorted(results) == sorted(job.job_id for job in jobs)
        workflow.storage.db_mutation_barrier()
        counts = workflow.storage.job_status_counts("A")
        assert counts.get("done") == 4 and sum(counts.values()) == 4
        for job in jobs:
            output = json.loads(workflow.storage.output_file("A", job.job_id).read_text(encoding="utf-8"))
            owner = workflow.storage.read_job_current_owner("A", job.job_id)
            assert owner is not None
            assert output == {
                "status": "done", "result_type": "int", "result_repr": str(job.job_id),
                "generation": owner["generation"], "execution_id": owner["execution_id"],
            }
    finally:
        release.set()
        runner.join(timeout=10)
        workflow.storage.db_mutation_barrier()
        cleanup_deadline = time.perf_counter() + 10
        while workflow.storage.mutation_writer_diagnostics()["writer_alive"]:
            assert time.perf_counter() < cleanup_deadline, "Mutation writer did not retire"
            time.sleep(0.01)
        workflow.storage.close_thread_connection()


def test_untimed_owned_task_keeps_deadlines_and_runtime_unset(tmp_path):
    workflow = MicroWorkflow(project_dir=tmp_path, runner="direct")
    workflow.graph([("A", "B")])
    observed: list[str] = []

    @workflow.task("A")
    def a(ctx):
        with workflow.scheduler_supervisor._condition:
            watches = [watch for watch in workflow.scheduler_supervisor._watches.values()
                       if watch.execution_id == ctx.execution_id]
            assert len(watches) == 1
            watch = watches[0]
            assert not watch.supervised
            assert watch.total_deadline is None
            assert watch.checkpoint_deadline is None
            assert watch.external_wait_deadline is None
        observed.append(ctx.execution_id)
        return "ok"

    @workflow.task("B")
    def b(ctx):
        return None

    assert workflow.run_one("A") == "ok"
    owner = workflow.storage.read_job_current_owner("A", 1)
    assert owner is not None
    assert observed == [owner["execution_id"]]
    assert workflow.storage.read_job_runtime("A", 1) == {}
    assert not (workflow.storage.job_base_dir("A", 1) / "runtime.json").exists()
    assert workflow.scheduler_supervisor._watches == {}
    assert workflow.storage.get_live_main_session() is None


def test_dynamic_checkpoint_timeout_requires_supervised_handler(tmp_path):
    workflow = MicroWorkflow(project_dir=tmp_path, runner="direct")
    workflow.graph([("A", "B")])

    @workflow.task("A")
    def a(ctx):
        ctx.checkpoint("section", timeout=0.1)

    @workflow.task("B")
    def b(ctx):
        return None

    job = workflow.start("A")
    with pytest.raises(Exception) as error:
        workflow.run_job("A", job.job_id, ignore_readiness=True)
    assert "checkpoint_timeout" in str(error.value.__cause__ or error.value)


def test_router_checkpoint_timeout_is_written_to_schema(tmp_path):
    workflow = MicroWorkflow(project_dir=tmp_path, runner="direct")
    workflow.graph([("A", "B")])
    router = NodeRouter("A", checkpoint_timeout=2.5)

    @router.task
    def a(ctx):
        return 1

    workflow.include_router(router)

    @workflow.task("B")
    def b(ctx):
        return None

    schema = workflow.storage.read_json(workflow.storage.node_schema_file("A"))
    assert schema["checkpoint_timeout"] == 2.5
    assert workflow.nodes["A"].main_task.checkpoint_timeout == 2.5


def test_checkpoint_watchdog_works_inside_process_runner(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    behavior = tmp_path / "src" / "node_behavior"
    behavior.mkdir(parents=True)
    (tmp_path / "src" / "graph.py").write_text("EDGES = [('A', 'B')]\n", encoding="utf-8")
    (behavior / "A.py").write_text(
        '''import time
from micro_workflow_manager import NodeRouter
router = NodeRouter("A", runner="process", checkpoint_timeout=0.05)
router.create_job(number=1)
@router.task
def run(ctx):
    ctx.checkpoint("remote wait", progress=0.2)
    time.sleep(0.15)
    ctx.write_output("late.txt", "bad")
    return "late"
@router.fallback(name="quick")
def quick(ctx, error=None):
    return "fallback"
''',
        encoding="utf-8",
    )
    (behavior / "B.py").write_text(
        '''from micro_workflow_manager import NodeRouter
router = NodeRouter("B")
@router.task
def run(ctx):
    return None
''',
        encoding="utf-8",
    )
    assert cli.main(["init"]) == 0
    assert cli.main(["graph", "src/graph.py", "--runner", "process"]) == 0
    capsys.readouterr()

    assert cli.main(["run", "A", "--runner", "process"]) == 0
    capsys.readouterr()
    output = json.loads((tmp_path / "node" / "A" / "jobs" / "1" / "output.json").read_text())
    assert output["result_repr"] == "'fallback'"
    assert not (tmp_path / "node" / "A" / "output" / "late.txt").exists()
    events = FileStorage(tmp_path).read_job_events("A", 1)
    assert any(
        event.get("event") == "timeout" and event.get("timeout_kind") == "checkpoint"
        for event in events
    )
